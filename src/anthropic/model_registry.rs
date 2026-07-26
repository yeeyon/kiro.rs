//! 模型注册表：模型 ID 解析 / 归一化 / 能力推断的**单一事实来源**。
//!
//! # 设计目标：新模型开箱即用（native support）
//!
//! 改造前，每当 Anthropic / OpenAI 发新模型，都必须改两处硬编码再重新编译：
//! 1. [`super::converter::map_model`] 的 `contains("4-8")` 版本号 allow-list；
//! 2. `handlers::available_models()` 里手写的 `/v1/models` 静态目录。
//!
//! 漏改任何一处，客户端就拿不到新模型（`map_model` 返回 `None` → 400）。
//!
//! 本模块把这套逻辑换成「**按结构解析 + 按代际推断**」：
//! - [`ModelIdentity::parse`] 从 ID 里解析出厂商 / 家族 / 代际（generation），
//!   而不是逐个字符串比对；
//! - 能力（上下文窗口、原生 reasoning、xhigh effort）由**代际阈值 + 少量
//!   deny-list 例外**决定，未知的新模型默认落在「新代际」一侧；
//! - [`catalog`] 由三层合并：内置 seed、上游 `ListAvailableModels` 动态快照、
//!   用户配置 override。
//!
//! 结果：只要新模型沿用现有命名规律（`claude-<family>-<generation>` / `gpt-<generation>-*`）
//! **且参数契约不变**，路由、上下文窗口、`/v1/models` 广告全部自动生效，无需改码。
//! 只有当上游引入**新参数语义**（例如又一个 effort 档位、或某模型拒收
//! `additionalModelRequestFields`）时才需要动这里 —— 那种情况登记到
//! [`NATIVE_REASONING_DENY`] / [`XHIGH_DENY`] 或配置 override 即可。

use std::collections::BTreeMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// =============================================================================
// 代际阈值与例外表（唯一需要人工维护的地方）
// =============================================================================

/// Anthropic 起，`additionalModelRequestFields.output_config` 被接受的最低代际。
const NATIVE_REASONING_MIN_GEN: Generation = Generation::new(4, 6);

/// `effort = "xhigh"` 被接受的最低代际（4.5 / 4.6 系会 400）。
const XHIGH_MIN_GEN: Generation = Generation::new(4, 7);

/// 1M 上下文窗口的最低代际（Kiro 2026-03-24 起 4.6 系升级到 1M）。
const LARGE_CONTEXT_MIN_GEN: Generation = Generation::new(4, 6);

/// 未知 Anthropic 模型允许透传的代际下限：低于此值视为 legacy（如
/// `claude-3-5-sonnet`），保持 `None` 不路由。
const ANTHROPIC_PASSTHROUGH_MIN_MAJOR: u32 = 4;

/// OpenAI 侧允许透传的代际下限（`gpt-4` 及更早不由 Kiro 提供）。
const OPENAI_PASSTHROUGH_MIN_MAJOR: u32 = 5;

/// 已实测**拒收** `output_config` 的模型（代际阈值的例外）。
///
/// 这些 ID 代际达标但上游仍 400，必须显式排除。实测发现新的就加一行。
const NATIVE_REASONING_DENY: &[&str] = &["claude-sonnet-4.8"];

/// 已实测拒收 `effort = "xhigh"` 的模型（代际阈值之外的额外例外）。
const XHIGH_DENY: &[&str] = &[];

/// 仅在 `thinking.type = "adaptive"` 下才接受 `output_config` 的模型。
const ADAPTIVE_ONLY_REASONING: &[&str] = &["claude-opus-4.6"];

/// 不参与原生 reasoning 的家族（无论代际）。
const NO_REASONING_FAMILIES: &[&str] = &["haiku"];

/// 默认上下文窗口（未知模型的保守值）。
const DEFAULT_CONTEXT_WINDOW: i32 = 200_000;
/// 大窗口模型的上下文窗口。
const LARGE_CONTEXT_WINDOW: i32 = 1_000_000;
/// Kiro 上的 GPT-5.x 系列窗口。
const OPENAI_CONTEXT_WINDOW: i32 = 272_000;
/// `/v1/models` 广告的默认最大输出 tokens。
const DEFAULT_MAX_OUTPUT_TOKENS: i32 = 64_000;

/// 模型名里代表「开启思考」的后缀。
pub const THINKING_SUFFIX: &str = "-thinking";

// =============================================================================
// 代际（generation）
// =============================================================================

/// 模型代际，如 `4.8` → `Generation { major: 4, minor: 8 }`。
///
/// 用序数比较代替字符串比对，是「新模型自动支持」的核心：`5.1` > `4.7`
/// 无需登记即成立。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Generation {
    pub major: u32,
    pub minor: u32,
}

impl Generation {
    pub const fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }
}

impl std::fmt::Display for Generation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.minor == 0 {
            write!(f, "{}", self.major)
        } else {
            write!(f, "{}.{}", self.major, self.minor)
        }
    }
}

// =============================================================================
// 厂商
// =============================================================================

/// 模型厂商。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Vendor {
    Anthropic,
    OpenAI,
    /// Kiro 自有的 `auto`（上游按任务挑模型）。
    KiroAuto,
}

impl Vendor {
    /// `/v1/models` 的 `owned_by` 字段。
    pub fn owned_by(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::OpenAI => "openai",
            Self::KiroAuto => "kiro",
        }
    }
}

// =============================================================================
// 模型身份解析
// =============================================================================

/// 从任意客户端模型名解析出的结构化身份。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelIdentity {
    /// 厂商。
    pub vendor: Vendor,
    /// 家族名（`sonnet` / `opus` / `haiku` / `fable` / `mythos` …）。
    /// 无法识别家族时为 `None`（如 `claude-5`、`gpt-5.6-sol`）。
    pub family: Option<String>,
    /// 代际。无版本号时为 `None`。
    pub generation: Option<Generation>,
    /// 客户端是否用 `-thinking` 后缀请求了思考模式。
    pub thinking_suffix: bool,
    /// 去掉 `-thinking` 后的小写原始 ID（OpenAI 透传时用它，因为
    /// `sol`/`terra`/`luna` 这类后缀无法从结构化字段重建）。
    pub base_id: String,
}

/// Anthropic 已知家族名。顺序无关，仅用于在 token 流里定位家族。
const ANTHROPIC_FAMILIES: &[&str] = &["sonnet", "opus", "haiku", "fable", "mythos"];

impl ModelIdentity {
    /// 解析模型 ID。无法归到任何已知厂商时返回 `None`。
    ///
    /// 解析而非匹配：`claude-sonnet-4-5-20250929-thinking` 会被拆成
    /// `{vendor: Anthropic, family: sonnet, generation: 4.5, thinking: true}`，
    /// 所以未来的 `claude-sonnet-6-2-20270101` 同样能被识别。
    pub fn parse(raw: &str) -> Option<Self> {
        let lower = raw.trim().to_ascii_lowercase();
        if lower.is_empty() {
            return None;
        }

        let (base, thinking_suffix) = match lower.strip_suffix(THINKING_SUFFIX) {
            Some(stripped) => (stripped.to_string(), true),
            // 兼容 `...thinking` / `..._thinking` 等写法
            None if lower.contains("thinking") => (
                lower
                    .replace("-thinking", "")
                    .replace("_thinking", "")
                    .replace("thinking", ""),
                true,
            ),
            None => (lower.clone(), false),
        };
        let base = base.trim_matches(['-', '_', ' ']).to_string();

        if base == "auto" {
            return Some(Self {
                vendor: Vendor::KiroAuto,
                family: None,
                generation: None,
                thinking_suffix,
                base_id: base,
            });
        }

        let vendor = if base.starts_with("gpt") || base.starts_with("o1") || base.starts_with("o3") {
            Vendor::OpenAI
        } else if base.starts_with("claude") || ANTHROPIC_FAMILIES.iter().any(|f| base.contains(f)) {
            Vendor::Anthropic
        } else {
            return None;
        };

        // token 化：`-` / `_` / `/` 都当分隔符，同时把 `sonnet4.8` 这类粘连形式拆开。
        let tokens = tokenize(&base);
        let (family, generation) = match vendor {
            Vendor::Anthropic => parse_anthropic_tokens(&tokens),
            Vendor::OpenAI => (None, parse_openai_generation(&tokens)),
            Vendor::KiroAuto => (None, None),
        };

        Some(Self {
            vendor,
            family,
            generation,
            thinking_suffix,
            base_id: base,
        })
    }

    /// 规范化的 Kiro 模型 ID（未经目录校验）。
    ///
    /// Anthropic：`claude-<family>-<generation>`；无家族但有代际：`claude-<generation>`。
    /// OpenAI / auto：原样透传（后缀不可重建）。
    pub fn canonical_id(&self) -> Option<String> {
        match self.vendor {
            Vendor::KiroAuto => Some("auto".to_string()),
            Vendor::OpenAI => Some(self.base_id.clone()),
            Vendor::Anthropic => {
                let generation = self.generation?;
                match &self.family {
                    Some(family) => Some(format!("claude-{family}-{generation}")),
                    None => Some(format!("claude-{generation}")),
                }
            }
        }
    }

    /// 是否为 `haiku` 等不参与原生 reasoning 的家族。
    fn family_blocks_reasoning(&self) -> bool {
        self.family
            .as_deref()
            .is_some_and(|f| NO_REASONING_FAMILIES.contains(&f))
    }
}

/// 把模型 ID 切成 token，并拆开 `sonnet4.8` / `opus5` 这类粘连写法。
fn tokenize(base: &str) -> Vec<String> {
    let mut out = Vec::new();
    for piece in base.split(['-', '_', '/', ' ', '.']) {
        if piece.is_empty() {
            continue;
        }
        // 字母与数字交界处再切一刀：`sonnet4` → [`sonnet`, `4`]
        let mut current = String::new();
        let mut current_is_digit: Option<bool> = None;
        for ch in piece.chars() {
            let is_digit = ch.is_ascii_digit();
            if current_is_digit.is_some_and(|prev| prev != is_digit) {
                out.push(std::mem::take(&mut current));
            }
            current.push(ch);
            current_is_digit = Some(is_digit);
        }
        if !current.is_empty() {
            out.push(current);
        }
    }
    out
}

/// 日期戳（`20250929`）的位数，用于把它从版本号里排除。
const DATE_STAMP_LEN: usize = 8;

fn as_version_number(token: &str) -> Option<u32> {
    if token.is_empty() || token.len() >= DATE_STAMP_LEN || !token.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    token.parse().ok()
}

/// 解析 Anthropic ID 的家族与代际。
///
/// 两条路径：
/// 1. 命中 [`ANTHROPIC_FAMILIES`] 已知家族 → 版本号只从**家族名之后**取，
///    这样 legacy 的 `claude-3-5-sonnet-20241022` 不会被误判成 3.5 代
///    （它的数字在家族名之前）。
/// 2. 未命中已知家族 → 把 `claude` 与第一个版本号之间的 token 当作家族名。
///    这是「Anthropic 起了个全新家族名」时仍能路由的关键
///    （`claude-nimbus-7` → family=nimbus, gen=7）。
fn parse_anthropic_tokens(tokens: &[String]) -> (Option<String>, Option<Generation>) {
    let claude_idx = tokens.iter().position(|t| t == "claude");
    let search_start = claude_idx.map_or(0, |i| i + 1);

    if let Some(family_idx) = tokens
        .iter()
        .position(|t| ANTHROPIC_FAMILIES.contains(&t.as_str()))
    {
        let generation = parse_generation(tokens.get(family_idx + 1..).unwrap_or(&[]));
        return (Some(tokens[family_idx].clone()), generation);
    }

    // 未知家族：定位第一个版本号 token，其与 `claude` 之间的部分即家族名。
    let rest = tokens.get(search_start..).unwrap_or(&[]);
    let Some(version_idx) = rest.iter().position(|t| as_version_number(t).is_some()) else {
        return (None, None);
    };
    let family = rest[..version_idx]
        .iter()
        .filter(|t| !t.is_empty())
        .cloned()
        .collect::<Vec<_>>()
        .join("-");
    let generation = parse_generation(&rest[version_idx..]);

    (
        (!family.is_empty()).then_some(family),
        generation,
    )
}

/// 从 token 序列头部解析代际：`["4","8",...]` → 4.8；`["5",...]` → 5.0。
fn parse_generation(tokens: &[String]) -> Option<Generation> {
    let major = as_version_number(tokens.first()?)?;
    let minor = tokens
        .get(1)
        .and_then(|t| as_version_number(t))
        .unwrap_or(0);
    Some(Generation::new(major, minor))
}

/// OpenAI ID 的代际：`gpt-5.6-sol` → 5.6。
fn parse_openai_generation(tokens: &[String]) -> Option<Generation> {
    let start = tokens
        .iter()
        .position(|t| as_version_number(t).is_some())?;
    parse_generation(&tokens[start..])
}

// =============================================================================
// 目录条目
// =============================================================================

/// 一个可路由模型的目录条目。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelEntry {
    /// Kiro 侧真实模型 ID（下发给上游的值），如 `claude-opus-4.8`。
    pub id: String,
    /// `/v1/models` 展示名。
    pub display_name: String,
    /// 厂商。
    pub vendor: Vendor,
    /// 上下文窗口。`None` 表示按代际推断。
    pub context_window: Option<i32>,
    /// 最大输出 tokens（`/v1/models` 广告用）。
    pub max_output_tokens: i32,
    /// `/v1/models` 里额外广告的客户端别名（带日期戳的 Anthropic 官方 ID 等）。
    pub aliases: Vec<String>,
    /// `created` 时间戳（`/v1/models` 广告用）。
    pub created: i64,
    /// 是否来自上游 `ListAvailableModels`（而非内置 seed）。
    pub from_upstream: bool,
}

impl ModelEntry {
    fn seed(
        id: &str,
        display_name: &str,
        vendor: Vendor,
        context_window: i32,
        created: i64,
        aliases: &[&str],
    ) -> Self {
        Self {
            id: id.to_string(),
            display_name: display_name.to_string(),
            vendor,
            context_window: Some(context_window),
            max_output_tokens: DEFAULT_MAX_OUTPUT_TOKENS,
            aliases: aliases.iter().map(|s| s.to_string()).collect(),
            created,
            from_upstream: false,
        }
    }
}

/// 内置 seed 目录：进程启动即可用，不依赖上游可达性。
///
/// 只是**兜底**，不是白名单 —— 上游 `ListAvailableModels` 出现的新模型会被
/// [`update_from_upstream`] 合并进来，未出现在任何目录里的新模型也仍能靠
/// [`resolve`] 的代际规则透传。
fn seed_catalog() -> Vec<ModelEntry> {
    const JUN_15_2026: i64 = 1_781_481_600;
    const JUL_25_2026: i64 = 1_784_937_600;
    const GPT_5_6: i64 = 1_782_000_000;

    vec![
        ModelEntry::seed("auto", "Auto", Vendor::KiroAuto, LARGE_CONTEXT_WINDOW, GPT_5_6, &[]),
        ModelEntry::seed(
            "gpt-5.6-sol",
            "GPT-5.6 Sol",
            Vendor::OpenAI,
            OPENAI_CONTEXT_WINDOW,
            GPT_5_6,
            &[],
        ),
        ModelEntry::seed(
            "gpt-5.6-terra",
            "GPT-5.6 Terra",
            Vendor::OpenAI,
            OPENAI_CONTEXT_WINDOW,
            GPT_5_6,
            &[],
        ),
        ModelEntry::seed(
            "gpt-5.6-luna",
            "GPT-5.6 Luna",
            Vendor::OpenAI,
            OPENAI_CONTEXT_WINDOW,
            GPT_5_6,
            &[],
        ),
        ModelEntry::seed(
            "claude-fable-5",
            "Claude Fable 5",
            Vendor::Anthropic,
            LARGE_CONTEXT_WINDOW,
            JUN_15_2026,
            &[],
        ),
        ModelEntry::seed(
            "claude-opus-5",
            "Claude Opus 5",
            Vendor::Anthropic,
            LARGE_CONTEXT_WINDOW,
            JUL_25_2026,
            &[],
        ),
        ModelEntry::seed(
            "claude-sonnet-5",
            "Claude Sonnet 5",
            Vendor::Anthropic,
            LARGE_CONTEXT_WINDOW,
            JUN_15_2026,
            &[],
        ),
        ModelEntry::seed(
            "claude-opus-4.8",
            "Claude Opus 4.8",
            Vendor::Anthropic,
            LARGE_CONTEXT_WINDOW,
            1_776_384_000,
            &["claude-opus-4-8"],
        ),
        ModelEntry::seed(
            "claude-sonnet-4.8",
            "Claude Sonnet 4.8",
            Vendor::Anthropic,
            LARGE_CONTEXT_WINDOW,
            1_776_384_000,
            &["claude-sonnet-4-8"],
        ),
        ModelEntry::seed(
            "claude-opus-4.7",
            "Claude Opus 4.7",
            Vendor::Anthropic,
            LARGE_CONTEXT_WINDOW,
            1_772_236_800,
            &["claude-opus-4-7"],
        ),
        ModelEntry::seed(
            "claude-opus-4.6",
            "Claude Opus 4.6",
            Vendor::Anthropic,
            LARGE_CONTEXT_WINDOW,
            1_766_620_800,
            &["claude-opus-4-6"],
        ),
        ModelEntry::seed(
            "claude-sonnet-4.6",
            "Claude Sonnet 4.6",
            Vendor::Anthropic,
            LARGE_CONTEXT_WINDOW,
            1_766_620_800,
            &["claude-sonnet-4-6"],
        ),
        ModelEntry::seed(
            "claude-opus-4.5",
            "Claude Opus 4.5",
            Vendor::Anthropic,
            DEFAULT_CONTEXT_WINDOW,
            1_762_041_600,
            &["claude-opus-4-5-20251101"],
        ),
        ModelEntry::seed(
            "claude-sonnet-4.5",
            "Claude Sonnet 4.5",
            Vendor::Anthropic,
            DEFAULT_CONTEXT_WINDOW,
            1_758_931_200,
            &["claude-sonnet-4-5-20250929"],
        ),
        ModelEntry::seed(
            "claude-haiku-4.5",
            "Claude Haiku 4.5",
            Vendor::Anthropic,
            DEFAULT_CONTEXT_WINDOW,
            1_759_276_800,
            &["claude-haiku-4-5-20251001"],
        ),
    ]
}

// =============================================================================
// 配置 override（不重编译即可支持带新参数的模型）
// =============================================================================

/// `config.json` 的 `model_registry` 段：给「参数契约变了」的新模型留的逃生口。
///
/// 代际规则覆盖不到的情况（上游新增 effort 档位、某新模型拒收
/// `additionalModelRequestFields`）不必等新版本二进制，改配置重启即可。
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelRegistryOverrides {
    /// 追加 / 覆盖目录条目。键为 Kiro 模型 ID。
    #[serde(default)]
    pub models: BTreeMap<String, ModelOverride>,
    /// 额外的「客户端名 → Kiro ID」硬映射，优先于结构化解析。
    #[serde(default)]
    pub aliases: BTreeMap<String, String>,
    /// 追加到 [`NATIVE_REASONING_DENY`] 的模型 ID。
    #[serde(default)]
    pub deny_native_reasoning: Vec<String>,
    /// 追加到 [`XHIGH_DENY`] 的模型 ID。
    #[serde(default)]
    pub deny_xhigh_effort: Vec<String>,
    /// 强制允许原生 reasoning（覆盖代际阈值与 deny-list）。
    #[serde(default)]
    pub allow_native_reasoning: Vec<String>,
}

/// 单个模型的配置 override。
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    /// `anthropic` / `openai` / `kiro`，缺省按 ID 解析。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owned_by: Option<String>,
    /// 是否在 `/v1/models` 广告（默认 true）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertise: Option<bool>,
}

// =============================================================================
// 全局注册表
// =============================================================================

/// 注册表内部状态。
struct RegistryState {
    /// 目录：Kiro 模型 ID → 条目（seed ∪ 上游 ∪ override）。
    entries: BTreeMap<String, ModelEntry>,
    /// 配置 override。
    overrides: ModelRegistryOverrides,
    /// 不广告的模型 ID（override 里 `advertise: false`）。
    hidden: Vec<String>,
}

impl RegistryState {
    fn new() -> Self {
        let mut state = Self {
            entries: BTreeMap::new(),
            overrides: ModelRegistryOverrides::default(),
            hidden: Vec::new(),
        };
        for entry in seed_catalog() {
            state.entries.insert(entry.id.clone(), entry);
        }
        state
    }

    fn apply_overrides(&mut self) {
        self.hidden.clear();
        for (id, ov) in &self.overrides.models {
            let vendor = ov
                .owned_by
                .as_deref()
                .and_then(|v| match v.to_ascii_lowercase().as_str() {
                    "anthropic" => Some(Vendor::Anthropic),
                    "openai" => Some(Vendor::OpenAI),
                    "kiro" | "auto" => Some(Vendor::KiroAuto),
                    _ => None,
                })
                .or_else(|| ModelIdentity::parse(id).map(|i| i.vendor))
                .unwrap_or(Vendor::Anthropic);

            let entry = self.entries.entry(id.clone()).or_insert_with(|| ModelEntry {
                id: id.clone(),
                display_name: default_display_name(id),
                vendor,
                context_window: None,
                max_output_tokens: DEFAULT_MAX_OUTPUT_TOKENS,
                aliases: Vec::new(),
                created: 0,
                from_upstream: false,
            });

            entry.vendor = vendor;
            if let Some(name) = &ov.display_name {
                entry.display_name = name.clone();
            }
            if let Some(window) = ov.context_window {
                entry.context_window = Some(window);
            }
            if let Some(max_out) = ov.max_output_tokens {
                entry.max_output_tokens = max_out;
            }
            for alias in &ov.aliases {
                if !entry.aliases.contains(alias) {
                    entry.aliases.push(alias.clone());
                }
            }
            if ov.advertise == Some(false) {
                self.hidden.push(id.clone());
            }
        }
    }
}

/// 由 ID 猜一个展示名：`claude-opus-4.8` → `Claude Opus 4.8`。
fn default_display_name(id: &str) -> String {
    id.split(['-', '.'])
        .filter(|s| !s.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) if first.is_ascii_alphabetic() => {
                    format!("{}{}", first.to_ascii_uppercase(), chars.as_str())
                }
                _ => part.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn registry() -> &'static RwLock<RegistryState> {
    static REGISTRY: std::sync::OnceLock<RwLock<RegistryState>> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(RegistryState::new()))
}

/// 安装配置 override（进程启动时调用一次）。
pub fn set_overrides(overrides: ModelRegistryOverrides) {
    let mut state = registry().write();
    state.overrides = overrides;
    state.apply_overrides();
    tracing::info!(
        models = state.overrides.models.len(),
        aliases = state.overrides.aliases.len(),
        "model_registry: 已应用配置 override"
    );
}

/// 合并上游 `ListAvailableModels` 的结果。
///
/// 这是「新模型零改码」的第二条腿：Kiro 一旦开始广告新模型 ID，本地目录
/// 立刻学到它（含真实 `maxInputTokens`），`/v1/models` 随即广告出去。
pub fn update_from_upstream(models: &[crate::kiro::model::available_models::UpstreamModel]) {
    if models.is_empty() {
        return;
    }
    let mut state = registry().write();
    let mut learned = Vec::new();

    for upstream in models {
        let id = upstream.model_id.trim();
        if id.is_empty() {
            continue;
        }
        let vendor = ModelIdentity::parse(id)
            .map(|i| i.vendor)
            .unwrap_or(Vendor::Anthropic);
        let window = upstream
            .token_limits
            .as_ref()
            .and_then(|t| t.max_input_tokens)
            .and_then(|v| i32::try_from(v).ok())
            .filter(|v| *v > 0);

        match state.entries.get_mut(id) {
            Some(entry) => {
                entry.from_upstream = true;
                // 上游给出的真实窗口优先于内置推断值。
                if let Some(window) = window {
                    entry.context_window = Some(window);
                }
                if let Some(name) = &upstream.model_name
                    && !name.trim().is_empty()
                {
                    entry.display_name = name.clone();
                }
            }
            None => {
                learned.push(id.to_string());
                state.entries.insert(
                    id.to_string(),
                    ModelEntry {
                        id: id.to_string(),
                        display_name: upstream
                            .model_name
                            .clone()
                            .filter(|n| !n.trim().is_empty())
                            .unwrap_or_else(|| default_display_name(id)),
                        vendor,
                        context_window: window,
                        max_output_tokens: DEFAULT_MAX_OUTPUT_TOKENS,
                        aliases: dashed_aliases(id),
                        created: 0,
                        from_upstream: true,
                    },
                );
            }
        }
    }

    // override 始终压在最上层。
    state.apply_overrides();

    if !learned.is_empty() {
        tracing::info!(
            models = ?learned,
            "model_registry: 从上游 ListAvailableModels 学到新模型，已可路由并广告"
        );
    }
}

/// 为 `claude-opus-4.8` 生成客户端常用的连字符别名 `claude-opus-4-8`。
fn dashed_aliases(id: &str) -> Vec<String> {
    if !id.contains('.') {
        return Vec::new();
    }
    vec![id.replace('.', "-")]
}

// =============================================================================
// 解析 → Kiro 模型 ID
// =============================================================================

/// 把客户端模型名解析为 Kiro 侧真实模型 ID。
///
/// 顺序：
/// 1. 配置 `aliases` 硬映射；
/// 2. 目录直接命中（ID 或别名，含上游学到的新模型）；
/// 3. 结构化解析 + 代际规则透传（**新模型走这条**）。
///
/// 返回 `None` 只发生在真正无法服务的情况：legacy 代际（`claude-3-5-sonnet`）
/// 或非 Kiro 厂商。
pub fn resolve(model: &str) -> Option<String> {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    let state = registry().read();

    // 1. 配置硬映射与目录别名（含 `-thinking` 变体）。
    // 这一步在结构化解析**之前**：显式声明的别名可以是任意字符串
    // （如 `zephyr`），不必符合 `claude-*` / `gpt-*` 命名。
    let bare = lower.strip_suffix(THINKING_SUFFIX).unwrap_or(&lower);
    for key in [lower.as_str(), bare] {
        if let Some(target) = state.overrides.aliases.get(key) {
            return Some(target.clone());
        }
        if let Some(entry) = state
            .entries
            .values()
            .find(|e| e.aliases.iter().any(|a| a.eq_ignore_ascii_case(key)))
        {
            return Some(entry.id.clone());
        }
        if state.entries.contains_key(key) {
            return Some(key.to_string());
        }
    }

    let identity = ModelIdentity::parse(trimmed)?;

    // 2. 目录命中：按去掉 thinking 的原始 ID，再按规范化 ID。
    if state.entries.contains_key(&identity.base_id) {
        return Some(identity.base_id.clone());
    }
    let canonical = identity.canonical_id();
    if let Some(canonical) = &canonical
        && state.entries.contains_key(canonical)
    {
        return Some(canonical.clone());
    }

    // 3. 结构化透传 / 代际吸附。
    let canonical = canonical?;
    match identity.vendor {
        Vendor::KiroAuto => Some(canonical),
        Vendor::OpenAI => identity
            .generation
            .filter(|g| g.major >= OPENAI_PASSTHROUGH_MIN_MAJOR)
            .map(|_| canonical),
        Vendor::Anthropic => resolve_anthropic_generation(&state, &identity, canonical),
    }
}

/// Anthropic 未命中目录时的处置：
/// - 代际高于本家族已知最高 → 透传（新模型，native support）；
/// - 代际低于已知最低但主版本号相同 → 吸附到该家族最低已知代际
///   （`claude-haiku-4-20250514` → `claude-haiku-4.5`）；
/// - 主版本号低于 [`ANTHROPIC_PASSTHROUGH_MIN_MAJOR`] → 不服务（legacy）。
fn resolve_anthropic_generation(
    state: &RegistryState,
    identity: &ModelIdentity,
    canonical: String,
) -> Option<String> {
    let generation = identity.generation?;
    if generation.major < ANTHROPIC_PASSTHROUGH_MIN_MAJOR {
        return None;
    }

    // 无家族的 `claude-<generation>`：目录没有就不猜，交给 aliases / 上游发现。
    let family = identity.family.as_deref()?;

    let mut known: Vec<Generation> = state
        .entries
        .keys()
        .filter_map(|id| {
            let other = ModelIdentity::parse(id)?;
            (other.vendor == Vendor::Anthropic && other.family.as_deref() == Some(family))
                .then_some(other.generation)
                .flatten()
        })
        .collect();
    known.sort_unstable();

    match (known.first(), known.last()) {
        // 新代际：透传，让上游决定。
        (Some(_), Some(max)) if generation > *max => Some(canonical),
        // 落在已知区间内但没有精确条目（如 haiku 4.0）→ 吸附到最低已知代际。
        (Some(min), Some(_)) if generation < *min => {
            Some(format!("claude-{family}-{min}"))
        }
        (Some(_), Some(_)) => Some(canonical),
        // 全新家族：主版本号达标即透传。
        _ => Some(canonical),
    }
}

// =============================================================================
// 能力查询
// =============================================================================

/// 上下文窗口：目录值（含上游真实 `maxInputTokens`）优先，其次按代际推断。
pub fn context_window(model: &str) -> i32 {
    let resolved = resolve(model);
    let lookup_id = resolved.as_deref().unwrap_or(model);

    if let Some(window) = registry()
        .read()
        .entries
        .get(lookup_id)
        .and_then(|e| e.context_window)
    {
        return window;
    }

    // 目录未收录（全新模型）→ 按厂商 / 代际推断。
    match ModelIdentity::parse(lookup_id) {
        Some(identity) => match identity.vendor {
            Vendor::KiroAuto => LARGE_CONTEXT_WINDOW,
            Vendor::OpenAI => OPENAI_CONTEXT_WINDOW,
            Vendor::Anthropic => match identity.generation {
                Some(generation) if generation >= LARGE_CONTEXT_MIN_GEN => LARGE_CONTEXT_WINDOW,
                _ => DEFAULT_CONTEXT_WINDOW,
            },
        },
        None => DEFAULT_CONTEXT_WINDOW,
    }
}

/// 该模型是否接受 `additionalModelRequestFields.output_config`。
///
/// 规则：代际 ≥ [`NATIVE_REASONING_MIN_GEN`] 且不在 deny-list / 排除家族里。
/// 未来的 Anthropic 新模型自动为 true —— 不需要登记。
/// OpenAI 侧走自己的 reasoning 通道，这里一律 false。
pub fn supports_native_reasoning(model_id: &str) -> bool {
    let id = model_id.trim().to_ascii_lowercase();

    {
        let state = registry().read();
        if state
            .overrides
            .allow_native_reasoning
            .iter()
            .any(|m| m.eq_ignore_ascii_case(&id))
        {
            return true;
        }
        if state
            .overrides
            .deny_native_reasoning
            .iter()
            .any(|m| m.eq_ignore_ascii_case(&id))
        {
            return false;
        }
    }

    if NATIVE_REASONING_DENY.iter().any(|m| *m == id) {
        return false;
    }

    let Some(identity) = ModelIdentity::parse(&id) else {
        return false;
    };
    if identity.vendor != Vendor::Anthropic || identity.family_blocks_reasoning() {
        return false;
    }
    identity
        .generation
        .is_some_and(|generation| generation >= NATIVE_REASONING_MIN_GEN)
}

/// 该模型是否只在 `thinking.type = "adaptive"` 下接受 `output_config`。
pub fn requires_adaptive_thinking(model_id: &str) -> bool {
    let id = model_id.trim().to_ascii_lowercase();
    ADAPTIVE_ONLY_REASONING.iter().any(|m| *m == id)
}

/// 该模型是否接受 `effort = "xhigh"`。
///
/// 代际 ≥ [`XHIGH_MIN_GEN`] 即允许；旧代际（4.5 / 4.6）与 haiku 降级到 `high`。
/// 无法解析的未知 ID 保持宽松（允许），避免误伤上游新命名。
pub fn supports_xhigh_effort(model_id: &str) -> bool {
    let id = model_id.trim().to_ascii_lowercase();

    if registry()
        .read()
        .overrides
        .deny_xhigh_effort
        .iter()
        .any(|m| m.eq_ignore_ascii_case(&id))
    {
        return false;
    }
    if XHIGH_DENY.iter().any(|m| *m == id) {
        return false;
    }

    let Some(identity) = ModelIdentity::parse(&id) else {
        // 完全无法解析 → 保持宽松，别拦住上游的新命名方案。
        return true;
    };
    if identity.family_blocks_reasoning() {
        return false;
    }
    match identity.generation {
        Some(generation) => generation >= XHIGH_MIN_GEN,
        // 有厂商但无代际（`claude-5` 之类已在 parse 里给出代际；这里是
        // `claude-unknown` 这种）→ 宽松放行。
        None => true,
    }
}

/// `-thinking` 后缀模型应使用的 `thinking.type`。
///
/// Opus 4.6 只接受 `adaptive`，其余用标准 `enabled`。
pub fn thinking_type_for(model: &str) -> &'static str {
    match resolve(model) {
        Some(id) if requires_adaptive_thinking(&id) => "adaptive",
        _ => "enabled",
    }
}

// =============================================================================
// /v1/models 目录导出
// =============================================================================

/// 一条对外广告的模型（`/v1/models` 的一项，未含 Anthropic 包装字段）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvertisedModel {
    pub id: String,
    pub display_name: String,
    pub owned_by: &'static str,
    pub max_tokens: i32,
    pub created: i64,
}

/// 导出 `/v1/models` 目录：每个模型 → 主 ID + 别名，各自再配一个
/// `-thinking` 变体（Claude Code / cc-kiro 靠它切思考模式）。
///
/// 排序：`created` 倒序（新模型在前，方便探测最新可用模型的客户端），
/// 同 `created` 时按 ID 升序保证稳定。
pub fn advertised_models() -> Vec<AdvertisedModel> {
    let state = registry().read();
    let mut sorted: Vec<&ModelEntry> = state
        .entries
        .values()
        .filter(|e| !state.hidden.iter().any(|h| h == &e.id))
        .collect();
    sorted.sort_by(|a, b| b.created.cmp(&a.created).then_with(|| a.id.cmp(&b.id)));

    let mut out = Vec::with_capacity(sorted.len() * 4);
    for entry in sorted {
        for id in std::iter::once(&entry.id).chain(entry.aliases.iter()) {
            out.push(AdvertisedModel {
                id: id.clone(),
                display_name: entry.display_name.clone(),
                owned_by: entry.vendor.owned_by(),
                max_tokens: entry.max_output_tokens,
                created: entry.created,
            });
            // `auto` 没有思考变体（上游自行决定）。
            if entry.vendor == Vendor::KiroAuto {
                continue;
            }
            out.push(AdvertisedModel {
                id: format!("{id}{THINKING_SUFFIX}"),
                display_name: format!("{} (Thinking)", entry.display_name),
                owned_by: entry.vendor.owned_by(),
                max_tokens: entry.max_output_tokens,
                created: entry.created,
            });
        }
    }
    out
}

/// 当前目录里的 Kiro 模型 ID（诊断 / 测试用）。
pub fn catalog_ids() -> Vec<String> {
    registry().read().entries.keys().cloned().collect()
}

// =============================================================================
// 上游动态刷新
// =============================================================================

/// 首次刷新前的等待时间：给 token 刷新留出窗口，避免启动瞬间打空。
const UPSTREAM_REFRESH_INITIAL_DELAY: std::time::Duration = std::time::Duration::from_secs(10);
/// 刷新周期。上游模型目录变动不频繁，6 小时足够，且几乎零成本。
const UPSTREAM_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(6 * 3600);
/// 首次失败后的重试间隔（凭据可能还没就绪）。
const UPSTREAM_REFRESH_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(300);

/// 启动后台任务，周期性地从上游 `ListAvailableModels` 同步模型目录。
///
/// 这是「新模型零改码」的关键：Kiro 上线新模型后，本进程最多一个周期内就学到
/// 它，随即可路由、可在 `/v1/models` 广告，无需升级二进制。
/// 失败只记日志 —— 内置 seed 目录保证降级后功能不变。
pub fn spawn_upstream_refresh(
    token_manager: std::sync::Arc<crate::kiro::token_manager::MultiTokenManager>,
) {
    tokio::spawn(async move {
        tokio::time::sleep(UPSTREAM_REFRESH_INITIAL_DELAY).await;
        let mut synced_once = false;

        loop {
            match token_manager.discover_available_models().await {
                Ok(resp) => {
                    update_from_upstream(&resp.models);
                    if !synced_once {
                        tracing::info!(
                            upstream_models = resp.models.len(),
                            catalog_size = catalog_ids().len(),
                            "model_registry: 已与上游 ListAvailableModels 同步"
                        );
                        synced_once = true;
                    }
                    tokio::time::sleep(UPSTREAM_REFRESH_INTERVAL).await;
                }
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        "model_registry: 上游模型目录同步失败，继续使用内置目录"
                    );
                    tokio::time::sleep(if synced_once {
                        UPSTREAM_REFRESH_INTERVAL
                    } else {
                        UPSTREAM_REFRESH_RETRY_DELAY
                    })
                    .await;
                }
            }
        }
    });
}

/// 重置为初始 seed 状态（仅测试使用）。
#[cfg(test)]
fn reset_for_test() {
    let mut state = registry().write();
    *state = RegistryState::new();
}

/// 测试串行化锁：注册表是进程级全局状态，改它的测试不能并发。
#[cfg(test)]
fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kiro::model::available_models::UpstreamModel;

    fn upstream(id: &str, name: Option<&str>, max_input: Option<i64>) -> UpstreamModel {
        // UpstreamModel 只实现了 Deserialize，借 JSON 构造。
        let mut obj = serde_json::Map::new();
        obj.insert("modelId".into(), serde_json::json!(id));
        if let Some(name) = name {
            obj.insert("modelName".into(), serde_json::json!(name));
        }
        if let Some(max_input) = max_input {
            obj.insert(
                "tokenLimits".into(),
                serde_json::json!({ "maxInputTokens": max_input }),
            );
        }
        serde_json::from_value(serde_json::Value::Object(obj)).expect("构造 UpstreamModel")
    }

    // ---- 解析 ----

    #[test]
    fn parse_extracts_vendor_family_generation() {
        let id = ModelIdentity::parse("claude-sonnet-4-5-20250929").unwrap();
        assert_eq!(id.vendor, Vendor::Anthropic);
        assert_eq!(id.family.as_deref(), Some("sonnet"));
        assert_eq!(id.generation, Some(Generation::new(4, 5)));
        assert!(!id.thinking_suffix);
    }

    #[test]
    fn parse_strips_thinking_suffix() {
        let id = ModelIdentity::parse("claude-opus-4.8-thinking").unwrap();
        assert_eq!(id.generation, Some(Generation::new(4, 8)));
        assert!(id.thinking_suffix);
        assert_eq!(id.base_id, "claude-opus-4.8");
    }

    #[test]
    fn parse_handles_glued_and_dotted_forms() {
        for raw in ["claude-sonnet5", "claude-sonnet.5", "claude-sonnet-5"] {
            let id = ModelIdentity::parse(raw).unwrap();
            assert_eq!(id.family.as_deref(), Some("sonnet"), "{raw}");
            assert_eq!(id.generation, Some(Generation::new(5, 0)), "{raw}");
        }
    }

    #[test]
    fn parse_ignores_date_stamp_as_version() {
        let id = ModelIdentity::parse("claude-sonnet-5-20260101").unwrap();
        assert_eq!(
            id.generation,
            Some(Generation::new(5, 0)),
            "8 位日期戳不能被当成次版本号"
        );
    }

    #[test]
    fn parse_does_not_read_version_before_family() {
        // legacy `claude-3-5-sonnet`：数字在家族名之前，不构成代际。
        let id = ModelIdentity::parse("claude-3-5-sonnet-20241022").unwrap();
        assert_eq!(id.family.as_deref(), Some("sonnet"));
        assert_eq!(id.generation, None);
    }

    #[test]
    fn parse_openai_generation() {
        let id = ModelIdentity::parse("gpt-5.6-sol").unwrap();
        assert_eq!(id.vendor, Vendor::OpenAI);
        assert_eq!(id.generation, Some(Generation::new(5, 6)));
    }

    #[test]
    fn parse_rejects_non_kiro_vendors() {
        assert!(ModelIdentity::parse("llama-3-70b").is_none());
        assert!(ModelIdentity::parse("").is_none());
    }

    // ---- 核心目标：新模型免改码 ----

    #[test]
    fn future_anthropic_models_route_without_registration() {
        let _guard = test_lock();
        reset_for_test();
        // 这些 ID 都不在 seed 目录里，代际高于已知最高 → 结构化透传。
        assert_eq!(
            resolve("claude-sonnet-6-2"),
            Some("claude-sonnet-6.2".to_string())
        );
        assert_eq!(
            resolve("claude-opus-6-20270301-thinking"),
            Some("claude-opus-6".to_string())
        );
        assert_eq!(
            resolve("claude-sonnet-5-1"),
            Some("claude-sonnet-5.1".to_string())
        );
    }

    #[test]
    fn future_openai_models_route_without_registration() {
        let _guard = test_lock();
        reset_for_test();
        assert_eq!(resolve("gpt-6-nova"), Some("gpt-6-nova".to_string()));
        assert_eq!(resolve("gpt-5.9-aurora"), Some("gpt-5.9-aurora".to_string()));
        // 旧代际仍不服务。
        assert_eq!(resolve("gpt-4"), None);
        assert_eq!(resolve("gpt-4o"), None);
    }

    #[test]
    fn future_models_inherit_new_generation_capabilities() {
        let _guard = test_lock();
        reset_for_test();
        // 代际 ≥ 4.6 → 1M 窗口 + 原生 reasoning；≥ 4.7 → xhigh。
        assert_eq!(context_window("claude-sonnet-6-2"), LARGE_CONTEXT_WINDOW);
        assert!(supports_native_reasoning("claude-sonnet-6.2"));
        assert!(supports_xhigh_effort("claude-sonnet-6.2"));
        // 新 OpenAI 模型拿到 272K，但不下发 output_config。
        assert_eq!(context_window("gpt-6-nova"), OPENAI_CONTEXT_WINDOW);
        assert!(!supports_native_reasoning("gpt-6-nova"));
    }

    #[test]
    fn legacy_models_stay_unsupported() {
        let _guard = test_lock();
        reset_for_test();
        assert_eq!(resolve("claude-3-5-sonnet-20241022"), None);
        assert_eq!(resolve("claude-3-opus"), None);
    }

    // ---- 目录吸附 ----

    #[test]
    fn below_known_minimum_generation_snaps_to_lowest_entry() {
        let _guard = test_lock();
        reset_for_test();
        // haiku 目录里只有 4.5；`claude-haiku-4-20250514` 解析为 4.0 → 吸附。
        assert_eq!(
            resolve("claude-haiku-4-20250514"),
            Some("claude-haiku-4.5".to_string())
        );
    }

    #[test]
    fn aliases_and_canonical_ids_both_resolve() {
        let _guard = test_lock();
        reset_for_test();
        for raw in [
            "claude-opus-4-8",
            "claude-opus-4.8",
            "claude-opus-4-8-thinking",
        ] {
            assert_eq!(resolve(raw), Some("claude-opus-4.8".to_string()), "{raw}");
        }
    }

    // ---- 上游学习 ----

    #[test]
    fn upstream_learned_model_routes_and_advertises() {
        let _guard = test_lock();
        reset_for_test();
        // 一个完全陌生的家族，seed 里没有。
        assert_eq!(resolve("claude-nimbus-7"), Some("claude-nimbus-7".to_string()));

        update_from_upstream(&[upstream(
            "claude-nimbus-7",
            Some("Claude Nimbus 7"),
            Some(2_000_000),
        )]);

        assert_eq!(resolve("claude-nimbus-7"), Some("claude-nimbus-7".to_string()));
        assert_eq!(
            context_window("claude-nimbus-7"),
            2_000_000,
            "上游 maxInputTokens 应覆盖推断值"
        );
        let ids: Vec<String> = advertised_models().into_iter().map(|m| m.id).collect();
        assert!(ids.contains(&"claude-nimbus-7".to_string()));
        assert!(ids.contains(&"claude-nimbus-7-thinking".to_string()));
        reset_for_test();
    }

    #[test]
    fn upstream_refresh_overrides_known_context_window() {
        let _guard = test_lock();
        reset_for_test();
        update_from_upstream(&[upstream("claude-sonnet-4.5", None, Some(500_000))]);
        assert_eq!(context_window("claude-sonnet-4-5-20250929"), 500_000);
        reset_for_test();
    }

    #[test]
    fn empty_upstream_list_keeps_seed_catalog() {
        let _guard = test_lock();
        reset_for_test();
        let before = catalog_ids().len();
        update_from_upstream(&[]);
        assert_eq!(catalog_ids().len(), before);
    }

    // ---- 配置 override ----

    #[test]
    fn config_override_declares_new_model_params() {
        let _guard = test_lock();
        reset_for_test();
        let mut overrides = ModelRegistryOverrides::default();
        overrides.models.insert(
            "claude-zephyr-9".to_string(),
            ModelOverride {
                display_name: Some("Claude Zephyr 9".to_string()),
                context_window: Some(3_000_000),
                max_output_tokens: Some(128_000),
                aliases: vec!["zephyr".to_string()],
                owned_by: Some("anthropic".to_string()),
                advertise: None,
            },
        );
        overrides
            .deny_native_reasoning
            .push("claude-zephyr-9".to_string());
        set_overrides(overrides);

        assert_eq!(resolve("zephyr"), Some("claude-zephyr-9".to_string()));
        assert_eq!(context_window("claude-zephyr-9"), 3_000_000);
        assert!(
            !supports_native_reasoning("claude-zephyr-9"),
            "配置 deny 应压过代际规则"
        );
        let advertised = advertised_models();
        let entry = advertised
            .iter()
            .find(|m| m.id == "claude-zephyr-9")
            .expect("override 模型应被广告");
        assert_eq!(entry.max_tokens, 128_000);
        assert_eq!(entry.display_name, "Claude Zephyr 9");

        reset_for_test();
    }

    #[test]
    fn config_override_can_force_allow_native_reasoning() {
        let _guard = test_lock();
        reset_for_test();
        // sonnet-4.8 内置 deny；配置允许后应放行。
        assert!(!supports_native_reasoning("claude-sonnet-4.8"));
        let overrides = ModelRegistryOverrides {
            allow_native_reasoning: vec!["claude-sonnet-4.8".to_string()],
            ..Default::default()
        };
        set_overrides(overrides);
        assert!(supports_native_reasoning("claude-sonnet-4.8"));
        reset_for_test();
    }

    #[test]
    fn config_override_can_hide_model() {
        let _guard = test_lock();
        reset_for_test();
        let mut overrides = ModelRegistryOverrides::default();
        overrides.models.insert(
            "claude-haiku-4.5".to_string(),
            ModelOverride {
                advertise: Some(false),
                ..Default::default()
            },
        );
        set_overrides(overrides);
        let ids: Vec<String> = advertised_models().into_iter().map(|m| m.id).collect();
        assert!(!ids.iter().any(|id| id.starts_with("claude-haiku")));
        // 隐藏 ≠ 不可路由。
        assert_eq!(
            resolve("claude-haiku-4-5-20251001"),
            Some("claude-haiku-4.5".to_string())
        );
        reset_for_test();
    }

    // ---- /v1/models 目录 ----

    #[test]
    fn advertised_catalog_has_aliases_thinking_variants_newest_first() {
        let _guard = test_lock();
        reset_for_test();
        let models = advertised_models();
        let ids: Vec<&str> = models.iter().map(|m| m.id.as_str()).collect();

        for expected in [
            "claude-opus-4.8",
            "claude-opus-4-8",
            "claude-opus-4-8-thinking",
            "claude-sonnet-5",
            "claude-sonnet-5-thinking",
            "claude-fable-5",
            "gpt-5.6-sol",
            "auto",
        ] {
            assert!(ids.contains(&expected), "{expected} 应被广告");
        }
        // auto 不产生 thinking 变体。
        assert!(!ids.contains(&"auto-thinking"));
        // created 倒序：新模型在前。
        let created: Vec<i64> = models.iter().map(|m| m.created).collect();
        assert!(
            created.windows(2).all(|w| w[0] >= w[1]),
            "广告顺序应按 created 倒序"
        );
    }

    #[test]
    fn thinking_type_is_decided_per_model() {
        let _guard = test_lock();
        reset_for_test();
        assert_eq!(thinking_type_for("claude-opus-4-6-thinking"), "adaptive");
        assert_eq!(thinking_type_for("claude-opus-4-8-thinking"), "enabled");
        assert_eq!(thinking_type_for("claude-sonnet-6-1-thinking"), "enabled");
    }

    #[test]
    fn auto_model_routes_with_large_window() {
        let _guard = test_lock();
        reset_for_test();
        assert_eq!(resolve("auto"), Some("auto".to_string()));
        assert_eq!(context_window("auto"), LARGE_CONTEXT_WINDOW);
    }

    #[test]
    fn generation_compares_ordinally_not_lexically() {
        assert!(Generation::new(5, 0) > Generation::new(4, 8));
        assert!(Generation::new(4, 10) > Generation::new(4, 9));
        assert!(Generation::new(10, 0) > Generation::new(9, 9));
    }
}

