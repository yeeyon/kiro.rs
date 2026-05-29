# Changelog

All notable changes to this project are documented in this file. The format
loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.5.1] - 2026-05-29

主题：彻底重构 prompt cache 与计费指标——上游 `meteringEvent` 实测只下发 `credit`、不带 token / cache 字段，因此把基础设施收敛到「进程内」：移除 Redis 依赖、按 Anthropic `cache_control` 多断点协议在中转层自建 prompt cache、把 `credit` 作为新维度贯穿后端聚合 → API → 前端。仪表盘同步重做：5 系列图表 + 双 Y 轴 + K/M/B 紧凑数值 + 卡片随时间窗切换。

> 0.5.0 因 Credit 数值显示问题被作废（`formatCredits` 在 `value ≥ 1` 时直接打印完整浮点）；0.5.1 修复该问题并整合所有内容，请所有用户直接升级到 0.5.1，跳过 0.5.0。

### 💥 Breaking — 基础设施

- **彻底移除 Redis 依赖**：`anthropic/cache.rs` 整模块删除（约 740 行），`Cargo.toml` 删 `redis` crate，`docker-compose.yml` 删 `redis` 服务、`depends_on`、`redis-data` 命名卷，`config.example.json` 删 `redisUrl` / `cacheDebugLogging` / `cacheMaxReadRatio`，对应的 `Config::redis_url` / `cache_debug_logging` / `cache_max_read_ratio` 字段也删。已有部署里这三个配置字段会被忽略；不会破坏功能（只是无法识别），但**升级前请把它们从 `data/config.json` 删掉以免日后误以为还在生效**。
- **API 响应字段含义变化**：`/v1/messages` 响应里的 `usage.cache_creation_input_tokens` / `cache_read_input_tokens` 不再是「Redis 缓存」（已下线）也不是「Anthropic 上游缓存」（实测上游不下发），而是**中转层自己根据请求体 `cache_control` 断点产出的提示词缓存计数**。详见下方"中转层 Prompt Cache"章节。
- **`UsageRecordHook::record` 签名加 `credits: f64` 参数**；`ClientKeyManager::record_usage` 同步加。下游若 fork 了 handler 调用链需要补一个参数。

### ✨ 新功能 — 中转层 Prompt Cache（无外部依赖）

- **进程内提示词缓存**：新模块 `src/anthropic/prompt_cache.rs`。按 Anthropic 协议把请求体里 `cache_control` 断点（最多 4 个，分布于 `tools` / `system` / `messages[].content`）切成一组前缀段，对每段累加 SHA-256 哈希作为 key，TTL 默认 5 分钟、`cache_control.ttl="1h"` 解析为 1 小时。
  - **命中规则**：取最深命中段索引 `i*` → `cache_read = segments[i*].cumulative_tokens`，`cache_creation = total - segments[i*].cumulative_tokens`；全部 miss 时 `cache_creation = total`、`cache_read = 0`。每次请求结束时把所有段（命中 / 未命中）写回，刷新 LRU `last_hit_at` 与 TTL。
  - **持久化**：cache_dir 下 `prompt_cache.json`（按字节哈希 → `{tokens, expires_at, last_hit_at}`），后台 60s 一次 flush（仅 dirty 时落盘），启动时过滤过期条目重建。LRU 上限 4096 条。
- **流式 / 非流式两路接线**：`StreamContext` / `BufferedStreamContext` 新增 `set_initial_cache_tokens(cc, cr)`。`message_start` / `message_delta.usage` 与非流响应的 `usage.cache_creation_input_tokens` / `cache_read_input_tokens` 全部由 PromptCache 真实产出，不再硬编码 0。
- **真实验证**：两次完全相同的 `/v1/messages` 请求（带 `cache_control: ephemeral` 系统提示），第一次 `cache_creation=94 / cache_read=0`，第二次 `cache_creation=0 / cache_read=94`，精确按协议工作。
- **9 个新单测**覆盖 lookup / record / TTL / LRU / flush + reload / 多断点命中。

### ✨ 新功能 — Credit 计费维度

- **解析上游 meteringEvent**：之前 `Event::Metering` 被丢成 `()`。新模块 `src/kiro/model/events/metering.rs` 严格解析真实 payload `{unit, unitPlural, usage(f64)}`（实测确认上游不下发 token / cache 字段；不做字段名候选 fallback，直接读 `usage`）。
- **Credit 全链路**：`UsageRecord` / `BucketStats` / `TimeSeriesPoint` / `OverviewStats` / `ClientKey` 全部新增 `credits` 字段；流式 / 非流式 hook 都把 `credits` 累加并写入。
- **API 暴露**：`GET /api/admin/stats/overview` 多 `todayCredits` / `weekCredits`；`GET /api/admin/stats/timeseries` 每个时序点多 `credits`。
- **前端展示**：概览页顶部新增 "近 X Credit" 卡片（grid 由 4 列改为 5 列）；时序图 Tooltip 单独一行展示「本桶 Credit」（量级与 token 差异过大，不画线）。

### ✨ 新功能 — 仪表盘改造

- **Token 使用趋势图重做**（`time-series-chart.tsx`）：5 系列折线（Input / Output / Cache Creation / Cache Read / Cache Hit Rate），双 Y 轴：左轴 token 量级（紧凑 K/M/B），右轴 0–100% 命中率（紫色虚线，刻度固定 [0, 20, 40, 60, 80, 100]）；自定义深色 Tooltip，命中率 = `cacheRead / (input + cacheRead)`。全零数据时左轴强制显示 `0` 刻度，避免空白图表；Legend 改空心圆 + 英文标签。
- **顶部卡片随时间窗切换**：之前调用 / Token 卡片永远显示「今日」，新增 `useMemo` 把当前 `seriesData` 按 24h / 7d / 30d 聚合，标题动态变成"近 24 小时调用 / 输入 Token"等。`activeClientKeys` 仍是当前活跃数。
- **数值紧凑格式 K/M/B**：新增 `formatNumber()` 工具（基于 `Intl.NumberFormat` compact notation），覆盖概览卡片 / 模型表 / 凭据柱图 / 时序图 / 凭据列表 Badge。`formatCredits()` 对 credit 浮点专用：`≤ 0` → `"0"`、`< 1000` → 3 位小数、`≥ 1000` → K/M/B。Y 轴 / Tooltip / 表格全走同一格式器。
- **凭据柱图按 email 显示**：之前 X 轴 label 是 `#id`（email 字段始终空），后端 `stats_by_credential` 在 handler 拼装时已经反查注入了 `email`，前端改为以 email 为主、`#id` 兜底；过长 email 截断到 22 字符（保留 @domain），完整 email 在 Tooltip 显示。

### ✨ 新功能 — KAM 凭据导出

- **新端点 `GET /api/admin/credentials/export?ids=...`**：导出选中凭据为 KAM 1.8.3+ 平铺 JSON 格式，含 `refreshToken` / `accessToken` / `clientSecret` 等敏感字段。
- **`MultiTokenManager::clone_all_credentials`** 用于 admin 服务层取完整凭据快照（脱敏由调用方控制）。
- **新 admin-ui 类型 `KamExportAccount` / `KamExportResponse`**，前端凭据列表批量选择后可一键下载。

### 🛠 修复

- **Credit 数值小数位失控（0.5.0 → 0.5.1）**：`formatCredits()` 中 `value ≥ 1` 的分支会回退到 `formatNumber`，而 `formatNumber` 对 `< 1000` 的数直接 `String(value)`，导致 `1.5755479141293534` 这类长浮点被原样打印。修复后统一规则：
  - `≤ 0 / null / NaN` → `"0"`
  - `0 < value < 1000` → 保留 3 位小数（`1.576` / `0.017`）
  - `value ≥ 1000` → `Intl.NumberFormat` compact notation（`1.2K` / `3.4M`）
- **重启后用量统计丢失**：根因是当 `--credentials credentials.json`（无目录前缀）启动时，`PathBuf::from("credentials.json").parent()` 返回 `Some("")`，导致 `cache_dir = ""`：`UsageRecorder` 把 `usage_log.*.jsonl` 写到 CWD（路径无前缀），`UsageAggregator::rebuild_from_logs("")` 调用 `read_dir("")` 失败，重启后历史记录看似全丢。修复：`MultiTokenManager::cache_dir()` 与 `UsageRecorder::new` / `rebuild_from_logs` 都把空路径归一为 `.`，并把"创建目录失败 / 读取目录失败"由静默 `_` 改成 `tracing::warn!` 显式打印路径。重建完成日志带上目录与条目数。
- **`StatsResponse` 不再有 `let mut overview = ...` + `let _ = (&mut overview).today_calls;` 这种 dead-code 黑魔法**——直接用不可变 `overview`。

### 🎨 体验

- **API Key 随机生成器收紧**：之前默认 40 字节 base64url，会产生 `sk-admin--Wt2ZN...` 这种双连字符的视觉断裂。改为：字符表只含 `a-zA-Z0-9`（拒绝采样保证均匀），32 字符（~190 bit 熵），按对话框模式选择前缀（admin Key 用 `sk-admin-`，业务 Key 仍用 `sk-kiro-`）。**移除 `Math.random` 弱熵 fallback**，缺 `crypto.getRandomValues` 时直接抛错。

### 📦 依赖 / 构建

- **删除依赖**：Rust 端 `redis = "0.27"`。
- **前端构建分块**：`recharts` 及其 d3 依赖链单独成块（约 410 KB / gzip 106 KB），仅"概览"路由懒加载触发；`vendor` chunk 从 510 KB 缩到 69 KB；`sonner` 也单独成块；`chunkSizeWarningLimit` 提到 600 KB。
- **`.gitignore` / `.dockerignore`** 新增 `prompt_cache.json`（运行时落盘，不入库）。
- **测试覆盖**：单测从 233 增到 237（PromptCache 9 + Metering 2 - 现有路径调整）。

### 📦 升级指南

1. **`docker compose pull && docker compose up -d`** 即可。如果之前部署了 `redis` 服务，可以一并停掉删掉（数据无价值）。
2. **删除过时配置**：编辑 `data/config.json`，删除 `redisUrl` / `cacheDebugLogging` / `cacheMaxReadRatio` 三个字段（保留也只是被忽略，不会报错）。
3. **下游客户端**：响应里的 `cache_creation_input_tokens` / `cache_read_input_tokens` 字段含义变了——现在反映的是中转层提示词缓存而非上游缓存。如果下游用这两个字段做计费对账，需要重新理解口径（中转层缓存命中并不会减少上游 credit 消耗，是 SDK 体验优化）。
4. **历史用量**：`usage_log.*.jsonl` 的旧记录会被自动加载（`credits` 字段缺失时默认 0），重启不丢趋势。新的请求开始会带 credit。
5. **若你已经升级到 0.5.0**：直接升 0.5.1；不需要清理任何状态文件。

## [0.4.0] - 2026-05-22

主题：把 kiro.rs 从「单 Key 的 Anthropic 协议适配器」推进到 Key 分发场景——加入面向下游用户的客户端 Key 分发、按 Key/凭据/模型维度的 Token 用量统计与仪表盘趋势可视化。

### ✨ 新功能 — 客户端 API Key 分发

- **新的两层 Key 模型**：`config.apiKey`（master）保留向后兼容，新增 `csk_*` 客户端 Key 层。每把 Key 独立启用/禁用、独立计数，泄露后只需替换一把而非全员换 master。
  - 持久化到 `client_api_keys.json`（与 `credentials.json` 同目录），无 SQLite 依赖
  - `subtle::ConstantTimeEq` 全表常量时间比对，防 HashMap 短路引发的时序攻击
  - 鉴权顺序：master apiKey → 客户端 Key；命中后通过 `Extension(KeyContext { key_id })` 注入下游 handler
- **Admin API**：6 个新端点
  - `GET /api/admin/client-keys` 列表（脱敏展示 `csk_abcd...mnop`）
  - `POST /api/admin/client-keys` 创建（响应里返回明文 key，**仅此一次**）
  - `PUT /api/admin/client-keys/:id` 改名 / 改描述
  - `DELETE /api/admin/client-keys/:id` 删除
  - `POST /api/admin/client-keys/:id/disabled` 启用/禁用
  - `POST /api/admin/client-keys/:id/reset-stats` 重置累计计数
- **新前端 Tab「客户端 Key」**：表格展示名称、脱敏 Key、状态、总调用、总输入/输出 Token、最后使用时间、操作按钮；新建后弹出明文一次性展示对话框（带显示/隐藏切换、复制按钮）。

### ✨ 新功能 — Token 用量统计与仪表盘

- **请求级用量记录**：`/v1/messages` 流式 / 缓冲流式 / 非流式三条路径在结束（含错误）时统一写入用量。`KiroProvider` 改造返回 `KiroCallResult { response, credential_id }`，把命中凭据 ID 透传到 handler 用于按上游凭据维度聚合。
- **JSONL 持久化 + 内存聚合**：
  - `usage_log.YYYY-MM-DD.jsonl` 按日滚动，单行一条记录（ts/keyId/credentialId/model/inputTokens/outputTokens/cacheCreation/cacheRead/durationMs/status）
  - `UsageAggregator` 维护 168 小时桶 + 31 天桶的 ring buffer，启动时从历史 JSONL 重建，重启不丢趋势
  - 后台任务每 24 小时清理超过 31 天的旧日志
- **统计 API**：4 个新端点
  - `GET /api/admin/stats/overview` — 今日 / 最近 7 天的调用次数、Token、错误数 + 活跃 Key/凭据数
  - `GET /api/admin/stats/timeseries?range=24h|7d|30d` — 按桶聚合的时序点
  - `GET /api/admin/stats/by-model?range=...` — 各模型的 calls / input / output 排行
  - `GET /api/admin/stats/by-credential?range=...` — 各上游凭据贡献，附 email
- **新前端 Tab「概览」**：4 张统计卡片 + 三类图表
  - 时间 × Token 折线图（input/output/cacheRead/cacheCreation 四条线）
  - 按模型分布饼图 + 详情表
  - 按上游凭据堆叠柱图（Top 12）
  - 右上 24h / 7d / 30d 切换器
- **客户端 Key 维度的累计**：成功请求会同时把 input/output/cacheCreation/cacheRead 累加到对应客户端 Key 的总数，列表页直接看到每把 Key 的总消耗。

### 🎨 界面 — 多 Tab 导航 + 顶栏统一

- **从单 Dashboard 改为三 Tab SPA**：概览（默认）/ 凭据管理 / 客户端 Key。`App.tsx` 顶栏内置 Tab，URL hash（`#/overview` / `#/credentials` / `#/keys`）同步，未引入 react-router。
- **`TopbarTools` 工具组件**：把"负载均衡切换 / 刷新 / 在线更新 / 设置（含 Key 修改对话框）"从凭据管理 Tab 抽到 App 顶栏，三个 Tab 都可访问；刷新按钮一次性失效凭据 / 客户端 Key / stats 三类查询。
- **响应式 Tab 行**：桌面端 Tab 在 logo 旁，移动端折到顶栏第二行。
- **Dashboard 嵌入模式**：新增 `embedded` prop，在 Tab 内渲染时隐藏自带顶栏、跳过外层 padding，避免与 App 顶栏重复。

### 🛠 性能 / 体验

- **图表渲染优化**：三个 chart 全部 `React.memo` + `useMemo` 稳定 props 引用，关闭 recharts 默认 1.5s 入场动画；时序图根据点数自动稀疏 X 轴 ticks（≤12 全显，≤48 取 12 个，更长取 16 个）避免标签重叠引发的反复布局测量。
- **数据查询节流**：所有 stats hook 加 `staleTime: 25s`（30s refetchInterval 之内切 Tab 不重复请求）+ `placeholderData: keepPreviousData`（切 range 期间复用旧数据避免 chart 卸载重挂）+ `refetchOnWindowFocus: false`（避免窗口聚焦同时打 4 个请求）。
- **图表 Tooltip 暗色主题**：抽出 `tooltip-style.ts` 共享样式，`labelStyle` / `itemStyle` 单独设白色——recharts 不让 label/item 继承 `contentStyle.color`，这是之前看不清的根因。
- **柱图布局修复**：图例从底部移到右上，X 轴 `height: 56` + bottom margin `48`，避免「输入/输出」图例覆盖倾斜的 X 轴标签。

### 📦 依赖 / 构建

- **新增前端依赖**：`recharts ^2.15`（仪表盘图表，~95KB gzip）。
- **`.gitignore` 新增 4 类条目**：`client_api_keys.json`（含明文 csk）、`usage_log.*.jsonl`、`usage_stats.json`、`*.staged-*` / `*.backup`（在线更新产物）。

### 📦 升级指南

1. **现有部署直接 `docker compose pull && docker compose up -d`**，旧 master `apiKey` 完全兼容，所有现有客户端无需改动。
2. **想用客户端 Key 分发**：登录 Admin 面板 → 切到「客户端 Key」Tab → 新建 → 把弹窗里的明文 `csk_xxx` 给下游用户，让客户端把它放进 `x-api-key` 或 `Authorization: Bearer` 头。
3. **想看仪表盘**：`/admin` → 概览 Tab，新部署默认无历史数据，发起几次请求即可看到趋势开始填充。
4. **历史日志**：服务启动时自动从 `usage_log.*.jsonl` 重建近 31 天聚合，无需迁移脚本。

## [0.3.2] - 2026-05-22

主题：把在线更新对话框打磨成可日常使用的工具——加入 GitHub Token 配置消除限流问题，加入版本验证防止重复更新，加入 staged 复用让两步操作变成无缝衔接，并清理视觉噪音。

### ✨ 新功能

- **GitHub Token 配置**：在线更新对话框新增 GitHub Personal Access Token 输入区，保存后所有 GitHub API 调用都会带上 `Authorization: Bearer <token>`，把限流从匿名 60/小时 提升到认证 5000/小时。匿名访问触发 `403 API rate limit exceeded` 时不再无解。
  - 配置文件新增 `githubToken` 字段（顶层）
  - Admin API：`GET /api/admin/config/update` 返回 `githubTokenSet: bool`（不回明文，避免泄露），`PUT /api/admin/config/update` 接受 `githubToken: string`（空字符串表示清除）
- **Token 验证 + 限流可视化**：新增 `POST /api/admin/system/update/rate-limit` 端点，调用 GitHub `/rate_limit` 实时返回当前限额状态。该 GitHub 端点本身不消耗任何配额，可放心反复调用。
  - 前端在 token 输入框旁加「验证」按钮：保存前用输入的 token 试一次，避免保存了无效 token
  - 对话框打开时自动用已保存 token 查一次限额，展示「已认证 / 匿名」徽章、`@username`、`已用 N/上限`、进度条、重置时间
  - 剩余次数低于上限 5% 时进度条变 amber 提醒
- **「上次更新于」时间戳**：apply 成功后记录 RFC3339 时间到 `updateLastAppliedAt` 字段，对话框展示「上次更新于：YYYY-MM-DD HH:MM:SS」（本地时区）。回退时清空。

### 🛠 体验优化

- **拉取镜像 → 更新并重启 复用 staged**：「拉取镜像」按钮不再是死功能。下载产物保存到 `<exe>.staged-<version>`，「更新并重启」检测到同版本 staged 时直接 install + exit，跳过重复下载。两步操作之间几乎无感知延迟。
- **当前已是最新版本时禁用「更新并重启」**：避免对相同版本做无意义的下载-替换-重启。后端在 `apply_image_update` 入口加版本检查，前端按钮根据 `hasUpdate` 同步禁用，鼠标悬停显示原因。
- **GitHub Token Scopes 不再展示**：原本会把 token 的 OAuth scopes 列出来（如 `admin:org, repo, ...`），是不必要的权限信息泄露。后端不再读取 `X-OAuth-Scopes` header，前端不再显示 Scopes 行。

### 🎨 界面调整

- **更新对话框扁平化**：移除外层卡片包装与 4 层嵌套边框，三个分区改为 `<section>` + `border-t pt-4` 顶分隔线。
- **取消「有更新」时整块变黄**：原本有更新时整个面板背景变 amber，已经有绿色「可更新」徽章传达同样信息。现在面板始终是中性背景，只保留徽章。
- **限流摘要卡内嵌**：限流状态展示不再是独立带边框的卡片，而是直接平铺在 GitHub Token 区下方，仅用图标颜色（绿/红）和进度条颜色（绿/黄）区分状态。

## [0.3.1] - 2026-05-22

### ⚠️ 不兼容变更（Breaking changes）

- **配置字段清理**：`config.json` 删除 `updateImage` 与 `updatePreviousImage` 字段，新增 `updatePreviousVersion`。`updateImage` 在新方案里没有意义（在线更新已不再操作 docker 镜像），保留只会误导。已存在的 `updateImage` 字段会被静默忽略。
- **Admin API 响应字段调整**：`GET /api/admin/config/update` 返回值移除 `image`，把 `previousImage` 改为 `previousVersion`；`PUT /api/admin/config/update` 不再接受 `image` 参数；`POST /api/admin/system/update/{pull,apply,rollback}` 响应移除 `image` 字段。前端已同步更新。
- **`docker-compose.yml` 移除 docker socket 与 compose 文件挂载**：在线更新不再需要这两个挂载点。继续使用旧 compose 文件部署也能跑通，但会带着不必要的安全风险。

### 🛠 在线更新机制改造

- **从「容器自管自重建」改为「文件级二进制替换」**：`apply_image_update` 不再调用 `docker compose pull/up`，改成下载 GitHub Releases 上对应平台的二进制压缩包，校验 `SHA256SUMS.txt`，原子替换 `<exe>`，旧版本备份为 `<exe>.backup`，最后调用 `std::process::exit(0)` 退出，由 `docker-compose.yml` 里的 `restart: unless-stopped` 接管重启。这样从根本上消除了"网络错误时旧容器被停止、新镜像没拉到、服务挂起"的事故路径。
- **回退也改为文件级**：`rollback_image_update` 从 `<exe>.backup` 还原可执行文件并退出进程，不再依赖 `kiro-rs:rollback` 镜像 tag，断网也能恢复。
- **`check_update` 统一走 GitHub Releases API**：取消对 Docker Hub `/v2/repositories/.../tags` 的依赖，单一 endpoint 既拿版本号又拿 changelog，请求次数减半。
- **移除 docker socket 与 docker CLI 依赖**：`Dockerfile` / `Dockerfile.release` 不再安装 `docker-cli` 与 `docker-cli-compose`；`docker-compose.yml` 删除 `/var/run/docker.sock` 与 `docker-compose.yml` 的挂载。镜像体积更小，容器逃逸面显著缩小。
- **删除 600+ 行旧逻辑**：`ComposeContext` / `detect_compose_metadata` / `tag_rollback_image` / `validate_image_ref` / `dockerhub_owner_repo` / `DockerHubTagsResponse` 等 docker 相关代码全部移除；`UpdateConfigResponse` / `ImageUpdateResponse` / `SetUpdateConfigRequest` 同步精简。
- **前端 UI 同步**：「在线更新」对话框移除「镜像」输入框与「保存配置」按钮（这两个控件操作的字段已不存在），保留「拉取镜像」「更新并重启」「回退到上一版本」三大功能按钮的位置、名称、操作流程不变。
- 配套加 `flate2` / `tar` / `zip` 依赖用于解压 release archive。

### 🚀 CI/CD 加速

- **前端只构建一次**：新增 `build-frontend` job，跑一次 `bun run build` 并把 `admin-ui/dist` 上传为 artifact；后续 7 个二进制矩阵 + 2 个镜像矩阵直接 `download-artifact` 复用，多平台 runner 不再重复装 Bun / 跑 vite。
- **release profile 调优**：`Cargo.toml` 把 `lto = true`（fat）改为 `lto = "thin"` + `codegen-units = 16`，单作业 `cargo build` 的链接耗时显著下降，对运行时性能影响可忽略。
- **Docker 镜像复用预编译二进制**：新增 `Dockerfile.release`，CI 里 `build-images` 改为 `needs: build-artifacts`，下载已经构建好的 `Linux-musl-x64` / `Linux-musl-arm64` 二进制后直接 `COPY` 进 alpine，跳过 Dockerfile 内重复的 cargo 编译阶段。开发用 `Dockerfile`、`docker-build.yaml` 仍走完整源码构建。
- **mold linker（Linux gnu 目标）**：在 `x86_64-unknown-linux-gnu` / `aarch64-unknown-linux-gnu` 矩阵上通过 `rui314/setup-mold@v1` 启用 mold，`RUSTFLAGS=-C link-arg=-fuse-ld=mold`，链接阶段从 5–15s 降至 1–3s。macOS / Windows / musl 目标保持默认链接器以避开兼容性风险。
- **`cargo build` 全部加 `--locked`**：确保 CI 构建严格按提交的 `Cargo.lock` 解析，避免锁文件漂移导致重复编译。

### 📦 升级指南

1. **保留 docker compose 部署的用户**：直接 `docker compose pull && docker compose up -d` 升到 0.3.1；老 compose 文件里的 `docker.sock` / `docker-compose.yml` 挂载可以从下次 PR 起删掉，不影响功能。
2. **手动跑二进制的用户**：从 GitHub Releases 下载新版本替换原有二进制即可。
3. **配置文件清理**：可以从 `data/config.json` 中删除 `updateImage` / `updatePreviousImage` 字段，服务不会再使用它们。

## [0.3.0] - 2026-05-22

### ⚠️ 不兼容变更（Breaking changes）

- 容器发布渠道从 GitHub Container Registry **迁移到 Docker Hub**。
  - 默认镜像由 `ghcr.io/zyphrzero/kiro-rs:latest` 改为 `zyphrzero/kiro-rs:latest`。
  - 旧的 GHCR 镜像 **不再发布新版本**；继续使用 GHCR 的部署需要把镜像引用改回 `ghcr.io/...` 自行同步。
- 配置文件移除以下字段（直接删除即可，迁移逻辑参见下方"在线更新"小节）：
  - `githubToken`
  - `updateComposeFile`
  - `updateService`
- `docker-compose.yml` 默认镜像同步切换到 Docker Hub。

### 🛠️ 构建工具链升级

- **包管理器迁移到 Bun**
  - 删除 `pnpm-lock.yaml` / `pnpm-workspace.yaml` / `.npmrc`，新增 `admin-ui/bun.lock` 锁文件。
  - `package.json` 用 `trustedDependencies` 字段替代 pnpm 的 `onlyBuiltDependencies`，继续放行 `@swc/core`、`esbuild` 的安装脚本。
  - `Dockerfile` 前端构建阶段改用 `oven/bun:1-alpine`，命令统一为 `bun install --frozen-lockfile --ignore-scripts` + `bun run build`。
  - GitHub Actions（`build.yaml` / `release.yaml`）用 `oven-sh/setup-bun@v2` 替换 `setup-node` + `pnpm/action-setup`，CI 不再依赖 corepack；bun 版本锁定到 `1.3`，并通过 `actions/cache` 缓存 `~/.bun/install/cache`，多平台矩阵复用同一份依赖缓存。
  - `README.md` 与 `src/admin_ui/router.rs` 中的 `pnpm` 命令提示同步更新为 `bun`。
- **前端依赖整体升级到 2026 主版本**
  - Vite 5 → **8**（Rolldown 引擎，构建时间从约 3.7 s 降到约 0.4 s）。
  - React 18.3 → **19.2**，类型包 `@types/react` / `@types/react-dom` 同步升到 19.x。
  - TypeScript 5.6 → **6.0**；移除 TS 6 已弃用的 `tsconfig.json#baseUrl`，仅保留 `paths`（依赖 `moduleResolution: bundler` 解析）。
  - 前端 React 插件 `@vitejs/plugin-react-swc` 4 → **`@vitejs/plugin-react` 6**：Vite 8 + Rolldown 自带 oxc 转换，官方推荐切回原版 `plugin-react`，移除 swc 二进制依赖。
  - Tailwind 3.4 → **4.3**：新增 `@tailwindcss/postcss` PostCSS 插件，`postcss.config.js` 切换插件键名；`src/index.css` 用 `@import "tailwindcss"` 替代 `@tailwind base/components/utilities`，并通过 `@config "../tailwind.config.js"` 复用既有 hsl 主题变量与 `@apply` 配置。
  - Radix UI 套件、`@tanstack/react-query`、`axios`、`lucide-react`、`sonner`、`tailwind-merge` 一并升到当前 latest。
  - 新增 `src/vite-env.d.ts`（`/// <reference types="vite/client" />`），让 TS 6 严格模式下 `import './index.css'` 类型检查通过。
- **构建产物分包优化**
  - `vite.config.ts` 启用 `build.rolldownOptions.output.codeSplitting.groups`，按 `react` / `radix` / `query` / `icons` / `vendor` 拆分三方依赖 chunk，业务 chunk 体积全部回落到 500 kB 以下，便于浏览器缓存复用。
  - `App.tsx` 改用 `lazy` + `Suspense` 懒加载 `Dashboard`，未登录用户首屏不再下载管理面板代码。

### ✨ 新功能

- **首次启动自动初始化配置文件**
  - 启动时若 `config.json` 不存在，会自动写入一份最小默认配置：监听 `0.0.0.0:8990`、随机生成 `apiKey`（`sk-kiro-rs-...`）和 `adminApiKey`（`sk-admin-...`），并打印到日志。
  - `credentials.json` 不存在时自动写入 `[]`，后续可直接在 Admin UI 添加凭据。
  - Docker 首次部署不再需要手工准备 `data/config.json` / `data/credentials.json`，挂上 `data/` 目录直接 `docker compose up -d` 即可。
- **镜像在线更新**
  - 全新 Admin UI「镜像在线更新」面板：支持一键更新、回退、查看版本信息。
  - compose 文件路径与 service 名运行时从当前容器的 docker compose 标签自动发现，前端无需配置。
  - 更新前自动给当前镜像打 `kiro-rs:rollback` 本地 tag，断网也能一键回退到上一版本。
  - 失败提示更友好：检测到 compose yml 不存在 / 是目录时给出可操作的中文提示。
- **检查更新**
  - 后台轮询 Docker Hub 仓库 tags，发现新语义化版本时在工具栏图标显示红点。
  - 弹窗内展示「当前版本 / 最新版本 / 构建类型 / 发布时间」，并提供"立即检查"按钮。
- **无人值守自动更新**
  - 新增 `updateAutoApply` / `updateAutoApplyTime` 两个配置：开启后每天到指定时间自动检查并应用新版本，单分钟去重 + 单版本去重。
  - Admin UI 提供开关 + 时间选择器，修改即时生效。
- **凭据列表**
  - 支持鼠标左键拖拽框选凭据，跨网格区域均可触发；按住 Ctrl/Meta 拖拽可附加到既有选区。
  - 新增「全选当前页 / 取消全选」按钮，与既有"已选 N"徽章并存。
  - 卡片左侧勾选框命中区放大到 28×28，更易点击。

### 🎨 界面调整

- 顶栏与登录页 logo 改为项目自定义 PNG（`kirors.png`），不再使用占位的渐变方块图标。
- 镜像在线更新弹窗精简：标题旁的 ℹ️ 图标 hover/点击展示前置条件 Tooltip，不再占用主体空间。
- Tooltip 触发逻辑修复：弹窗打开时不会再因为焦点自动落到 ℹ️ 上而立即弹出。

### 🛠️ 维护

- `Cargo.toml` 升级到 `0.3.0`；`admin-ui/package.json` 同步对齐到 `0.3.0`。
- GitHub Actions 工作流（`release.yaml` / `docker-build.yaml`）切换到 Docker Hub 推送，使用 `DOCKERHUB_USERNAME` + `DOCKERHUB_TOKEN` secrets 登录。
- Release Notes 自动从 `CHANGELOG.md` 抽取对应版本章节。

### 📦 升级指南

1. **Docker Hub 部署**（推荐）
   - 直接使用 `zyphrzero/kiro-rs:latest` 替换现有镜像引用。
   - 不再需要 `githubToken` 字段；默认 `docker-compose.yml` 已切换到 Docker Hub。
2. **保留 GHCR 部署**
   - 把 `updateImage` 改回 `ghcr.io/<owner>/kiro-rs:latest`；但此后该镜像不再随项目更新，请自行 fork 或镜像同步。
3. **配置文件清理**
   - 删除 `githubToken`、`updateComposeFile`、`updateService`（如果仍存在）。
   - 如需开启每日自动更新，添加 `"updateAutoApply": true` 与 `"updateAutoApplyTime": "03:00"`。
4. **首次发布**
   - 维护者需在仓库 Settings → Secrets 添加 `DOCKERHUB_USERNAME` + `DOCKERHUB_TOKEN`，否则 CI 推送会失败。

