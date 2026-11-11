# Issue #119 handoff (not for PR)

This branch only carries notes for continuing on another machine. Never open a PR from it.
The PR branches are:

| Branch | Content | Base |
| --- | --- | --- |
| `perf/issue-119-phase1-impl` | Phase 1: per-request timing summary (`endpoint`, `output_tokens_source`, `timing` in `traces.db`, on `GET /api/admin/traces`) | upstream `5c8c348` (v0.9.8) |
| `perf/issue-119-tcp-nodelay` | `TCP_NODELAY` on accepted client sockets | `perf/issue-119-phase1-impl` |

Field definitions and bench numbers are in `docs/gateway-performance.md` on each branch.
`design-final.md` here is the design contract.

## State on 2026-11-11

- Linux (Docker) suite: phase1 1227 passed, nodelay 1228 passed, 0 failed, 3 ignored.
  Clippy warnings equal upstream (117 bin, 220 test).
- Independent review: no blockers; its findings are fixed in `98e085f`, `db04a97`, `078a556`.
- Fixture bench (loopback Linux): first text P50 about 2.7 to 4 ms on all four APIs.
  Longest chunk gap P50 43 ms without `TCP_NODELAY`, 7 ms with it.
- Not verified yet: Windows/macOS build, real Kiro auth/refresh/proxy, real network timing.

## Live test on the machine with Kiro

1. Git identity for this repo: `git config user.name yeeyon`,
   `git config user.email yeeyon90@gmail.com`. Remotes: `origin` = `yeeyon/kiro.rs`, `upstream` = `ZyphrZero/kiro.rs`.
2. Build each branch: `cargo build --release` (admin UI needs `admin-ui/dist`; run `pnpm install && pnpm run build`
   in `admin-ui/`, or create an empty `admin-ui/dist` for a test-only build), or `docker build -t kiro-rs:<tag> .`.
3. Optional: `cargo test --locked` on each branch (Windows/macOS coverage was not run yet).
4. Run your normal config with `"traceEnabled": true`. If credentials use refresh tokens, do not run the same
   credential in two kiro.rs instances at once; a refresh rotates the token in `credentials.json`.
5. Probe (needs `curl` and `jq`, Git Bash on Windows):
   ```sh
   KIRO_URL=http://127.0.0.1:8990 KIRO_KEY=<apiKey> ADMIN_KEY=<adminApiKey> MODEL=<model> \
     sh handoff/live-probe.sh 5 | tee kiro-live-<branch>.txt
   ```
   Run once on `perf/issue-119-phase1-impl` and once on `perf/issue-119-tcp-nodelay`.
6. Pass criteria: all requests 200 on all four APIs, stream and non-stream; each trace has `endpoint` and
   `timing`; stream rows have `firstEmitTextUs` after `upstreamHeadersUs`; `outputTokensSource` is `exact`
   when Kiro returned usage; admin UI trace page still loads. Also use Claude Code through `/cc/v1/messages`
   for a few turns on each branch and note how streaming feels.

## After the live test

Add a baseline-versus-change table from the two probe outputs to `docs/gateway-performance.md`, then open
PR 1 (`perf/issue-119-phase1-impl` into `ZyphrZero/kiro.rs:master`, referencing #119) and PR 2
(`perf/issue-119-tcp-nodelay`, after PR 1 or stacked) from the `yeeyon` account.
