# OpenLogi

Native, local-first Logitech Options+ alternative built with Rust/GPUI for
macOS, Linux, and Windows. Supports HID++ mice/keyboards, UVC webcams, and Litra
lights, with plain-TOML configuration, no account, and no telemetry.

## Essential rules

- Converse in Traditional Chinese; write GitHub artifacts in English. When offering choices, recommend one with a reason.
- Verify reports against current code; fix root causes at the owning module/lifecycle.
- The agent owns runtime HID I/O and input hooks; desktop/overlay are IPC clients. CLI hardware diagnostics may access devices directly.
- Shared presentation belongs in `openlogi-ui`; overlay never depends on desktop. IPC wire changes are versioned and append-only.
- Shared decisions have one owner. Consolidate on the second copy and add an ast-grep guard.
- Iterate with focused tests/checks. For stable Rust changes, run formatting, relevant tests, and changed-crate Clippy; check consumers of changed APIs. Docs-only edits need no Rust checks.
- Use focused Conventional Commits; no AI attribution. Commit/push requires elevated context. Before an authorized push, pass the [local gate](.agents/rules/ci.md#local-gate-hard-stop-before-push--scale-it-to-the-affected-graph) without bypassing hooks.
- release-plz owns versions, root changelog, and release tags; never manually create tags or rerun published releases.
- Keep guidance canonical and linked; preserve imported skills/licenses/locks. Propose guidance changes separately during ordinary code work.

## Read only when relevant

- Product scope: [README.md](README.md). Setup, build/run, mocks, packaging: [DEVELOPMENT.md](docs/DEVELOPMENT.md). Config/CLI: [CONFIGURATION.md](docs/CONFIGURATION.md), [USAGE.md](docs/USAGE.md).
- Before rebase, issue or PR work: [GitHub workflow](docs/DEVELOPMENT.md#github-workflow). Draft external-repo posts/public replies on the maintainer's behalf for approval.
- Before editing, read applicable crate-local `AGENTS.md` and the rules below; clients must load them explicitly.

| Affected area | Rules |
|---|---|
| Rust / Cargo | [rust](.agents/rules/rust.md) |
| Desktop / UI / overlay | [gui](.agents/rules/gui.md) |
| Localization | [i18n](.agents/rules/i18n.md) |
| IPC / wire types in core, hid, agent-core, agent | [IPC contract](crates/openlogi-ipc/AGENTS.md) |
| Platform code / macOS FFI | [cross-platform](.agents/rules/cross-platform.md), [objc-ffi](.agents/rules/objc-ffi.md) |
| Device / HID | [device contract](crates/openlogi-device/AGENTS.md) |
| xtask / packaging / GitHub scripts | [xtask contract](xtask/AGENTS.md), [xtask README](xtask/README.md) |
| CI / ast-grep | [ci](.agents/rules/ci.md) |

Load task skills from `.agents/skills/<name>/SKILL.md` (including required references):

- GPUI code/state/testing: `gpui-kit`; UI design/copy: `gpui-kit-design-guides`; native UI verification/mocks: `testing-openlogi-ui`.
- Device discovery/open/pairing/reconnect or CLI/GUI disagreement: `diagnosing-openlogi-devices`.
- Regression proof/check selection/commit or push verification: `verifying-openlogi-changes`; device fixtures: `contributing-device-fixtures`.
- macOS permission reports or permission/helper-launch/bundle-signing edits: [.claude/skills/openlogi-macos-permissions/SKILL.md](.claude/skills/openlogi-macos-permissions/SKILL.md).
