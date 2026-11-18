---
trigger: model_decision
description: "Load before you edit jobConfig state, Deployment tab components (webview/src/components/BottomPanel/), webview/src/App.tsx handleSetJobConfig, runner .sh scripts, src/execution/PipelineRunner.ts, or src/security/scriptGuard.ts."
---

# Deployment State Updaters & Runner Script Allowlist

- Child components must update `jobConfig` with functional updaters (`setJobConfig((prev) => ({ ...prev, ... }))`), never `{ ...jobConfig, ... }` prop snapshots.
- Add any new `jobConfig` fields to `handleSetJobConfig` in `webview/src/App.tsx` so changes trigger `needsRebuild: true`.
- Register every executable `.sh` runner script basename in `ALLOWED_EXECUTABLE_SCRIPTS` (`src/security/scriptGuard.ts`) or `handleRunPipeline` refuses execution.
