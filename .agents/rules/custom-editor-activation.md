---
trigger: model_decision
description: "Load before you edit package.json contributions (activationEvents, customEditors, configurationDefaults), src/core/constants.ts, src/extension.ts activate(), src/editor/restoredTabUpgrade.ts, sample save or workspace folder handlers, webview loadPipeline filename handling, or any .spark file registration."
---

# Custom Editor Association, `.spark` Extension & Zero-`.beam` Invariant

- Keep `SPARK_EDITOR_VIEW_TYPE` (`spark-pipeline-builder.sparkEditor`) identical across `src/core/constants.ts` and `package.json` (`activationEvents`, `contributes.customEditors`, and `configurationDefaults`).
- Preserve `upgradeRestoredSparkTextTabs()` at the end of `activate()` in `src/extension.ts`.
- If another Extension Development Host opens a shared workspace while this extension is inactive, VS Code persists open `.spark` files as plain-text tabs (`override: "default"`). The activation hook upgrades clean plain-text `.spark` tabs back to the visual editor without touching dirty or diff tabs.
- Normalize pipeline filenames, sample save dialogs (`handleOpenSample`, `nextAvailableSparkFilename`), sibling output folders (`handleReadFolderFiles`, `handleReadFileContent`), and webview `loadPipeline` / `findMainPipelineFile` extension stripping (`useVsCodeBridge.ts`) through `PIPELINE_FILE_EXT` (`.spark`) or `/\.spark$/i`. Never append or strip `.beam`, which produces doubled `*.beam.spark` suffixes.
- Never introduce `.beam` or `beamgen` literals anywhere in `src/`, `webview/`, `resources/`, `vitest*.mts`, `package.json`, or documentation. Verify zero matches with `git grep -in "\.beam" -- src/ webview/ resources/ vitest.config.mts vitest.integration.config.mts package.json`.
