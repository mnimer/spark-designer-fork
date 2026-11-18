---
trigger: model_decision
description: "Load before you edit package.json contributions (activationEvents, customEditors, configurationDefaults), src/core/constants.ts, src/extension.ts activate(), src/editor/restoredTabUpgrade.ts, or any custom editor registration for .spark files."
---

# Custom Editor Association & Restored Tab Upgrade (`src/editor/restoredTabUpgrade.ts`)

- Keep `SPARK_EDITOR_VIEW_TYPE` (`spark-pipeline-builder.sparkEditor`) identical across `src/core/constants.ts` and `package.json` (`activationEvents`, `contributes.customEditors`, and `configurationDefaults`).
- Preserve `upgradeRestoredSparkTextTabs()` at the end of `activate()` in `src/extension.ts`.
- If another Extension Development Host opens a shared workspace while this extension is inactive, VS Code persists open `.spark` files as plain-text tabs (`override: "default"`). The activation hook upgrades clean plain-text `.spark` tabs back to the visual editor without touching dirty or diff tabs.
