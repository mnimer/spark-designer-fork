---
trigger: model_decision
description: "Load before you add or edit an asset discovery command, gcloud/bq/aws CLI lookup, or IPC message: src/ipc/handlers/discoveryHandlers.ts, src/cli/, src/shared/protocol.ts, src/ipc/messageRouter.ts, or webview/src/hooks/useAssetLoader.ts."
---

# Asset Discovery IPC & CLI Safety (`src/ipc/handlers/discoveryHandlers.ts`)

- When you add an asset discovery command, update all four registries in lockstep:
  - `src/shared/protocol.ts` (`WEBVIEW_TO_HOST_COMMANDS` and `AssetResponseCommand`)
  - `src/ipc/messageRouter.ts` (`MESSAGE_HANDLERS` and `ASSET_REQUEST_COMMANDS`)
  - `webview/src/hooks/useAssetLoader.ts` (`AssetType` and `ASSET_CONFIGS`)
  - `src/extension.ts` exports
- Run all CLI lookups (`gcloud`, `bq`, `aws`) through `listAssets` using `execFile` argument arrays. Never interpolate user values into shell strings.
- Validate regions (`isValidGcpRegion`) and resource identifiers with strict regexes before spawning CLI commands.
- `gcloud beta dataproc session-templates list` takes `--location=<REGION>`, whereas `clusters` and `batches` take `--region=<REGION>`.
