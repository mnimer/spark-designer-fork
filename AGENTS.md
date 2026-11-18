# AGENTS.md: Apache Spark / Managed Spark Pipeline Builder (VS Code Extension)

## Project Overview & Architecture

- **Primary Goal**: Build a visual drag-and-drop VS Code extension for designing Apache Spark 4.x (PySpark DataFrame, Catalyst SQL, and Structured Streaming) pipelines targeting Local PySpark / Spark Connect, Dataproc Clusters, and GCP Serverless Spark.
- **Task Plan**: `TASKS.md` is the single source of truth for phases and task status  Update only the `Status` cell in the Task Index table.
- **Agent Customizations Layout**: Store project-wide instructions in root `AGENTS.md` (not `GEMINI.md`). Store all workspace rules, skills, and workflows under `.agents/` (`.agents/rules/`, `.agents/skills/`, `.agents/workflows/`), never under `.gemini/`.
- **Directory Layout**:
  - `src/`: VS Code Extension Host (Node.js / CommonJS, `tsconfig.json`).
  - `src/shared/` & `src/spark/`: Pure cross-boundary TypeScript modules shared by Extension Host and Webview.
  - `webview/src/`: React 19 + React Flow + Tailwind CSS frontend (`webview/tsconfig.app.json`, Vite).
  - `resources/`: Codegen prompt templates, JSON schemas, runner scripts, and `codegen-skills/`.
  - `src/test/unit/` & `webview/src/**/*.test.{ts,tsx}`: Vitest unit test suites.
  - `src/test/integration/`: Multi-agent Spark evaluator and offline `pytest` harness.

## Model Selection Rules

- **Valid Model**: Use `gemini-3.8-flash` on Vertex AI with `location: 'global'`.
- **Forbidden Models**: Never revert to or map model names to `gemini-2.5*` or Gemini 1.x/2.x, and never use regional Vertex AI endpoints (`us-central1` returns 404 for `gemini-3.7-flash`).

## Hard Codebase & Worktree Invariants

1. **Offline Airlock Worktree Bootstrap**: Fresh git worktrees lack `node_modules`, and `npm ci` fails in Airlock (`vite@8.3.x` is unavailable). Before running `tsc`, `vitest`, `vite`, or `eslint` in any worktree, link packages via hardlink copy:
   ```bash
   test -d node_modules || cp -al /usr/local/google/home/mikenimer/Development/apache-spark-designer/node_modules ./node_modules
   test -d webview/node_modules || cp -al /usr/local/google/home/mikenimer/Development/apache-spark-designer/webview/node_modules ./webview/node_modules
   ```
   Never stage `node_modules/`, `out/`, or `webview/dist/`.
2. **Cross-Boundary Purity (`src/shared/*` & `src/spark/*`)**:
   - Zero imports of `vscode`, `fs`, `path`, `child_process`, `os`, `react`, or `node:*` (enforced by `src/test/unit/shared-purity.test.ts`).
   - `webview/tsconfig.app.json` enforces `"verbatimModuleSyntax": true` and `"erasableSyntaxOnly": true`: always use `import type { ... }` for type-only imports and never declare TypeScript `enum`s or parameter properties. Run `npm --prefix webview run typecheck` so `webview/node_modules/.bin/tsc` validates webview TypeScript.
   - Never write literal `*/` inside a JSDoc comment (closes the comment block and breaks `tsc`).
3. **Webview Property Registry Parity (`webview/src/catalog/propertyFormRegistry.test.ts`)**:
   - `Object.keys(PROPERTY_FORM_REGISTRY).length` must equal `Object.keys(NODE_DOCUMENTATION_REGISTRY).length` (`webview/src/catalog/documentation/`).
   - Every registry entry must use `lazyForm(...)`, and alias keys must reuse the exact same `lazyForm` constant so distinct wrappers resolve to distinct components.
4. **Asset Discovery IPC & CLI Safety (`src/ipc/handlers/discoveryHandlers.ts`)**:
   - When you add an asset discovery command, update all four registries in lockstep: `src/shared/protocol.ts` (`WEBVIEW_TO_HOST_COMMANDS` and `AssetResponseCommand`), `src/ipc/messageRouter.ts` (`MESSAGE_HANDLERS` and `ASSET_REQUEST_COMMANDS`), `webview/src/hooks/useAssetLoader.ts` (`AssetType` and `ASSET_CONFIGS`), and `src/extension.ts` exports.
   - Run all CLI lookups (`gcloud`, `bq`, `aws`) through `listAssets` using `execFile` argument arrays. Never interpolate user values into shell strings.
   - Validate regions (`isValidGcpRegion`) and resource identifiers with strict regexes before spawning CLI commands. Note that `gcloud beta dataproc session-templates list` takes `--location=<REGION>`, whereas `clusters` and `batches` take `--region=<REGION>`.
5. **Custom Editor Association & Restored Tab Upgrade (`src/editor/restoredTabUpgrade.ts`)**:
   - Keep `SPARK_EDITOR_VIEW_TYPE` (`spark-pipeline-builder.sparkEditor`) identical across `src/core/constants.ts` and `package.json` (`activationEvents`, `contributes.customEditors`, and `configurationDefaults`).
   - Preserve `upgradeRestoredSparkTextTabs()` at the end of `activate()` in `src/extension.ts`. If another Extension Development Host opens a shared workspace while this extension is inactive, VS Code persists open `.spark` files as plain-text tabs (`override: "default"`). The activation hook upgrades clean plain-text `.spark` tabs back to the visual editor without touching dirty or diff tabs.
6. **UI & Design System (`DESIGN.md`)**:
   - Strictly follow the 8pt spacing grid (`p-2`, `p-3`, `p-4`, `gap-2`, `gap-4`) and `var(--vscode-*)` theme tokens.
   - Nested React components accept props down and emit events up via `onChange`. Include `<InfoTooltip text="..." />` on property fields.
   - Because `<InfoTooltip>` renders `<span role="tooltip" aria-label="...">` inside `<label>`, pass `{ selector: 'input' }` / `{ selector: 'select' }` / `{ selector: 'textarea' }` to `getByLabelText(...)` (or use `getByRole`) in React Testing Library tests so label queries never collide with tooltip text.
   - Reuse `StorageBackendFields` (`webview/src/components/common/StorageBackendFields.tsx`) across all File Format sources and sinks.
   - `PropertyForm.tsx` automatically renders `<NodeDocsCard />`, `<StepExecutionSettingsField />` (for `STEP` nodes), and `<StreamingSinkSettingsField />` (for streaming `SINK` nodes) above the lazy form. Never render those three wrapper cards inside individual Source, Sink, or Step panels, and always render `<SystemInstructionsField />` at the bottom of each panel.
   - In generated code, do not add undocumented fallbacks. Verify official documentation or fail fast.
7. **Standalone Git Search & Dual-Toolchain Split (`root` vs. `webview/`)**:
   - This repository lives outside CitC and Cog, so `cs --corpus=local` fails immediately. Use `git grep` and `git ls-files` instead.
   - Never run root `npx tsc -p webview/tsconfig.app.json` (root TS 5.9 produces false `unknown`/`never` errors on TS 6.0 webview files). Run `npm run typecheck:webview` instead.
   - Root `npm run lint` (`eslint src`) skips `webview/**`. Run `npm --prefix webview run lint`, which enforces `react-refresh/only-export-components` (keep non-component helpers in `.ts` files) and JSDoc on `TSInterfaceDeclaration` and exported arrow components.
   - Root `npm test` runs only `src/test/unit/**/*.test.ts`. Run `npm run test:webview` (or `(cd webview && npx vitest run <paths>)`) for webview tests, or `npm run test:all` for both suites.
8. **Deployment State Updaters & Runner Script Allowlist**:
   - Child components must update `jobConfig` with functional updaters (`setJobConfig((prev) => ({ ...prev, ... }))`), never `{ ...jobConfig, ... }` prop snapshots. Add any new `jobConfig` fields to `handleSetJobConfig` in `webview/src/App.tsx` so changes trigger `needsRebuild: true`.
   - Register every executable `.sh` runner script basename in `ALLOWED_EXECUTABLE_SCRIPTS` (`src/security/scriptGuard.ts`) or `handleRunPipeline` refuses execution.
9. **Codegen Skill Library & Runner Template Invariants (`src/test/unit/codegen/codegen-skills-invariants.test.ts`)**:
   - Extend existing `resources/codegen-skills/io-<connector>/SKILL.md` files for matching Sources and Sinks (one `io-*` skill per connector and zero duplicate `sparkTransform` owners). Restrict `{{...}}` placeholders to `MODEL_FACING_PLACEHOLDERS` and `{{spark_version}}`.
   - Keep `src/codegen/codegenConfig.ts` (`STREAMING_SOURCE_TRANSFORMS`, `STREAMING_STEP_TRANSFORMS`, `PYTHON_DATASOURCE_TRANSFORMS`), `src/shared/sparkNodeCapabilities.ts`, and `src/spark/versionMatrix.ts` synchronized whenever catalog transforms change.
   - Keep runner templates byte-identical in `resources/codegen-skills/runner-*/templates/python/` and `resources/templates/pyspark/`, and preserve the test-pinned `## Credential Handling Rules` strings in `core-pyspark/SKILL.md`.
   - Load `.agents/skills/spark-connector-authoring/SKILL.md` before authoring or editing codegen skills.

## Documentation & Author Credit Rules

- **Single-Header Author Credit**: Place `Author: Mike Nimer <mikenimer@google.com>` **once per file**, inside the top module/class-level docstring only. Never add author credits to method or function docstrings.
- **ASD-STE100 Documentation Style**: Write docstrings, inline comments (`// NOTE:`, `// TODO:`, `// HACK:`, `// FIXME:`), and markdown docs in active voice with direct sentences (`<= 20` words for procedural steps, `<= 25` words for descriptive text), zero semicolons in prose, and condition-first logic ("If X, do Y.").
- **Living Docs Sync**: Keep `ARCHITECTURE.md` and `DESIGN.md` synchronized when changing subsystems, graph rules, generated repo structure, or canvas badges/handles.

## Testing & Git Guidelines

- **Unit Test & Lint Coverage**: Every new or modified module requires Vitest unit tests covering normal, boundary, and failure conditions, plus `npm run compile`, `npm run typecheck:webview`, `npm run lint`, `npm --prefix webview run lint`, `npm run build:webview`, and `npm run test:all` exiting `0`.
- **Squash-Merged `develop` Base Rule**: Phase PRs merge into `origin/develop` via GitHub Squash and Merge. Never stack or fast-forward a new phase worktree onto un-squashed commits from a sibling feature branch. If local `develop` lacks a merged prerequisite phase, ask the user to pull `develop` and rebase the worktree first. If a branch was stacked onto an un-squashed branch `<old-tip>`, recover after fetching via `git rebase --onto origin/develop <old-tip> <branch>`.
- **Git Commits**: Never commit unless explicitly requested by the user. When asked to commit, follow `.agents/workflows/gitcommit.md`: format `<type>(<scope>): <subject>`, include a detailed body, and omit all file paths, file extensions, and markdown links from both subject and body. Wrap commit messages shown in chat inside a single ` ```text ` block.
- **Remote Git Operations**: HTTPS `origin` has no non-interactive credential helper in agent shells. Never run `git fetch` or `git push` against remote. Leave pushing and PR creation to the user.
