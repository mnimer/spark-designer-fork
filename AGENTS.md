# AGENTS.md: Apache Spark / Managed Spark Pipeline Builder (VS Code Extension)

## Project Overview & Architecture

- **Primary Goal**: Build a visual drag-and-drop VS Code extension for designing Apache Spark 4.x (PySpark DataFrame, Catalyst SQL, and Structured Streaming) pipelines targeting Local PySpark / Spark Connect, Dataproc Clusters, and GCP Serverless Spark.
- **Task Plan**: `TASKS.md` is the single source of truth for phases and task status. Update only the `Status` cell in the Task Index table.
- **Agent Customizations Layout**: Store project-wide instructions in root `AGENTS.md` (not `GEMINI.md`). Store all workspace rules, skills, and workflows under `.agents/` (`.agents/rules/`, `.agents/skills/`, `.agents/workflows/`), never under `.gemini/`.
- **Directory Layout**:
  - `src/`: VS Code Extension Host (Node.js / CommonJS, `tsconfig.json`).
  - `src/shared/` & `src/spark/`: Pure cross-boundary TypeScript modules shared by Extension Host and Webview.
  - `webview/src/`: React 19 + React Flow + Tailwind CSS frontend (`webview/tsconfig.app.json`, Vite).
  - `resources/`: Codegen prompt templates, JSON schemas, runner scripts, and `codegen-skills/`.
  - `src/test/unit/` & `webview/src/**/*.test.{ts,tsx}`: Vitest unit test suites.
  - `src/test/integration/`: Multi-agent Spark evaluator and offline `pytest` harness.

## Rules Index (`.agents/rules/`)

Detailed rules live in `.agents/rules/`. If your tool does not load that folder automatically, read the matching rule file before you work in its scope.

| Rule File | Trigger | Scope |
| :--- | :--- | :--- |
| `git-workflow.md` | `always_on` | Commit policy, commit message format, no remote fetch/push, squash-merged `develop` base. |
| `toolchain-and-verification.md` | `always_on` | Airlock `node_modules` bootstrap, `git grep` search, root vs. `webview/` toolchain, verification gate. |
| `documentation-style.md` | `always_on` | Single-header author credit, ASD-STE100 style, living docs sync. |
| `model-selection.md` | `always_on` | Valid Gemini model, forbidden models, global Vertex AI endpoint. |
| `shared-module-purity.md` | `model_decision` | `src/shared/` and `src/spark/` import bans, webview TypeScript syntax limits, JSDoc safety. |
| `webview-ui-design-system.md` | `model_decision` | 8pt grid, theme tokens, property form contracts, RTL queries, property registry parity. |
| `asset-discovery-ipc.md` | `model_decision` | Four-registry lockstep for discovery commands, safe CLI execution, region validation. |
| `custom-editor-activation.md` | `model_decision` | `SPARK_EDITOR_VIEW_TYPE` sync and restored `.spark` tab upgrade. |
| `deployment-state-and-runners.md` | `model_decision` | Functional `jobConfig` updaters and runner script allowlist. |
| `codegen-skill-library.md` | `model_decision` | Codegen skill invariants, transform sync, byte-identical runner templates, no undocumented fallbacks. |
