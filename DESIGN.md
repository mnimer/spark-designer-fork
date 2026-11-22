---
name: "Apache Spark Pipeline Designer"
author: "Mike Nimer <mikenimer@google.com>"
description: "Unified UI/UX design system and token specification for the visual Apache Spark and Managed Spark pipeline builder VS Code extension."
version: "2.2.0"
colors:
  canvas-bg: "#0d1117"
  workbench-bg: "#161b22"
  card-bg: "#161b22"
  subcard-bg: "#0d1117"
  input-bg: "#0d1117"
  border: "#30363d"
  border-light: "#21262d"
  text-primary: "#f0f6fc"
  text-secondary: "#c9d1d9"
  text-muted: "#8b949e"
  highlight: "#58a6ff"
  accent: "#238636"
  button-secondary: "#21262d"
  hover: "#21262d"
  source-primary: "#10b981"
  source-accent: "rgba(16, 185, 129, 0.1)"
  source-border: "rgba(16, 185, 129, 0.3)"
  step-primary: "#3b82f6"
  step-accent: "rgba(59, 130, 246, 0.1)"
  step-border: "rgba(59, 130, 246, 0.3)"
  sink-primary: "#ef4444"
  sink-accent: "rgba(239, 68, 68, 0.1)"
  sink-border: "rgba(239, 68, 68, 0.3)"
  sample-primary: "#a855f7"
  sample-accent: "rgba(168, 85, 247, 0.1)"
  sample-border: "rgba(168, 85, 247, 0.3)"
typography:
  display-lg:
    fontFamily: "Inter, -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "1.125rem"
    fontWeight: "700"
    lineHeight: "1.5"
  body-md:
    fontFamily: "Inter, -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "0.875rem"
    fontWeight: "400"
    lineHeight: "1.25rem"
  label-xs:
    fontFamily: "Inter, -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "0.75rem"
    fontWeight: "700"
    lineHeight: "1rem"
    letterSpacing: "0.05em"
  badge-micro:
    fontFamily: "Inter, -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "0.625rem"
    fontWeight: "700"
    lineHeight: "0.875rem"
    letterSpacing: "0.05em"
  code-sm:
    fontFamily: "JetBrains Mono, Fira Code, monospace"
    fontSize: "0.75rem"
    fontWeight: "400"
    lineHeight: "1.25rem"
rounded:
  sm: "4px"
  md: "6px"
  lg: "8px"
  xl: "12px"
  full: "9999px"
spacing:
  xs: "4px"
  sm: "8px"
  md: "12px"
  lg: "16px"
  xl: "24px"
  "2xl": "32px"
  drawer: "320px"
components:
  node-card:
    backgroundColor: "{colors.card-bg}"
    borderColor: "{colors.border}"
    borderRadius: "{rounded.md}"
    padding: "{spacing.md}"
    minWidth: "180px"
  palette-drawer:
    width: "{spacing.drawer}"
    backgroundColor: "{colors.workbench-bg}"
    borderColor: "{colors.border}"
  property-inspector:
    width: "{spacing.drawer}"
    backgroundColor: "{colors.workbench-bg}"
    borderColor: "{colors.border}"
  bottom-panel:
    backgroundColor: "{colors.workbench-bg}"
    borderColor: "{colors.border}"
    headerHeight: "{spacing.2xl}"
---

## Overview

The Apache Spark Pipeline Designer provides a high-contrast visual IDE for Apache Spark 4.x pipelines. Users author, configure, and inspect Imperative PySpark 4.x and Spark Declarative Pipelines (`SDP`, Apache Spark 4.2+) directed acyclic graphs (`DAGs`).

### Design Principles
1. **8pt Grid Discipline**: Use `8px` multiples for layout spacing and `4px` half-steps for compact badges and inputs.
2. **Semantic VS Code Theming**: Derive surfaces and text from `--vscode-*` variables across Dark+, Light+, and High Contrast themes.
3. **Tri-Color DAG Taxonomy**: Color-code pipeline stages by category for instant visual scanning:
   - **Sources**: Emerald Green (`#10b981`)
   - **Transforms / Steps**: Azure Blue (`#3b82f6`)
   - **Sinks**: Rose Red (`#ef4444`)
   - **Samples**: Purple / Violet (`#a855f7`)
4. **Paradigm-Aware Visual Feedback**: Update toolbar pills, node micro-badges, palette items, and inspector panels together on paradigm changes.

---

## Colors

### Surfaces & Neutrals
- **Canvas Background (`--vscode-canvas-bg`)**: Deep slate foundation (`#0d1117`) for the React Flow canvas.
- **Workbench & Sidebars (`--vscode-workbench-bg`)**: Contrast surface (`#161b22`) for sidebars, drawers, and floating bars.
- **Card Surfaces (`--vscode-card-bg`)**: Elevated surface (`#161b22`) for node cards, palette items, and settings containers.
- **Subcards & Code Containers (`--vscode-subcard-bg`)**: Recessed surface (`#0d1117`) for inputs and syntax previews.
- **Borders (`--vscode-border`)**: Subtle structural dividers (`#30363d`) and hairline accents (`#21262d`).

### Pipeline Category Accents
- **Sources (`#10b981`)**: Data ingestion connectors (`KafkaSource`, `BigQuerySource`, `IcebergSource`, `CsvSource`, `JsonSource`, `ParquetSource`, `SpannerSource`).
  - Background: `bg-emerald-500/10`, Border: `border-emerald-500/30`, Text: `text-emerald-400`
- **Transforms & Steps (`#3b82f6`)**: Catalyst and streaming steps (`Filter`, `WithColumns`, `Watermark`, `GroupByAgg`, `Join`, `MergeCdcScd`, `ConditionalSplit`, `DataQualityAssert`).
  - Background: `bg-blue-500/10`, Border: `border-blue-500/30`, Text: `text-blue-400`
- **Sinks (`#ef4444`)**: Terminal output destinations (`IcebergSink`, `DeltaSink`, `BigQuerySink`, `KafkaSink`, `ParquetSink`, `ConsoleSink`).
  - Background: `bg-rose-500/10`, Border: `border-rose-500/30`, Text: `text-rose-400`
- **Samples (`#a855f7`)**: Starter `.spark` templates carrying `PySpark` or `SDP (4.2+)` paradigm badges.
  - Background: `bg-purple-500/10`, Border: `border-purple-500/30`, Text: `text-purple-400`

---

## Typography & 8pt Layout Scale

| Style Level | Font Family | Size | Weight | Line Height | Usage |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`display-lg`** | Inter | `1.125rem` (`18px`) | Bold (`700`) | `1.5` | Top-level drawer headers and modal titles |
| **`body-md`** | Inter | `0.875rem` (`14px`) | Regular (`400`) | `1.25rem` | Form inputs, dropdown values, card descriptions |
| **`label-xs`** | Inter | `0.75rem` (`12px`) | Bold (`700`) | `1rem` | Uppercase field labels and section dividers |
| **`badge-micro`** | Inter | `0.625rem` (`10px`) | Bold (`700`) | `0.875rem` | Paradigm pills, dataset micro-badges, count badges |
| **`code-sm`** | JetBrains Mono | `0.75rem` (`12px`) | Regular (`400`) | `1.25rem` | Generated code preview, dataset names, `$VARIABLE` chips |

| Token | Dimension | Tailwind Class | Application |
| :--- | :--- | :--- | :--- |
| `xs` | `4px` | `p-1`, `gap-1` | Micro-spacing, handle border thickness, badge padding |
| `sm` | `8px` | `p-2`, `gap-2` | Input padding, icon container padding, card inner gaps |
| `md` | `12px` | `p-3`, `gap-3` | Node internal padding, property group spacing, canvas grid gap |
| `lg` | `16px` | `p-4`, `gap-4` | Sidebar drawer padding, panel header padding, form vertical gaps |
| `xl` | `24px` | `p-6`, `gap-6` | Major column gutters in Deployment tab |
| `2xl` | `32px` | `h-8`, `p-8` | Action button heights and tab bar headers |
| `drawer` | `320px` | `w-80` | Left node palette drawer and right property inspector width |

---

## Components

### 1. Canvas Toolbar (`CanvasToolbar`) & Paradigm Compatibility Modal
- **Floating Control Bar (`data-testid="canvas-toolbar"`)**: Anchors Undo, Redo, Auto Layout, Zoom, and MiniMap controls at `top-3 right-3`.
- **Pipeline Mode Pill (`data-testid="toolbar-pipeline-mode"`)**: Displays `BATCH` or `STREAMING` from deterministic streaming propagation (`computeStreamingNodeSet`).
- **Paradigm Indicator & Switcher (`data-testid="toolbar-paradigm-indicator"`)**: Displays `IMPERATIVE` (blue pill) or `SDP (4.2+)` (amber pill).
  - When the user switches from `SDP (4.2+)` to `IMPERATIVE`, the editor updates immediately and records an undo snapshot.
  - When the user switches from `IMPERATIVE` to `SDP (4.2+)`, the editor runs `evaluateSdpCompatibility`.
  - If blocking constructs exist, the editor opens an accessible modal (`role="dialog"`, `data-testid="sdp-compatibility-modal"`). The modal lists each incompatible node label, transform, and SDP ceiling reason.

### 2. DAG Node (`PipelineNode`), Micro-Badges & Connection Handles

#### Card Shell & Inline Rename
- **Base Container (`data-testid="pipeline-node-<id>"`)**: Renders `relative bg-[var(--vscode-node-bg,#1E202A)] border rounded-md shadow-xl p-3 flex items-center gap-3 min-w-[180px]`.
- **Category Header & Icon Box**: Displays a `p-2` bordered icon box and uppercase `SOURCE` (`text-emerald-400`), `STEP` (`text-blue-400`), or `SINK` (`text-red-400`) label.
- **Inline Label Editing**: Double-clicking the card or clicking the hover `Pencil` button (`aria-label="Rename node"`) opens the inline label input (`aria-label="Edit node label"`).

#### Execution Mode, Planner & SDP Micro-Badges
All micro-badges use compact `10px` (`text-[10px]`) bold pill styling (`px-1 py-0.5 rounded font-bold leading-none border`).

| Badge Label | `data-testid` | Trigger Condition (`PipelineNode.tsx`) | Tailwind Background, Border & Text Tokens |
| :--- | :--- | :--- | :--- |
| **`BATCH`** | `node-mode-badge` | When `effectiveExecutionMode === 'batch'` on `SOURCE` or `SINK` cards | `bg-slate-500/15 border-[var(--vscode-border,#30363d)] text-[var(--vscode-text-secondary,#c9d1d9)]` |
| **`STREAM`** | `node-mode-badge` | When `effectiveExecutionMode === 'streaming'` on `SOURCE` or `SINK` cards | `bg-cyan-500/15 border-cyan-500/40 text-cyan-300` |
| **`BROADCAST`** | `node-broadcast-badge` | When `properties.broadcastRight === true` on `Join` steps | `bg-indigo-500/15 border-indigo-500/40 text-indigo-300` |
| **`N-IN`** | `node-multi-input-badge` | When `inputArity === 'n'` on `STEP` nodes (`UnionByName`, `SetOperation`, `SparkSqlQuery`) | `bg-blue-500/15 border-blue-500/40 text-blue-300` |
| **`CACHE`** | `node-cache-badge` | When `paradigm !== 'sdp'` and `data.isCached` is `true` | `bg-amber-500/15 border-amber-500/40 text-amber-300` |
| **`foreachBatch`** | `node-foreachbatch-badge` | When `paradigm !== 'sdp'` and `data.isForeachBatchSink` is `true` on `SINK` cards | `bg-violet-500/15 border-violet-500/40 text-violet-300` |
| **`VIEW`** | `node-sdp-badge` | When `paradigm === 'sdp'` on `@dp.temporary_view` steps | `bg-slate-500/20 border-slate-400/40 text-slate-200` |
| **`MV`** | `node-sdp-badge` | When `paradigm === 'sdp'` on `@dp.materialized_view` batch tables | `bg-purple-500/15 border-purple-500/40 text-purple-300` |
| **`TABLE`** | `node-sdp-badge` | When `paradigm === 'sdp'` on `@dp.table` or `dp.create_streaming_table` + `@dp.append_flow` | `bg-emerald-500/15 border-emerald-500/40 text-emerald-300` |
| **`CDC`** | `node-sdp-badge` | When `paradigm === 'sdp'` on `dp.create_auto_cdc_flow` SCD Type 1 flows | `bg-amber-500/15 border-amber-500/40 text-amber-300` |
| **`SINK`** | `node-sdp-badge` | When `paradigm === 'sdp'` on `dp.create_sink` external streaming sinks | `bg-rose-500/15 border-rose-500/40 text-rose-300` |

- When `paradigm === 'sdp'`, `PipelineNode` suppresses `CACHE` and `foreachBatch` pills and renders deterministic `node-sdp-badge` tokens from `sdpPlanner.ts`.

#### Validation Status Icons & Card Border Precedence
- **Validation Status Icons**:
  - **Cycle or Validation Error (`data-testid="cycle-error-badge"`)**: When `hasCycle` or `hasValidationError` is `true`, renders a rose `AlertCircle` (`w-3.5 h-3.5 text-rose-400 fill-rose-500/20`).
  - **Orphan Warning (`data-testid="orphan-warning-badge"`)**: When `isOrphan` is `true` without cycle or validation errors, renders an amber `AlertTriangle` (`text-amber-400 fill-amber-500/20`).
  - **Node Warning (`data-testid="node-warning-badge"`)**: When a non-orphan node carries `validationWarning` without errors, renders an amber `AlertTriangle` (`text-amber-400 fill-amber-500/20`).
- **Card Border States (`getNodeBorderClasses`, highest-to-lowest priority)**:
  - **Cycle Loop (`hasCycle`)**: Pulsing rose border and ring (`border-rose-500 ring-2 ring-rose-500/70 shadow-lg shadow-rose-500/25 animate-pulse`).
  - **Validation Error (`hasValidationError`)**: Dashed red border (`border-red-500 border-dashed`). When selected, adds `ring-2 ring-red-500/50`.
  - **Orphan Node (`isOrphan`)**: Dashed amber border (`border-amber-500 border-dashed`). When selected, switches to `border-amber-400 ring-2 ring-amber-500/50`.
  - **Category Default & Selected Rings**: Solid `border-emerald-500` (`SOURCE`), `border-blue-500` (`STEP`), or `border-red-500` (`SINK`). When selected, adds a matching `ring-2` glow.

#### `Join` & `MergeCdcScd` Dual-Input Handle & `BROADCAST` Specification
- **Dual-Input Geometry**: `Join` steps and Imperative `MergeCdcScd` steps enforce `min-h-[64px]` and expose two `14x14px` (`w-3.5 h-3.5`) left target handles at `left: -7`.
- **Top Left Handle (`id="left"`)**: Positioned at `top: 32%` with `#3b82f6` (`bg-blue-500`), or `#ef4444` (`bg-red-500`) on validation error.
  - Paired with an exterior `L` pill (`data-testid="join-left-handle-label"`) at `top: 32%` and `-left-6` (`text-blue-300 border-blue-500/30`).
- **Bottom Left Handle (`id="right"`)**: Positioned at `top: 68%` with `#6366f1` (`bg-indigo-500`), or `#ef4444` (`bg-red-500`) on validation error.
  - Paired with an exterior `R` pill (`data-testid="join-right-handle-label"`) at `top: 68%` and `-left-6` (`text-indigo-300 border-indigo-500/30`).
- **SDP `MergeCdcScd` Single-Input Exception**: When `paradigm === 'sdp'`, `MergeCdcScd` renders only a centered `id="left"` handle at `top: 50%` with the `L` pill. SDP owns the target streaming table inside `dp.create_auto_cdc_flow`, so the `right` handle is omitted.
- **`BROADCAST` Lookup Hint**: When `properties.broadcastRight === true` on `Join`, `PipelineNode` displays `BROADCAST` (`data-testid="node-broadcast-badge"`). Code generation wraps the right lookup branch in `F.broadcast(right_df)`.

#### `ConditionalSplit` Dynamic Multi-Output Handle Specification
- **Expanded Right Gutter & Height**: `ConditionalSplit` cards apply `pr-16 min-w-[224px]` and scale dynamic `minHeight` via `Math.max(64, splitHandles.length * 24 + 16)` px.
- **Vertical Distribution Formula**: `getConditionalSplitHandles` builds `N` ordered rule handles plus the terminal `id="else"` fallback (`splitHandles`). Each handle at index `idx` sits at `top = (((idx + 1) / (splitHandles.length + 1)) * 100)%`.
- **Named Rule Branches**: Each rule renders a `14x14px` right source handle at `right: -7` in `#10b981` (`bg-emerald-500`).
  - An interior monospace chip (`data-testid="split-handle-label-<id>"`) anchors at `right-3` with `max-w-[56px] truncate bg-emerald-500/15 border-emerald-500/30 text-emerald-300`.
- **Fallback `else` Branch**: The terminal `id="else"` handle renders at `right: -7` in `#f59e0b` (`bg-amber-500`).
  - Its interior monospace chip (`data-testid="split-handle-label-else"`) uses `bg-amber-500/15 border-amber-500/30 text-amber-300`.

#### Complete Connection Handle Matrix

| Node Archetype | Left Target Handle(s) (`14x14px`, `left: -7`) | Right Source Handle(s) (`14x14px`, `right: -7`) |
| :--- | :--- | :--- |
| **Source (`SOURCE`)** | None | Default output at `top: 50%`, `#10b981` (`bg-emerald-500`). Direct `SOURCE -> SINK` edges are valid. |
| **Sink (`SINK`)** | Default target at `top: 50%`, `#ef4444` (`bg-red-500`) | None |
| **Structural Step (`inputArity: 1`, `errorOutput: false`)** | Default target at `top: 50%`, `#3b82f6` (`#ef4444` on error) | Single `id="valid"` at `top: 50%`, `#10b981` (`bg-emerald-500`) |
| **Multi-Input Step (`inputArity: 'n'`)** | Single target at `top: 50%`, `#3b82f6` accepting `N` ordered inputs (`N-IN` badge) | `id="valid"` at `top: 50%` (`UnionByName`, `SetOperation`), or `valid`/`error` on Imperative `SparkSqlQuery` |
| **Error-Capable Step (`errorOutput: true`)** | Default target at `top: 50%`, `#3b82f6` (`#ef4444` on error) | Imperative (and SDP `DataQualityAssert`): `id="valid"` at `top: 30%` (`#10b981`) + `id="error"` at `top: 70%` (`#ef4444`). Other SDP steps: `id="valid"` at `top: 50%`. |
| **Dual-Input Step (`Join`, Imperative `MergeCdcScd`)** | `id="left"` at `top: 32%` (`#3b82f6`, `L` pill) + `id="right"` at `top: 68%` (`#6366f1`, `R` pill) | Single `id="valid"` at `top: 50%`, `#10b981` |
| **SDP `MergeCdcScd` Step (`paradigm === 'sdp'`)** | Single `id="left"` at `top: 50%` (`#3b82f6`, `L` pill) | Single `id="valid"` at `top: 50%`, `#10b981` |
| **Routing Step (`ConditionalSplit`)** | Default target at `top: 50%`, `#3b82f6` (`#ef4444` on error) | `N` rule handles (`#10b981`) + `id="else"` (`#f59e0b`) at `(((idx + 1) / (splitHandles.length + 1)) * 100)%` |

### 3. Left Node Palette (`NodesCard` & `SidebarItem`)
- **Vertical Tab Strip**: Renders vertical rotated labels (`Sources`, `Steps`, `Sinks`, `Samples`), category icons, and count badges.
- **SDP Disabled Palette State**: When `paradigm === 'sdp'`, `SidebarItem` evaluates `checkNodeSdpCompatibility` for every catalog item.
  - If SDP blocks an item (`foreachBatch`-only sinks, Spark ML steps, `CachePersist`), `SidebarItem` applies `opacity-45 cursor-not-allowed` and `aria-disabled="true"`.
  - Disabled items block drag-and-drop and click-to-add actions and render a portal `<InfoTooltip>` explaining the SDP ceiling.

### 4. Right Property Inspector (`PropertyPane` & `PropertyForm`)
- **Imperative Shared Cards**:
  - `NodeDocsCard`: Renders collapsible transform documentation above lazy-loaded property sheets.
  - `StepExecutionSettingsField`: Exposes `Read Mode` (`batch` vs `streaming`) on dual-mode sources and manual `Cache Output` overrides on batch steps.
  - `StreamingSinkSettingsField`: Exposes per-sink `outputMode`, `triggerType`, `triggerInterval`, and `checkpointLocation` on streaming sinks.
  - `SystemInstructionsField`: Appends per-node code generation prompt instructions at the bottom of every property sheet.
- **SDP Inspector Swap (`paradigm === 'sdp'`)**:
  - When `paradigm === 'sdp'`, `StreamingSinkSettingsField` returns `null` because SDP uses pipeline-level `sdpStorage` and triggered `AvailableNow` execution.
  - When `paradigm === 'sdp'`, `StepExecutionSettingsField` hides manual `.cache()` controls.
  - `SdpSinkSettingsField` renders on `SINK` nodes to configure three SDP sink contracts:
    - **Target Mode & Format**: Catalog Table (`sdpTableName`, `sdpFormat` such as `iceberg` or `delta`) vs. External Streaming Sink (`sdpSinkName`, `sdpFormat`).
    - **Mutually Exclusive Physical Layout**: Selector for `none`, `partition_cols` (`sdpPartitionCols`), or `cluster_by` (`sdpClusterBy`). Switching clears the opposite field to prevent `SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED`.
    - **Reset Protection (`pipelines.reset.allowed`)**: Toggle (`sdpResetAllowed`) emitting `table_properties={"pipelines.reset.allowed": "false"}` with guidance on `INVALID_RESETTABLE_DEPENDENCY` and external sink reset limits.

### 5. Bottom Panel (`SourceCodeTab` & `DeploymentTab`)
- **Source Code Tab**:
  - **Generator Settings (`GeneratorSettings`)**: Displays the `PySpark (Spark 4.x)` runtime badge (`data-testid="pyspark-runtime-badge"`) and output folder controls. Exposes the `AI Code Reviewer Pass` toggle (`data-testid="ai-reviewer-toggle"`) and `Generate Code` button.
  - **Codegen Model Selector & Refresh**: Renders the `CODEGEN MODEL` dropdown (`data-testid="codegen-model-select"`) and refresh button (`data-testid="refresh-codegen-models-btn"`). It loads models via `useAssetLoader('codegenModels')` and defaults to the latest Flash-class Gemini model, with `gemini-4-hydrogen` as offline fallback. If a user manually selects an available model, refreshing preserves that selection.
  - **Code Preview & Active Model Badge**: Renders syntax-highlighted file tabs (`src/pipeline.py` or `spark-pipeline.yml` + `transformations/*.py`) and live `codeGenerationProgress`. Displays `Model: <selectedModel>` inside the preview header badge (`data-testid="active-model-badge"`).
- **Deployment Tab**:
  - **Imperative Mode**: Renders the runner picker (`Local`, `DataprocCluster`, `ServerlessSpark`), Spark 4.x version selector, and `standard` tier default. Exposes `checkpointRoot`, `defaultTriggerType`, the `RUN NOW` button, and the execution terminal.
  - **SDP Mode (`SdpSettingsPanel` & `SdpRefreshControls`)**:
    - Configures `sdpStorage` (`gs://` URI outside source folders), plain-identifier `sdpCatalog` (`lake`), and `sdpDatabase`. Wires the BigLake Iceberg REST catalog (`bl://` or `gs://<bucket>`) and static Spark properties (`execution_config.properties` / `--conf`).
    - Exposes command mode (`run` vs `dry-run`) and refresh mode (`incremental`, `--refresh`, `--full-refresh`, `--full-refresh-all`) with graph-derived dataset checkboxes.
    - Enforces a dataset-named confirmation gate before full refreshes and warns on duplicate delivery for external `dp.create_sink` targets.
    - Guards against overlapping runs and displays an operational `TRUNCATE TABLE` notice for materialized views.

---

## Do's and Don'ts

### Do's
- **DO** use the 8pt grid scale (`p-2`, `p-3`, `p-4`, `p-6`, `gap-2`, `gap-4`) across all components.
- **DO** use semantic CSS variables (`var(--vscode-*)`) so every surface adapts across Dark+, Light+, and High Contrast themes.
- **DO** render floating tooltips via React Portals into `document.body` (`<InfoTooltip>`) to prevent container clipping.
- **DO** update `jobConfig` via functional state updaters (`setJobConfig((prev) => ({ ...prev, ... }))`).

### Don'ts
- **DON'T** hardcode static hex colors (`#ffffff`, `#000000`) for surfaces, borders, or text.
- **DON'T** introduce arbitrary off-grid spacing values.
- **DON'T** duplicate container-managed cards (`NodeDocsCard`, `StepExecutionSettingsField`, `StreamingSinkSettingsField`, `SdpSinkSettingsField`) inside individual node property forms.
