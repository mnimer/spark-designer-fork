---
name: "Apache Spark Pipeline Designer"
author: "Mike Nimer <mikenimer@google.com>"
description: "Unified UI/UX design system and token specification for the visual Apache Spark and Managed Spark pipeline builder VS Code extension."
version: "2.1.0"
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

The Apache Spark Pipeline Designer provides a high-contrast visual IDE for authoring, configuring, and inspecting Imperative PySpark 4.x and Spark Declarative Pipelines (SDP, Apache Spark 4.2+) directed acyclic graphs (DAGs).

### Design Principles
1. **8pt Grid Discipline**: All paddings, margins, component heights, and gaps follow multiples of `8px`, with `4px` half-steps reserved for compact badges, inputs, and borders.
2. **Semantic VS Code Theming**: All UI surfaces, borders, and typography derive from CSS custom properties (`--vscode-*`), supporting Dark+, Light+, High Contrast Dark (WCAG AAA), and High Contrast Light (WCAG AAA).
3. **Tri-Color DAG Taxonomy**: Pipeline stages remain instantly identifiable through category color coding:
   - **Sources**: Emerald Green (`#10b981`)
   - **Transforms / Steps**: Azure Blue (`#3b82f6`)
   - **Sinks**: Rose Red (`#ef4444`)
   - **Samples**: Purple / Violet (`#a855f7`)
4. **Paradigm-Aware Visual Feedback**: Switching between Imperative PySpark and Spark Declarative Pipelines updates toolbar pills, node micro-badges, palette item states, and property inspector controls in lockstep.

---

## Colors

### Surfaces & Neutrals
- **Canvas Background (`--vscode-canvas-bg`)**: Deep slate foundation (`#0d1117`) for the React Flow canvas.
- **Workbench & Sidebars (`--vscode-workbench-bg`)**: `#161b22` providing contrast against the canvas.
- **Card Surfaces (`--vscode-card-bg`)**: `#161b22` for floating node cards, draggable palette items, and controls.
- **Subcards & Code Containers (`--vscode-subcard-bg`)**: `#0d1117` recessed surfaces for inputs and syntax previews.
- **Borders (`--vscode-border`)**: Subtle dividers (`#30363d`) and hairline borders (`#21262d`).

### Pipeline Category Accents
- **Sources (`#10b981`)**: Data ingestion (`KafkaSource`, `BigQuerySource`, `IcebergSource`, `CsvSource`, `JsonSource`, `ParquetSource`, `SpannerSource`).
  - Background: `bg-emerald-500/10`, Border: `border-emerald-500/30`, Text: `text-emerald-400`
- **Transforms & Steps (`#3b82f6`)**: Catalyst and streaming transformations (`Filter`, `WithColumns`, `Watermark`, `GroupByAgg`, `Join`, `MergeCdcScd`, `DataQualityAssert`).
  - Background: `bg-blue-500/10`, Border: `border-blue-500/30`, Text: `text-blue-400`
- **Sinks (`#ef4444`)**: Terminal output destinations (`IcebergSink`, `DeltaSink`, `BigQuerySink`, `KafkaSink`, `ParquetSink`, `ConsoleSink`).
  - Background: `bg-rose-500/10`, Border: `border-rose-500/30`, Text: `text-rose-400`
- **Samples (`#a855f7`)**: Starter `.spark` templates with `PySpark` or `SDP (4.2+)` paradigm badges.
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
- **Pipeline Mode Pill (`data-testid="toolbar-pipeline-mode"`)**: Displays `BATCH` or `STREAMING` based on deterministic streaming propagation.
- **Paradigm Indicator & Switcher (`data-testid="toolbar-paradigm-indicator"`)**: Displays `IMPERATIVE` or `SDP (4.2+)` in the top canvas toolbar.
  - Clicking to switch from `SDP (4.2+)` to `IMPERATIVE` succeeds immediately and records an undo snapshot.
  - Clicking to switch from `IMPERATIVE` to `SDP (4.2+)` runs `evaluateSdpCompatibility`. If blocking constructs exist, the editor opens an accessible modal (`role="dialog"`, `data-testid="sdp-compatibility-modal"`) listing each incompatible node label, transform, and SDP ceiling reason.

### 2. DAG Node (`PipelineNode`) & Micro-Badges
- **Container**: `bg-[var(--vscode-node-bg)] border rounded-md shadow-xl p-3 flex items-center gap-3 min-w-[180px]`.
- **Imperative Mode Micro-Badges**:
  - `BATCH` / `STREAM` (`data-testid="node-mode-badge"`): Rendered on `SOURCE` and `SINK` cards.
  - `CACHE` (`data-testid="node-cache-badge"`): Rendered on batch fan-out nodes planned by `executionPlanner.ts`.
  - `foreachBatch` (`data-testid="node-foreachbatch-badge"`): Rendered on sinks grouped into `.writeStream.foreachBatch(...)`.
  - `BROADCAST` (`data-testid="node-broadcast-badge"`): Rendered on `Join` steps when `broadcastRight` is enabled.
  - `N-IN` (`data-testid="node-multi-input-badge"`): Rendered on multi-input steps (`UnionByName`, `SetOperation`, `SparkSqlQuery`).
- **SDP Mode Dataset Micro-Badges (`data-testid="node-sdp-badge"`)**:
  - When `paradigm === 'sdp'`, `PipelineNode` suppresses `CACHE` and `foreachBatch` badges and renders deterministic SDP dataset badges from `sdpPlanner.ts`:
    - `VIEW`: `@dp.temporary_view` non-persisted intermediate view
    - `MV`: `@dp.materialized_view` batch recomputed table
    - `TABLE`: `@dp.table` or `dp.create_streaming_table` + `@dp.append_flow` streaming table
    - `CDC`: `dp.create_auto_cdc_flow` SCD Type 1 target flow
    - `SINK`: `dp.create_sink` external streaming sink
- **Connection Handles**:
  - `Source`: One right output handle (`bg-emerald-500`). Direct `SOURCE -> SINK` connections are valid.
  - `Step (Standard)`: One left target handle (`bg-blue-500`) and one right output handle (`id="valid"`).
  - `Step (Error-Capable)`: Adds a second right handle (`id="error"` in rose/red) in imperative mode for dead-letter routing.
  - `Step (Join / MergeCdcScd)`: Two labeled left handles (`id="left"` and `id="right"`). In SDP mode, `MergeCdcScd` requires only the `left` streaming CDC handle because SDP owns the target table.
  - `Step (ConditionalSplit)`: Dynamic right output handles (one per rule plus `id="else"`).
  - `Sink`: One left target handle (`bg-rose-500`).

### 3. Left Node Palette (`NodesCard` & `SidebarItem`)
- **Vertical Tab Strip**: Vertical rotated labels (`Sources`, `Steps`, `Sinks`, `Samples`), category icons, and count badges.
- **SDP Disabled Palette State**: When `paradigm === 'sdp'`, `SidebarItem` evaluates `checkNodeSdpCompatibility` for every catalog item. Unsupported items (`foreachBatch`-only sinks, Spark ML steps, and `CachePersist`) render with `opacity-45 cursor-not-allowed` and `aria-disabled="true"`, block drag-and-drop and click-to-add actions, and display a portal `<InfoTooltip>` explaining why SDP blocks the node.

### 4. Right Property Inspector (`PropertyPane` & `PropertyForm`)
- **Imperative Shared Cards**:
  - `StepExecutionSettingsField`: Exposes `Read Mode` (`batch` vs `streaming`) on dual-mode sources and manual `Cache Output` overrides on batch steps.
  - `StreamingSinkSettingsField`: Exposes per-sink `outputMode`, `triggerType`, `triggerInterval`, and `checkpointLocation` on streaming sinks.
- **SDP Inspector Swap (`paradigm === 'sdp'`)**:
  - `StreamingSinkSettingsField` returns `null` (hiding per-sink trigger and checkpoint overrides because SDP uses pipeline-level `sdpStorage` and triggered `AvailableNow` execution).
  - `StepExecutionSettingsField` hides manual `.cache()` controls.
  - `SdpSinkSettingsField` renders on `SINK` nodes to configure:
    - **Target Mode & Format**: Catalog Table (`sdpTableName`, `sdpFormat` such as `iceberg` or `delta`) vs. External Streaming Sink (`sdpSinkName`, `sdpFormat`).
    - **Mutually Exclusive Physical Layout**: Selector for `none`, `partition_cols` (`sdpPartitionCols`), or `cluster_by` (`sdpClusterBy`), clearing the opposite field on switch to prevent `SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED`.
    - **Reset Protection (`pipelines.reset.allowed`)**: Toggle (`sdpResetAllowed`) emitting `table_properties={"pipelines.reset.allowed": "false"}` for streaming catalog tables, accompanied by guidance on `INVALID_RESETTABLE_DEPENDENCY` and external sink reset limits.

### 5. Bottom Panel (`SourceCodeTab` & `DeploymentTab`)
- **Source Code Tab**:
  - **Generator Settings (`GeneratorSettings`)**: Displays the `PySpark (Spark 4.x)` runtime badge, output directory controls, optional `AI Code Reviewer Pass` toggle, and `GENERATE CODE` CTA.
  - **Codegen Model Selector & Refresh**: Renders the `CODEGEN MODEL` dropdown (`data-testid="codegen-model-select"`) and refresh button (`data-testid="refresh-codegen-models-btn"`) inside `GeneratorSettings`. Populates options via `useAssetLoader('codegenModels')` and defaults to the latest Flash-class Gemini model, with `gemini-4-hydrogen` as the offline fallback. If a user manually selects a model, refreshing preserves that selection while it remains available.
  - **Code Preview & Active Model Badge**: Renders syntax-highlighted file tabs (`src/pipeline.py` or `spark-pipeline.yml` + `transformations/*.py`), live `codeGenerationProgress` status, and the header badge (`data-testid="active-model-badge"`) displaying `Model: <selectedModel>`.
- **Deployment Tab**:
  - **Imperative Mode**: Runner picker (`Local`, `DataprocCluster`, `ServerlessSpark`), Spark 4.x runtime version selector, `standard` tier default, `checkpointRoot`, `defaultTriggerType`, `RUN NOW` CTA, and execution terminal.
  - **SDP Mode (`SdpSettingsPanel` & `SdpRefreshControls`)**:
    - Pipeline-level `sdpStorage` (`gs://` URI outside source folders), plain-identifier `sdpCatalog` alias (`lake`), `sdpDatabase`, BigLake Iceberg REST catalog builder (`lakehouse` `bl://` or root `gcs_bucket` `gs://<bucket>`), and static Spark property routing (`execution_config.properties` / `--conf`).
    - Command mode (`run` vs `dry-run`), refresh mode (`incremental`, `--refresh`, `--full-refresh`, `--full-refresh-all`) with checkboxes populated strictly from graph datasets, explicit dataset-named confirmation gate before full refreshes, duplicate-delivery warning banner for external `dp.create_sink` targets, overlapping-run guard, and materialized view `TRUNCATE TABLE` operational notice.

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
