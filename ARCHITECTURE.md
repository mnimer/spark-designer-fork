# Architecture Guide: DataFlow Pipeline Builder

> **Author**: Mike Nimer <mikenimer@google.com>  
> **Version**: 1.0.0  
> **Purpose**: This document outlines the software architecture, runtime boundaries, inter-process communication (IPC) protocols, state synchronization models, and Gemini AI integration for the **DataFlow Pipeline Builder** VS Code extension.

---

## High-Level Overview

The DataFlow Pipeline Builder is an interactive visual IDE extension for authoring, configuring, validating, and deploying Apache Beam pipelines on Google Cloud Dataflow. It executes across two isolated runtime contexts connected via an asynchronous IPC bridge:

1. **VS Code Extension Host (Backend)**: Runs in Node.js on the local workstation. It manages the filesystem, handles workspace commands, executes local GCP CLI operations (`gcloud`, `bq`), and communicates with Vertex AI via the `@google/genai` SDK.
2. **Webview Canvas (Frontend)**: Runs inside a sandboxed browser context. Built with React, React Flow, and Tailwind CSS, it renders the interactive DAG graph, palette drawers, property inspectors, and code preview tabs.

```mermaid
graph TD
    subgraph "VS Code Extension Host (Node.js)"
        Ext["extension.ts (Controller)"]
        Prov["BeamEditorProvider (Custom Text Editor)"]
        GenAI["@google/genai (Gemini SDK client)"]
        Tree["DataflowJobsProvider (Sidebar Tree View)"]
    end

    subgraph "Webview Context (React + React Flow)"
        App["App.tsx (State Coordinator)"]
        Flow["React Flow Canvas (Visual Editor)"]
        Palette["NodesCard (Left Drawer Palette)"]
        Props["PropertyPane (Right Configuration Inspector)"]
        Bottom["BottomPanel (Source Code & Deployment Tabs)"]
        Theme["ThemeContext (VS Code Theme Observer)"]
    end

    subgraph "Local Workstation & Google Cloud"
        Disk["Workstation Filesystem (.beam & generated files)"]
        CLI["gcloud / bq CLIs (Local tools)"]
        GeminiAPI["Gemini API (Vertex AI Cloud GenAI)"]
        Dataflow["Google Cloud Dataflow (Execution Engine)"]
    end

    %% Webview Internal Communication
    App --> Flow
    App --> Palette
    App --> Props
    App --> Bottom
    Theme --> App

    %% IPC Bridge (VS Code Webview Message Channel)
    App -- "vscode.postMessage()" --> Ext
    Ext -- "webview.postMessage()" --> App

    %% Extension Operations
    Ext --> Disk
    Ext -- "child_process.exec()" --> CLI
    Ext --> GenAI
    Ext --> Tree

    %% External APIs
    GenAI --> GeminiAPI
    CLI --> Dataflow
```

---

## 1. Extension Host (`src/extension.ts`)

The Extension Host serves as the controller and native operating system interface for the extension.

### Core Responsibilities
- **Activation Lifecycle**: Registers commands (`refreshPipelines`, `showEditor`, `runPipeline`), custom editor providers, and tree view providers upon activation.
- **`BeamEditorProvider`**: Implements `vscode.CustomTextEditorProvider` to register as the primary visual editor for `*.beam` files. It manages the HTML webview lifecycle, loads React bundle assets (`main.js`, `index.css`), and handles document save/sync lifecycles.
- **IPC Command Dispatcher (`handleMessages`)**: Processes incoming JSON messages from the React webview and returns structured payloads.
- **GCP Environment Discovery**: Executes local CLI commands under the hood (`bq ls --format=json`, `gcloud compute networks subnets list --format=json`) to populate dropdown options in property forms with live project assets.
- **Gemini Client Manager**: Initializes the `@google/genai` client using the active project retrieved dynamically from `gcloud config get project`, pointing the client to Vertex AI (`location: 'global'`).
- **Sidebar Jobs Provider (`DataflowJobsProvider`)**: A dedicated native VS Code Tree View in the activity bar that lists active and historical Dataflow pipeline jobs.

### IPC Message Protocol Registry

| Direction | Command | Payload | Description |
| :--- | :--- | :--- | :--- |
| **Webview &rarr; Ext** | `savePipeline` | `{ yamlContent }` | Persists visual DAG state to the open `*.beam` YAML file. |
| **Webview &rarr; Ext** | `regenerateCode` | `{ yamlContent, language, outputFolder }` | Invokes Gemini to generate complete source code and dependencies. |
| **Webview &rarr; Ext** | `readFolderFiles` | `{ folderPath }` | Queries directory contents to populate generated file tabs. |
| **Webview &rarr; Ext** | `readFileContent` | `{ filePath }` | Loads file text for the syntax-highlighted code editor preview. |
| **Webview &rarr; Ext** | `pickFolder` | `—` | Opens native VS Code folder selection dialog. |
| **Webview &rarr; Ext** | `runPipeline` | `{ jobConfig, language, outputFolder }` | Executes local run script or submits Dataflow job. |
| **Webview &rarr; Ext** | `fetchSubnetworks` | `—` | Queries GCP subnets via `gcloud` CLI. |
| **Ext &rarr; Webview** | `pipelineLoaded` | `{ yamlContent }` | Sends initial `.beam` file content on editor launch. |
| **Ext &rarr; Webview** | `codeRegenerated` | `{ files, mainCode, success }` | Returns generated code and updates code preview. |
| **Ext &rarr; Webview** | `codeGenerationProgress` | `{ message }` | Progress text of a long generation run (optional reviewer steps). |
| **Ext &rarr; Webview** | `folderFilesLoaded`| `{ files }` | Returns updated directory file list. |
| **Ext &rarr; Webview** | `subnetworksLoaded`| `{ subnetworks }` | Returns available GCP subnets for deployment configuration. |

---

## 2. Webview Frontend (`webview/src/`)

The frontend is a single-page application built with React 19, TypeScript, React Flow, and Tailwind CSS, bundled with Vite.

### Component Architecture & Hierarchy

```
App.tsx (Root State Coordinator & Drag Context)
 ├── ThemeProvider (Auto-synchronizes VS Code theme classes)
 ├── ReactFlowProvider
 │    ├── ReactFlow Canvas (Infinite Grid, Background, Edges)
 │    │    ├── PipelineNode (Custom Node Component with Inline Renaming)
 │    │    ├── Controls (Zoom, Fit-view, Auto-layout, Minimap toggle)
 │    │    └── MiniMap (Theme-adaptive category colored nodes)
 │    ├── NodesCard (Left Slide-out Palette)
 │    │    ├── VerticalTabStrip (Sources, Steps, Sinks, Samples)
 │    │    ├── SearchInput (Live filtering)
 │    │    └── SidebarItem (Draggable & Click-to-add items)
 │    ├── PropertyPane (Right Slide-out Inspector)
 │    │    ├── NodeHeader (Editable label, ID, category tag)
 │    │    ├── PropertyForm (Dynamic transform property sheet dispatcher)
 │    │    │    ├── properties/source/*, properties/sink/*, properties/step/* (Per-transform panels)
 │    │    │    └── components/common/* (PropertyField, SecretManagerAuthFields,
 │    │    │         StepExecutionSettingsField, StreamingSinkSettingsField)
 │    │    ├── InfoTooltip (Portal-based documentation popup)
 │    │    └── RuntimeVariables (Copyable dynamic variable drawer)
 │    ├── CanvasToolbar (Undo, Redo, Auto-layout, and Pipeline Mode BATCH/STREAMING pill)
 │    └── BottomPanel (Resizable Drawer Container)
 │         ├── SourceCodeTab (Language picker, Output folder, Code preview)
 │         └── DeploymentTab (Runner config, Streaming defaults, Instance bounds, RUN NOW)
```

### Phase 1 Spark DAG Semantics, Validation & Deterministic Execution Planner

The extension host and React webview share a pure TypeScript graph engine under `src/shared/` and `src/spark/`:

- **Node Capability Registry (`src/shared/sparkNodeCapabilities.ts`)**: Declares read modes, streaming sink modes, DLQ capability, and input arity across all Spark transforms.
- **Direct Source-to-Sink Edges (`usePipelineGraph.ts`, `dagValidator.ts`)**: Allows direct `SOURCE -> SINK` edges for zero-transform replication jobs while rejecting self-loops and invalid directions.
- **Streaming Propagation (`src/shared/sparkDagRules.ts`)**: `computeStreamingNodeSet` marks downstream nodes as streaming while keeping static batch lookup branches for **Stream–Static Joins**.
- **Spark Topology Validation (`src/shared/dagValidator.ts`)**: Enforces acyclic graphs, `Join` handle occupancy, stream–static join rules, stream–stream outer join watermarks, arity, and checkpoint validation.
- **Deterministic Execution Planner (`src/shared/executionPlanner.ts`)**: `buildExecutionPlan` computes topological order, batch fan-out `.cache()` and `.unpersist()` points, DLQ splits, and `foreachBatch` sink groups.
- **Spark 4.x Runtime Matrix (`src/spark/versionMatrix.ts`)**: Maps Serverless 4.0, Serverless 3.0, Cluster 3.0, and Local runtimes (`standard` tier default) to JARs, Maven coordinates, and Python `DataSource` fallbacks.

---

## 3. State Synchronization & Code Generation

Pipeline configurations are persisted as human-readable YAML inside `*.spark` files.

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant Webview as Webview (React)
    participant Ext as Extension Host (Node.js)
    participant Builder as Prompt Builder (src/codegen)
    participant Gemini as Gemini API (gemini-3.7-flash)
    participant Disk as Filesystem

    User->>Webview: Edit Pipeline Canvas (add/move/rename/configure node)
    Note over Webview: State updates, marks needsRebuild = true
    Webview->>Ext: savePipeline (YAML Config)
    Ext->>Disk: Write pipeline.beam configuration file

    User->>Webview: Click "GENERATE CODE"
    Webview->>Ext: regenerateCode (YAML Config, Language, Path)
    Note over Ext: Validates the DAG, gates the output folder
    Ext->>Builder: buildPromptFromPipelineYaml (YAML, language, Beam version)
    Note over Builder: 1. Redact credentials<br/>2. Detect transforms and features<br/>3. Select skills<br/>4. Assemble one prompt
    Builder-->>Ext: prompt + selected skill names
    Ext->>Gemini: ONE generateContent call (prompt + JSON schema)
    Gemini-->>Ext: Structured JSON (files array)
    opt Reviewer setting is on (default off)
        Ext->>Ext: Read-only checks (no model call)
        Ext->>Gemini: Review call (findings JSON)
        Ext->>Gemini: Fix call for blocking findings (max 2 rounds)
        Ext-->>Webview: codeGenerationProgress (one message per step)
    end
    Ext->>Disk: Stage, scoped cleanup, commit files, gemini.log, manifest
    Ext-->>Webview: codeRegenerated (Success + main pipeline code)
    Note over Webview: Sets needsRebuild = false, updates preview
    Webview->>Ext: readFolderFiles
    Ext-->>Webview: folderFilesLoaded (Refreshes file tabs)
```

### Prompt Assembly from Skills & Response Schema
Code generation makes ONE model call. The prompt is assembled from small topic skills. No prompt text lives in TypeScript.

- **Skills**: `resources/codegen-skills/<name>/SKILL.md` (language-neutral rules) plus optional `python.md` / `java.md` (SDK specifics) and `templates/<language>/<file>` (runner templates). See `resources/codegen-skills/README.md`.
  - One skill per IO connector (`io-*`, one per catalog `beamTransform`), one per transform group (`transforms-*`), one per runner (`runner-direct`, `runner-dataflow`, `runner-spark`), language skills (`lang-*`, `packaging-*`, `testing-*`), cross-cutting skills (`credentials-and-options`, `graph-wiring`), feature-gated skills (`side-inputs`, `multi-output`, `streaming-windowing`, `custom-instructions`, `cross-language-transforms`), and `verification-gate` (always last).
- **Pipeline** (`src/codegen/`):
  1. `credentialRedaction.redactPipelineDocument()` strips credentials. It runs first because `gemini.log` stores the prompt.
  2. `PipelineFeatureDetector` reads the parsed YAML: `beam_transform` set, runners of the language, features (`side_inputs`, `multi_output`, `streaming`, `custom_instructions`, `xlang`).
  3. `CodegenSkillCatalog.select()` filters by caller, language, runner, kind, transforms, and features. The order is deterministic (kind, then name).
  4. `CodegenPromptBuilder.build()` emits `codegen-common`, then `## Pipeline YAML` (verbatim), then the redaction notice, then each `## Skill: <name>`. It resolves `{{beam_version}}` in skill text only (fix F7).
- **Shared entry point**: `buildPromptFromPipelineYaml()`. The extension (`codeGeneration.ts`) and the integration runner (`codegen-runner.ts`) both call it, so the tests send the same prompt as users. The selected skill names go to the output channel and to `gemini.log`.
- **Optional reviewer**: setting `dataflowDesigner.codegen.enableReviewer` (default `false`). `ReadOnlyCheckRunner` runs fast checks, `CodeReviewer` makes one review call and up to two fix calls. Templates: `resources/codegen-prompts/{reviewer,fix}.md`. Reviewer skills use `usedBy: [reviewer]`.
- **Response JSON Schema**: `resources/code-gen-schema.json`
  - Enforces structured JSON output with an array of files containing `filename`, `content`, and `executable` boolean flags.

---

## 4. Multi-Agent Evaluation Harness (Integration Testing)

To validate that LLM-generated Apache Beam repositories comply with Beam DAG semantics, security hardening, build deployability, and specification fidelity without human intervention, integration tests execute a multi-agent evaluation hierarchy:

1. **Deterministic Pre-Flight Gate (`src/test/integration/evaluators/deterministic-gate.ts`):** Fast non-LLM checks running `py_compile` / Java brace matching and regex scanning for plaintext secrets/credentials.
2. **Specialist Sub-Agents (concurrent via `Promise.all`):**
   - **`BeamArchitectureAgent` (`src/test/integration/evaluators/beam-architecture-evaluator.ts`):** Enforces distributed streaming semantics, windowing on unbounded data, DoFn output tagging integrity (preventing silent element drops), and DLQ error routing.
   - **`PipelineSpecificationAgent` (`src/test/integration/evaluators/pipeline-spec-evaluator.ts`):** Parses the input pipeline YAML and audits the generated code to verify 100% property completeness (e.g. table names, connection URLs, Kafka topics, serializers, flags), custom `system_instructions`, `runtimeVariables`, and `jobConfig`.
   - **`SecurityAndHardeningAgent` (`src/test/integration/evaluators/security-evaluator.ts`):** Audits Secret Manager credentials, container hardening (non-root execution), and shell quoting.
   - **`PackagingAndDeployabilityAgent` (`src/test/integration/evaluators/packaging-evaluator.ts`):** Verifies dependency constraints (e.g. Datafaker >= 2.7.0), build descriptors (`pom.xml`, `requirements.txt`), and runner scripts.
3. **Lead Arbiter & Synthesizer Agent (`src/test/integration/evaluators/arbiter-evaluator.ts`):** Computes multi-dimensional rubric scorecards (Architecture, Specification, Security, Packaging, Maintainability), enforces hard vetoes on critical defects, and emits actionable **Prompt Remediation Directives** that each target one skill file (`targetSkill`, for example `resources/codegen-skills/io-kafka/python.md`) rather than patching generated code.

---

## 5. UI/UX Design System Cross-Reference

For UI design tokens, 8pt spacing grid specifications, typography hierarchies, category color codes, and component styling rules, see `DESIGN.md`.
