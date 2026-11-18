---
trigger: model_decision
description: "Load before you create or edit React components, property forms (webview/src/properties/), PropertyForm.tsx, propertyFormRegistry.ts, catalog documentation (webview/src/catalog/documentation/), styling, or React Testing Library tests under webview/src/."
---

# Webview UI & Design System (`DESIGN.md`)

## Layout, Styling & Component Contracts

- Strictly follow the 8pt spacing grid (`p-2`, `p-3`, `p-4`, `gap-2`, `gap-4`) and `var(--vscode-*)` theme tokens.
- Nested React components accept props down and emit events up via `onChange`. Include `<InfoTooltip text="..." />` on property fields.
- Reuse `StorageBackendFields` (`webview/src/components/common/StorageBackendFields.tsx`) across all File Format sources and sinks.
- `PropertyForm.tsx` automatically renders `<NodeDocsCard />`, `<StepExecutionSettingsField />` (for `STEP` nodes), and `<StreamingSinkSettingsField />` (for streaming `SINK` nodes) above the lazy form. Never render those three wrapper cards inside individual Source, Sink, or Step panels, and always render `<SystemInstructionsField />` at the bottom of each panel.

## React Testing Library Queries

- Because `<InfoTooltip>` renders `<span role="tooltip" aria-label="...">` inside `<label>`, pass `{ selector: 'input' }` / `{ selector: 'select' }` / `{ selector: 'textarea' }` to `getByLabelText(...)` (or use `getByRole`) so label queries never collide with tooltip text.

## Property Registry Parity (`webview/src/catalog/propertyFormRegistry.test.ts`)

- `Object.keys(PROPERTY_FORM_REGISTRY).length` must equal `Object.keys(NODE_DOCUMENTATION_REGISTRY).length` (`webview/src/catalog/documentation/`).
- Every registry entry must use `lazyForm(...)`, and alias keys must reuse the exact same `lazyForm` constant so distinct wrappers resolve to distinct components.
