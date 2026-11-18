---
trigger: model_decision
description: "Load before you create or edit any file under src/shared/ or src/spark/ (cross-boundary TypeScript shared by the Extension Host and Webview), or any webview TypeScript that must pass verbatimModuleSyntax and erasableSyntaxOnly checks, or any JSDoc comment."
---

# Cross-Boundary Purity (`src/shared/*` & `src/spark/*`)

- Zero imports of `vscode`, `fs`, `path`, `child_process`, `os`, `react`, or `node:*` (enforced by `src/test/unit/shared-purity.test.ts`).
- `webview/tsconfig.app.json` enforces `"verbatimModuleSyntax": true` and `"erasableSyntaxOnly": true`: always use `import type { ... }` for type-only imports and never declare TypeScript `enum`s or parameter properties. Run `npm --prefix webview run typecheck` so `webview/node_modules/.bin/tsc` validates webview TypeScript.
- Never write a literal star-slash sequence inside a JSDoc comment (for example, a glob such as `runner-` + `*` + `/templates/`). It closes the comment block and breaks `tsc`.
