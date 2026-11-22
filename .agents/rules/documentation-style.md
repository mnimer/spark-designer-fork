---
trigger: always_on
---

# Documentation & Author Credit

- **Single-Header Author Credit**: Place `Author: Mike Nimer <mikenimer@google.com>` **once per file**, inside the top module/class-level docstring only. Never add author credits to method or function docstrings.
- **ASD-STE100 Documentation Style**: Write docstrings, inline comments (`// NOTE:`, `// TODO:`, `// HACK:`, `// FIXME:`), and markdown docs in active voice with direct sentences (`<= 20` words for procedural steps, `<= 25` words for descriptive text), zero semicolons in prose, and condition-first logic ("If X, do Y.").
- **Living Docs Sync**: Keep `ARCHITECTURE.md` and `DESIGN.md` synchronized when changing subsystems, graph rules, generated repo structure, or canvas badges/handles.
