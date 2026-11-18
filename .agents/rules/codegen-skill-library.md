---
trigger: model_decision
description: "Load before you author or edit codegen skills (resources/codegen-skills/), runner templates (resources/templates/pyspark/), src/codegen/codegenConfig.ts, src/shared/sparkNodeCapabilities.ts, src/spark/versionMatrix.ts, catalog sparkTransform values, or any generated PySpark code guidance."
---

# Codegen Skill Library & Runner Template Invariants (`src/test/unit/codegen/codegen-skills-invariants.test.ts`)

- Load `.agents/skills/spark-connector-authoring/SKILL.md` before authoring or editing codegen skills.
- Extend existing `resources/codegen-skills/io-<connector>/SKILL.md` files for matching Sources and Sinks (one `io-*` skill per connector and zero duplicate `sparkTransform` owners).
- Restrict `{{...}}` placeholders to `MODEL_FACING_PLACEHOLDERS` and `{{spark_version}}`.
- Keep `src/codegen/codegenConfig.ts` (`STREAMING_SOURCE_TRANSFORMS`, `STREAMING_STEP_TRANSFORMS`, `PYTHON_DATASOURCE_TRANSFORMS`), `src/shared/sparkNodeCapabilities.ts`, and `src/spark/versionMatrix.ts` synchronized whenever catalog transforms change.
- Keep runner templates byte-identical in `resources/codegen-skills/runner-*/templates/python/` and `resources/templates/pyspark/`.
- Preserve the test-pinned `## Credential Handling Rules` strings in `core-pyspark/SKILL.md`.
- In generated code, do not add undocumented fallbacks. Verify official documentation or fail fast.
