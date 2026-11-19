---
trigger: always_on
---

# Toolchain, Search & Verification

## Offline Airlock Worktree Bootstrap

Fresh git worktrees lack `node_modules`, and `npm ci` fails in Airlock (`vite@8.3.x` is unavailable). Before running `tsc`, `vitest`, `vite`, or `eslint` in any worktree, link packages via hardlink copy:

```bash
test -d node_modules || cp -al /usr/local/google/home/mikenimer/Development/apache-spark-designer/node_modules ./node_modules
test -d webview/node_modules || cp -al /usr/local/google/home/mikenimer/Development/apache-spark-designer/webview/node_modules ./webview/node_modules
```

Never stage `node_modules/`, `out/`, or `webview/dist/`.

## Standalone Git Search & Dual-Toolchain Split (`root` vs. `webview/`)

- This repository lives outside CitC and Cog, so `cs --corpus=local` fails immediately. Use `git grep` and `git ls-files` instead.
- Never run root `npx tsc -p webview/tsconfig.app.json` (root TS 5.9 produces false `unknown`/`never` errors on TS 6.0 webview files). Run `npm run typecheck:webview` instead.
- Root `npm run lint` (`eslint src`) skips `webview/**`. Run `npm --prefix webview run lint`, which enforces `react-refresh/only-export-components` (keep non-component helpers in `.ts` files) and JSDoc on `TSInterfaceDeclaration` and exported arrow components.
- Root `npm test` runs only `src/test/unit/**/*.test.ts`. Run `npm run test:webview` (or `(cd webview && npx vitest run <paths>)`) for webview tests, or `npm run test:all` for both suites.
- Root `npm run compile` skips `src/test/integration/`, because the root `tsconfig.json` excludes it. Run `npm run typecheck:integration` to typecheck the evaluator harness.
- The `webview-csp` unit tests skip if `webview/dist/` is missing. Run `npm run build:webview` before `npm run test:all` when you report test counts.

## Offline PySpark `pytest` Runner

- PySpark 4.0 cannot start a local session on Java 27 (`getSubject is not supported`). If the host JDK is newer than 21, the runner skips its tests.
- To run the generated-repo tests, set `JAVA_HOME` to JDK 17 or 21. Set `SPARK_PYTEST_PYTHON` to a Python that has `pyspark>=4` and `pytest`. Then run `npm run test:integration:pytest`.
- The runner writes a root `conftest.py` with `master("local[2]")`. The deterministic gate exempts `conftest.py`, `tests/`, and `run_local.sh` from the banned local-master scan. Keep that exemption.

## Unit Test & Lint Coverage Gate

Every new or modified module requires Vitest unit tests covering normal, boundary, and failure conditions. The following commands must all exit `0`:

- `npm run compile`
- `npm run typecheck:webview`
- `npm run typecheck:integration`
- `npm run lint`
- `npm --prefix webview run lint`
- `npm run build:webview`
- `npm run test:all`
