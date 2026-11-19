---
name: phase-swarm-execution
description: >-
  Executes a phase of tasks from TASKS.md using a parallel /swarm with disjoint
  shared git worktrees, atomic per-task commits, zero-regression verification
  against develop, TASKS.md status updates, and a GitHub PR description artifact.
  Use when asked to execute a phase or batch of TASKS.md items with /swarm or
  parallel subagents and commit each completed task.
---

# Phase Swarm Execution Playbook

## 1. Sync Worktree & Partition Tasks Without Collisions

1. **Fast-forward the phase worktree to `origin/develop` before spawning workers**: Pre-created phase worktrees often point to an older `develop` commit. Because `swarm{action: "add", workspace: "share"}` branches each worker worktree from the coordinator's current `HEAD`, fast-forward strictly to `origin/develop` (never onto an un-squashed sibling feature branch) and link `node_modules` first. If `origin/develop` lacks a merged prerequisite phase, ask the user to pull `develop` first:
   ```bash
   git merge --ff-only origin/develop
   test -d node_modules || cp -al /usr/local/google/home/mikenimer/Development/apache-spark-designer/node_modules ./node_modules
   test -d webview/node_modules || cp -al /usr/local/google/home/mikenimer/Development/apache-spark-designer/webview/node_modules ./webview/node_modules
   ```
2. **Record the base verification baseline before you spawn workers**: Run the full verification gate on the base commit in a detached temp worktree. Record the failing test names and the webview lint error count. Put these numbers in every brief, so that authors and verifiers judge "zero regressions" against one shared baseline. If base tests fail, decide ownership of each failure up front.
3. Read `TASKS.md`, `AGENTS.md`, `DESIGN.md`, and target interfaces (plus `.agents/skills/spark-connector-authoring/SKILL.md` for connector or step catalog phases) before briefing workers.
4. Group tasks into 3–6 author slices (`workspace: "share"`) with **100% disjoint file ownership**, plus:
   - `integrator` (`workspace: "inherit"` on the phase worktree branch)
   - `verifier` (or split `verifier-a` / `verifier-b` on `workspace: "inherit"`, strictly read-only)
   - `monitor` (when `swarm_monitor` is in `<skills>`)
   Verify every parallel author `swarm{action: "add"}` call explicitly sets `workspace: "share"`. Never leave an author on default `inherit`.
   Load `swarm_pane` and write `<appDataDir>/brain/<conversation-id>/.system_generated/swarm/plan.json` right after spawning workers (do not add a `narrator` unless requested). Route peer handoffs directly (`author -> verifier [Stage 1] -> integrator -> verifier [Stage 2] -> coordinator`) so `@coordinator` never becomes a bottleneck.
5. **Reserve shared barrel and registry files for `integrator`**: In catalog phases (`SRC*`, `SNK*`, `OP*`), authors own only isolated `resources/codegen-skills/*` and `webview/src/properties/*` files. Keep `webview/src/catalog/nodes/*.ts`, `webview/src/catalog/propertyFormRegistry.ts`, `webview/src/catalog/documentation/*.ts`, `webview/src/catalog/paletteGroupState.ts`, `webview/src/utils/legacyNodeMigration.ts`, `src/codegen/codegenConfig.ts`, `src/spark/versionMatrix.ts`, `src/test/unit/codegen/CodegenSkillCatalog.test.ts`, `src/test/unit/codegen/codegen-skills-invariants.test.ts`, and `TASKS.md` exclusively in `integrator`.
6. Leave `model` unset on `swarm{action: "add"}` unless the user requests another model. If a worker hits `503 No capacity available` or quota exhaustion while `waiting`, switch its model in place with `swarm{action: "model", name, model}` and inform the user. Never `kill` and re-`add` a stalled worker because `kill` deletes its worktree and context.
7. **Assign full vertical IPC call chains**: If a task sends state or actions between the Webview and Extension Host, assign the complete call path (`Leaf Component -> Context -> App.tsx -> useVsCodeBridge.ts -> src/shared/protocol.ts -> src/ipc/handlers/*.ts`) to one owner (or explicitly assign the bridge files) so options are never dropped at unowned middle layers.
8. **Allow stacked author branches for signature dependencies**: If one author needs another author's new function signature, the dependent author may build on the other branch. Each author must post the full commit chain (for example `A1 -> A2 -> B1`). Brief the integrator to cherry-pick in dependency order and to fold side-branch fixups into their target task commit.

## 2. Author Worktree Brief Requirements

Include these 5 rules in every `workspace: "share"` author brief:

1. **Verify isolated worktree & bootstrap `node_modules` via hardlinks**: Never run `npm ci` or `git worktree add` into scratch folders. If `pwd` is the shared phase worktree instead of a `swarm-*` worktree, stop and alert `@coordinator`:
   ```bash
   test -d node_modules || cp -al /usr/local/google/home/mikenimer/Development/apache-spark-designer/node_modules ./node_modules
   test -d webview/node_modules || cp -al /usr/local/google/home/mikenimer/Development/apache-spark-designer/webview/node_modules ./webview/node_modules
   ```
2. **Search with Git, not `cs`**: Never run `cs --corpus=local` (fails outside CitC and Cog). Use `git grep` and `git ls-files`.
3. **Enforce file boundaries & webview lint rules**: Touch only owned files. Never stage `node_modules/`, `out/`, or `webview/dist/`. Place any agent skills or rules under `.agents/` (never `.gemini/`). In `webview/src/`, keep non-component helpers in `.ts` modules (`react-refresh/only-export-components`), document every `interface` and exported arrow component with JSDoc, and pass `{ selector }` to `getByLabelText(...)` when `<InfoTooltip>` sits inside `<label>`.
4. **Commit verified work cleanly**: After targeted unit tests (`npx vitest run <paths>` for host and `(cd webview && npx vitest run <paths>)` for webview), typecheck (`npm run typecheck` and `npm run typecheck:webview`), lint (`npm run lint` and `npm --prefix webview run lint`), single-header `Author:` check, and ASD-STE100 zero-semicolon check pass, commit as `<type>(<scope>): <summary> (<TASK_ID>)` with **zero file paths, zero file extensions (including bare `.spark`, `.beam`, `.md`, `.ts`, and URI schemes like `gs://` or `file://`), and zero markdown links** in subject or body (`.agents/workflows/gitcommit.md`).
5. **Clean peer handoffs**: Post only authored commit SHAs and branch names, and `@mention` only the next peer (`@verifier` or `@integrator`). Tag `@coordinator` only on blockers or final completion.

## 3. Integration, Bundle Build & Phase Completion

1. **`integrator` workflow** (runs after verifiers approve all author branches):
   - If `origin/develop` advanced while authors were working, inspect `git diff <initial-base>..origin/develop -- AGENTS.md .agents/` before rebasing onto `origin/develop` and fold any newly landed rule changes into the affected task commits.
   - Cherry-pick all verifier-approved per-task commits onto the phase branch in exact numerical task order (`TASK1`..`TASKN`).
   - Fold any cross-slice integration fix into its target task commit with `git commit --fixup <task-sha>` and `GIT_SEQUENCE_EDITOR=true git rebase -i --autosquash origin/develop` rather than leaving extra fix commits at the branch tip.
   - Wire shared catalog registries, documentation maps, legacy migrations (`webview/src/utils/legacyNodeMigration.ts`), feature detector sets (`src/codegen/codegenConfig.ts`), unreferenced legacy Beam panel removals, and invariant tests.
   - Audit `git log origin/develop..HEAD` to ensure commit messages contain zero file paths, extensions, or links.
   - Verify **zero regressions** relative to the recorded base baseline across `npm run typecheck:integration`, `npm run lint`, `npm --prefix webview run lint`, `npm run test:unit`, and `npm run test:webview` (`npm run test:all`) without modifying unrelated legacy suites scheduled for later phases.
   - Update `TASKS.md` (`Status` -> `✅ Done`), sync `ARCHITECTURE.md` / `DESIGN.md` if applicable, and create the phase completion commit.
   - Run `npm run build` (`npm run build:webview && npm run compile`) so `out/extension.js` and `webview/dist/` are ready for immediate Extension Development Host (`F5`) testing, and confirm `git status` remains clean.
   - Have a read-only verifier audit the final branch commits, test outputs, and PR description accuracy (such as verifying which panels actually import `useAssetLoader`) before notifying `@coordinator`. If a `workspace: "inherit"` verifier runs a live `vitest.integration.config.mts` smoke test, remove the generated `src/test/integration/test-report.md` afterward so `git status --short` stays clean.
2. **Coordinator wrap-up**:
   - Post `[done] <summary>` (no `@` mentions) so `monitor` goes quiet, then `kill` finished workers (excluding `monitor`) only after all commits are landed on the phase branch.
   - `swarm{action: "kill"}` runs `git worktree remove --force` on each author worktree in the background (1–2 minutes when `node_modules/` is hardlinked). Do not force-remove them by hand, and never wait with `pgrep -f "swarm-..."` (which matches its own shell command line and loops forever). Wait with a bracket pattern (`while pgrep -f "[s]warm-" >/dev/null; do sleep 1; done`), then run `git worktree prune`. Confirm `git worktree list` and `git branch --list 'swarm-*'` contain zero leftover entries.
   - Present the copy-ready `phase -> develop` PR description artifact or ` ```text ` block.
