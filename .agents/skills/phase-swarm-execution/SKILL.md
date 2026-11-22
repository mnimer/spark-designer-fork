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

1. **Check free disk space & fast-forward the phase worktree to `origin/develop` before spawning workers**: Report free disk space (`df -h .`) in your pre-flight summary before creating worker worktrees. Each `swarm{action: "add", workspace: "share"}` worker branches from the coordinator's current `HEAD`. Follow `.agents/rules/git-workflow.md` (squash-merged `develop` base rule) and `.agents/rules/toolchain-and-verification.md` (offline Airlock `node_modules` hardlink bootstrap) before adding workers:
   ```bash
   df -h .
   git merge --ff-only origin/develop
   ```
2. **Record the base verification baseline before you spawn workers**: Run the verification gate from `.agents/rules/toolchain-and-verification.md` on the base commit inside a detached temporary worktree. Record any failing test names and lint counts in every brief so authors and verifiers judge zero regressions against one shared baseline. If base tests fail, assign ownership of each failure up front.
3. Read `TASKS.md`, `AGENTS.md`, `DESIGN.md`, and target interfaces before briefing workers. Load `.agents/skills/spark-connector-authoring/SKILL.md` for connector or step catalog phases.
4. Group tasks into 3–6 author slices (`workspace: "share"`) with **100% disjoint file ownership**, plus:
   - `integrator` (`workspace: "inherit"` on the phase worktree branch)
   - `verifier` (or split `verifier-a` / `verifier-b` on `workspace: "inherit"`, strictly read-only)
   - `monitor` (when `swarm_monitor` is in `<skills>`)
   Verify every parallel author `swarm{action: "add"}` call explicitly sets `workspace: "share"`. Never leave an author on default `inherit`.
   Load `swarm_pane` and write `<appDataDir>/brain/<conversation-id>/.system_generated/swarm/plan.json` right after spawning workers (do not add a `narrator` unless requested). Route peer handoffs directly (`author -> verifier [Stage 1] -> integrator -> verifier [Stage 2] -> coordinator`) so `@coordinator` never becomes a bottleneck.
5. **Reserve shared barrel and registry files for `integrator`, and co-locate shared-skill edits**: In catalog or extension phases (`SRC*`, `SNK*`, `OP*`, `EXT*`, `SDP*`), authors own only isolated `resources/codegen-skills/*` and `webview/src/properties/*` files. Keep `webview/src/catalog/nodes/*.ts`, `webview/src/catalog/propertyFormRegistry.ts`, `webview/src/catalog/documentation/*.ts`, `webview/src/catalog/paletteGroupState.ts`, `webview/src/utils/legacyNodeMigration.ts`, `src/shared/*`, `src/codegen/codegenConfig.ts`, `src/spark/versionMatrix.ts`, `webview/src/components/PipelineNode.tsx`, `webview/src/hooks/usePipelineGraph.ts`, `webview/src/graph/nodeFactory.ts`, count-invariant tests, and `TASKS.md` exclusively in `integrator`. If two tasks modify the same `resources/codegen-skills/<name>/SKILL.md`, assign both tasks to one author in numerical commit order. Lock exact exported React component identifiers and `properties.*` keys across author and `integrator` briefs up front.
6. Leave `model` unset on `swarm{action: "add"}` unless the user requests another model. If a `waiting` worker hits `503 No capacity available` or quota exhaustion, switch its model in place via `swarm{action: "model", name, model}` and inform the user. Never `kill` and re-`add` a stalled worker because `kill` deletes its worktree and context.
7. **Assign full vertical IPC call chains**: If a task sends state or actions between the Webview and Extension Host, assign the complete call path (`Leaf Component -> Context -> App.tsx -> useVsCodeBridge.ts -> src/shared/protocol.ts -> src/ipc/handlers/*.ts`) to one owner so options are never dropped at middle layers.
8. **Allow stacked author branches for signature dependencies**: If one author requires another author's new function signature, the dependent author may branch from that author's commit. Each author must post the full commit chain (`A1 -> A2 -> B1`), and `integrator` cherry-picks in dependency order while folding side-branch fixups into their target task commit.

## 2. Author Worktree Brief Requirements

Include these 5 rules in every `workspace: "share"` author brief:

1. **Verify isolated worktree & bootstrap `node_modules`**: Confirm `pwd` is a `swarm-*` worktree (if `pwd` is the shared phase worktree, stop and alert `@coordinator`), never run `git worktree add` or `git stash`, and hardlink `node_modules` per `.agents/rules/toolchain-and-verification.md`.
2. **Search & graph queries**: Follow `.agents/rules/graphify.md` and `.agents/rules/toolchain-and-verification.md` (`git grep` and `git ls-files` only).
3. **Enforce disjoint file boundaries & workspace conventions**: Touch only assigned files and place customizations under `.agents/` (`AGENTS.md`). Follow `.agents/rules/shared-module-purity.md`, `.agents/rules/webview-ui-design-system.md`, and `.agents/rules/documentation-style.md` across edited modules.
4. **Verify & commit cleanly**: Run targeted host/webview Vitest, typecheck, and ESLint checks per `.agents/rules/toolchain-and-verification.md`. Commit as `<type>(<scope>): <summary> (<TASK_ID>)` following `.agents/rules/git-workflow.md` and `.agents/workflows/gitcommit.md`.
5. **Clean peer handoffs**: Post only authored commit SHAs and branch names, and `@mention` only the next peer (`@verifier` or `@integrator`). Tag `@coordinator` only on blockers or final completion.

## 3. Integration, Bundle Build & Phase Completion

1. **`integrator` workflow** (runs after verifiers approve all author branches):
   - If Stage 1 review is split across multiple verifiers (`verifier-a`, `verifier-b`), call `read_board{since: 0}` on every wakeup and immediately call `swarm_wait` without touching files until every Stage 1 verifier posts approval.
   - If `origin/develop` advanced while authors worked, inspect `git diff <initial-base>..origin/develop -- AGENTS.md .agents/` before rebasing onto `origin/develop` and fold newly landed rule updates into affected commits.
   - Cherry-pick all verifier-approved commits onto the phase branch in strict numerical task order (`TASK1`..`TASKN`).
   - Wire each task's shared catalog registries, capability flags, `versionMatrix` entries, DAG handle rules, and count-invariant tests directly into that task's commit (`git commit --amend` or `git commit --fixup` + autosquash rebase) so every commit is atomic.
   - Audit `git log origin/develop..HEAD` against the commit-message hygiene rules in `.agents/rules/git-workflow.md`:
     ```bash
     git log --format="%s%n%b" origin/develop..HEAD | grep -En '://|\.(spark|beam|md|ts|tsx|py|sh|json|yml|yaml)\b|\[.*\]\(.*\)'
     ```
   - Run the full verification gate in `.agents/rules/toolchain-and-verification.md` (`compile`, `typecheck:webview`, `typecheck:integration`, `lint`, `webview lint`, `build:webview`, `test:all`) and confirm zero regressions against the recorded baseline without editing unrelated legacy suites.
   - Refresh `graphify update .` per `.agents/rules/graphify.md`, update `TASKS.md` (`Status` -> `✅ Done`), sync `ARCHITECTURE.md` / `DESIGN.md` per `.agents/rules/documentation-style.md`, and create the phase completion commit.
   - Run `npm run build` so `out/extension.js` and `webview/dist/` are ready for Extension Development Host (`F5`) testing, and confirm `git status --short` is empty.
   - Hand off to the Stage 2 read-only verifier to audit branch commits, test counts, and PR description accuracy before `@coordinator` finishes. If a read-only check writes `src/test/integration/test-report.md`, delete it so `git status --short` stays empty.
2. **Coordinator wrap-up**:
   - Post `[done] <summary>` (no `@` mentions) so `monitor` goes quiet, then `kill` finished workers (excluding `monitor`) only after all commits land on the phase branch.
   - `swarm{action: "kill"}` removes each author worktree asynchronously (1–2 minutes when `node_modules/` is hardlinked). Wait with a bracket pattern so `pgrep` never matches its own shell command, then prune worktrees:
     ```bash
     while pgrep -f "[s]warm-" >/dev/null
     do
       sleep 1
     done
     git worktree prune
     ```
   - Confirm `git worktree list` and `git branch --list 'swarm-*'` show zero leftover worker entries, then present the copy-ready `phase -> develop` PR description artifact or ` ```text ` block.
   - **Post-merge worktree & branch cleanup**: Right after a phase PR squash-merges into `develop`, verify zero active processes use the merged phase worktree, remove that phase worktree (`git worktree remove <path>`), run `git worktree prune`, and delete the merged local phase branch plus any remaining `swarm-*` branches (`git branch -D <branch>`) to reclaim disk space immediately.
