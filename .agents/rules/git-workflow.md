---
trigger: always_on
---

# Git Workflow

- **Git Commits**: Never commit unless explicitly requested by the user. When asked to commit, follow `.agents/workflows/gitcommit.md`: format `<type>(<scope>): <subject>`, include a detailed body, and omit all file paths, file extensions, and markdown links from both subject and body. Wrap commit messages shown in chat inside a single ` ```text ` block.
- **Remote Git Operations**: HTTPS `origin` has no non-interactive credential helper in agent shells. Never run `git fetch` or `git push` against remote. Leave pushing and PR creation to the user.
- **Squash-Merged `develop` Base Rule**: Phase PRs merge into `origin/develop` via GitHub Squash and Merge. Never stack or fast-forward a new phase worktree onto un-squashed commits from a sibling feature branch. If local `develop` lacks a merged prerequisite phase, ask the user to pull `develop` and rebase the worktree first. If a branch was stacked onto an un-squashed branch `<old-tip>`, recover after fetching via `git rebase --onto origin/develop <old-tip> <branch>`.
