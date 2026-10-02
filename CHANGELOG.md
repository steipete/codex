The changelog can be found on the [releases page](https://github.com/openai/codex/releases).

## Unreleased

- Fix intermittent Linux sandbox startup failures (`bwrap: Can't get type of source`) when concurrent workspace-write commands share a writable root that lacks protected metadata paths such as `.aws` or `.git` ([steipete/codex#1](https://github.com/steipete/codex/pull/1)).
- Fix missing command lifecycle events when unified exec cannot create a process, preserving approval rejection and sandbox retry behavior. Thanks @Marvinthebored ([#44557](https://github.com/openai/codex/issues/44557)).
