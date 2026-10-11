The changelog can be found on the [releases page](https://github.com/openai/codex/releases).

## Unreleased

- Exclude ephemeral thread diagnostics from the SQLite log database and feedback logs, including background model, tool, and app-server event processing.
- Fix missing command lifecycle events when unified exec cannot create a process, preserving approval rejection and sandbox retry behavior. Thanks @Marvinthebored ([#44557](https://github.com/openai/codex/issues/44557)).
