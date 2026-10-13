//! Missing-row discovery for a complete SQLite-backed catalog generation.

use std::path::Path;

use codex_state::StateRuntime;

use crate::ThreadListLayout;
use crate::list::collect_discovery_paths;
use crate::list::read_discovery_item;
use crate::metadata::extract_metadata_from_rollout;
use crate::state_db::normalize_cwd_for_state_db;

const INDEX_BATCH_SIZE: usize = 64;

/// Index eligible, unindexed rollouts before issuing a SQLite recency cursor.
///
/// Existing rows remain authoritative, including a reverted thread's selected rollout and
/// paginated metadata. The manifest retains only paths/IDs; previews and histories are processed
/// at most two at a time. Scan and database failures never certify a completed generation.
pub async fn discover_thread_metadata(
    state: &StateRuntime,
    root: &Path,
    layout: ThreadListLayout,
    default_provider: &str,
) -> anyhow::Result<()> {
    let paths = collect_discovery_paths(root, layout).await?;
    for batch in paths.chunks(INDEX_BATCH_SIZE) {
        let ids = batch.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let existing = state.get_threads(&ids).await?;
        let missing = batch
            .iter()
            .filter(|(id, _)| !existing.contains_key(id))
            .collect::<Vec<_>>();
        for pair in missing.chunks(2) {
            let first = index_candidate(state, &pair[0].1, layout, default_provider);
            if let Some(second) = pair.get(1) {
                let next = index_candidate(state, &second.1, layout, default_provider);
                if pair[0].0 == second.0 {
                    // Reverted siblings share an ID: preserve newest-first selection.
                    first.await?;
                    next.await?;
                } else {
                    tokio::try_join!(first, next)?;
                }
            } else {
                first.await?;
            }
        }
    }
    Ok(())
}

async fn index_candidate(
    state: &StateRuntime,
    path: &Path,
    layout: ThreadListLayout,
    default_provider: &str,
) -> anyhow::Result<()> {
    // Preserve the native active/archive eligibility rules, including empty archives.
    if read_discovery_item(path.to_path_buf(), layout)
        .await?
        .is_none()
    {
        tracing::warn!(
            "skipping ineligible rollout during catalog discovery: {}",
            path.display()
        );
        return Ok(());
    }
    let outcome = extract_metadata_from_rollout(path, default_provider).await?;
    let mut metadata = outcome.metadata;
    metadata.cwd = normalize_cwd_for_state_db(&metadata.cwd);
    if matches!(layout, ThreadListLayout::Flat) {
        metadata.archived_at = Some(metadata.updated_at);
    }
    // A native writer may have created the row while its rollout was being read.
    if !state
        .insert_thread_if_absent_with_memory_mode(&metadata, outcome.memory_mode.as_deref())
        .await?
    {
        // get_threads omits invalid records; an existing but unreadable row is not
        // evidence that this candidate was indexed successfully.
        state
            .get_thread(metadata.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("discovered thread was not indexed: {}", metadata.id))?;
    }
    Ok(())
}
