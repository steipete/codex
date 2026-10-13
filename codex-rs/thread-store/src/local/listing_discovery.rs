//! One complete discovery generation shared by first-page callers and continuations.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use codex_rollout::Cursor;
use codex_rollout::RolloutConfig;
use codex_rollout::ThreadListLayout;
use codex_rollout::discover_thread_metadata;
use codex_state::StateRuntime;
use tokio::sync::Mutex;

use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[derive(Default)]
pub(super) struct ListingDiscovery {
    completed: AtomicUsize,
    refresh: Mutex<()>,
}

impl ListingDiscovery {
    pub(super) async fn ensure(
        &self,
        state: &StateRuntime,
        config: &RolloutConfig,
        layout: ThreadListLayout,
        cursor: Option<&Cursor>,
    ) -> ThreadStoreResult<()> {
        let observed = self.completed.load(Ordering::Acquire);
        if cursor.is_some() && observed != 0 {
            return Ok(());
        }
        let _refresh = self.refresh.lock().await;
        if self.completed.load(Ordering::Acquire) != observed {
            return Ok(());
        }
        let root = config.codex_home.join(match layout {
            ThreadListLayout::NestedByDate => codex_rollout::SESSIONS_SUBDIR,
            ThreadListLayout::Flat => codex_rollout::ARCHIVED_SESSIONS_SUBDIR,
        });
        discover_thread_metadata(state, &root, layout, &config.model_provider_id)
            .await
            .map_err(|error| ThreadStoreError::Internal {
                message: format!("catalog discovery failed; retry the listing: {error}"),
            })?;
        // Only successful repair permits DB pagination. A fresh first page is the refresh
        // boundary for external imports; concurrent first pages share this completed pass.
        self.completed.fetch_add(1, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
#[path = "listing_discovery_tests.rs"]
mod tests;
