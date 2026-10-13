use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::time::Duration;
use std::time::UNIX_EPOCH;

use chrono::DateTime;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::StateDbHandle;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use uuid::Uuid;

use super::super::LocalThreadStore;
use super::super::test_support::test_config;
use super::super::test_support::write_archived_session_file;
use super::super::test_support::write_session_file;
use super::super::test_support::write_session_file_with_history_mode;
use crate::ListThreadsParams;
use crate::SortDirection;
use crate::ThreadSortKey;
use crate::ThreadStore;

async fn store(home: &Path) -> (LocalThreadStore, StateDbHandle) {
    let config = test_config(home);
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize empty index");
    runtime
        .mark_backfill_complete(/*last_watermark*/ None)
        .await
        .expect("complete initial backfill before importing files");
    (
        LocalThreadStore::new(config, Some(runtime.clone())),
        runtime,
    )
}

fn params() -> ListThreadsParams {
    ListThreadsParams {
        page_size: 64,
        cursor: None,
        sort_key: ThreadSortKey::RecencyAt,
        sort_direction: SortDirection::Desc,
        allowed_sources: vec![SessionSource::Cli],
        model_providers: Some(vec!["test-provider".to_string()]),
        cwd_filters: None,
        section: None,
        project_id: None,
        archived: false,
        search_term: None,
        relation_filter: None,
        use_state_db_only: false,
    }
}

fn id(number: u128) -> ThreadId {
    ThreadId::from_string(&Uuid::from_u128(number).to_string()).expect("thread id")
}

fn modified_at(path: &Path, seconds: u64) {
    fs::File::options()
        .write(true)
        .open(path)
        .expect("open fixture")
        .set_times(fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(seconds)))
        .expect("set deterministic file time");
}

fn session_meta_field(path: &Path, field: &str, value: serde_json::Value) {
    let contents = fs::read_to_string(path).expect("read fixture");
    let (header, remaining) = contents.split_once('\n').expect("session header");
    let mut header: serde_json::Value = serde_json::from_str(header).expect("session metadata");
    header["payload"][field] = value;
    fs::write(path, format!("{header}\n{remaining}")).expect("update fixture metadata");
}

#[tokio::test]
async fn overlapping_first_pages_share_one_discovery_generation() {
    let home = TempDir::new().expect("temporary home");
    let (store, _) = store(home.path()).await;
    for number in 1..=4 {
        write_session_file(home.path(), "2025-01-03T12-00-00", Uuid::from_u128(number))
            .expect("imported rollout");
    }
    let callers = (0..10).map(|_| {
        let client = store.clone();
        async move {
            client
                .list_threads(params())
                .await
                .expect("shared first page")
        }
    });
    for page in futures::future::join_all(callers).await {
        assert_eq!(page.items.len(), 4);
    }
    assert_eq!(
        store.listing_discovery[0]
            .completed
            .load(std::sync::atomic::Ordering::Acquire),
        1,
        "overlapping callers must complete just one discovery pass"
    );
}

#[tokio::test]
async fn recency_pages_include_all_imports_when_existing_recency_differs_from_mtime() {
    let home = TempDir::new().expect("temporary home");
    let (store, runtime) = store(home.path()).await;
    let mut expected = HashSet::new();
    for number in 1..=256 {
        let path = write_session_file(home.path(), "2025-01-03T12-00-00", Uuid::from_u128(number))
            .expect("unindexed rollout");
        modified_at(&path, 1_700_000_000 + number as u64);
        expected.insert(id(number));
    }
    for number in 257..=258 {
        let path = write_session_file(home.path(), "2025-01-03T12-00-00", Uuid::from_u128(number))
            .expect("indexed rollout");
        modified_at(&path, 1_900_000_000 + number as u64);
        let mut metadata = codex_state::ThreadMetadataBuilder::new(
            id(number),
            path,
            DateTime::from_timestamp(946_684_800, 0).expect("timestamp"),
            SessionSource::Cli,
        )
        .build("test-provider");
        metadata.first_user_message = Some("Existing preview".to_string());
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("seed older product recency");
        expected.insert(id(number));
    }
    let mut request = params();
    request.page_size = 1;
    let mut ids = Vec::new();
    loop {
        let page = store
            .list_threads(request.clone())
            .await
            .expect("recency page");
        ids.extend(page.items.into_iter().map(|thread| thread.thread_id));
        let Some(cursor) = page.next_cursor else {
            break;
        };
        assert!(ids.len() <= expected.len(), "pagination repeated threads");
        request.cursor = Some(cursor);
        request.page_size = 64;
    }
    assert_eq!(ids.len(), expected.len());
    assert_eq!(ids.into_iter().collect::<HashSet<_>>(), expected);
}

#[tokio::test]
async fn fresh_listing_discovers_imports_and_keeps_archive_and_memory_preferences() {
    let home = TempDir::new().expect("temporary home");
    let (store, runtime) = store(home.path()).await;
    assert!(
        store
            .list_threads(params())
            .await
            .expect("empty list")
            .items
            .is_empty()
    );
    let path = write_session_file(home.path(), "2025-01-03T12-00-00", Uuid::from_u128(1))
        .expect("active import");
    session_meta_field(&path, "memory_mode", serde_json::json!("disabled"));
    let archived_path =
        write_archived_session_file(home.path(), "2025-01-03T13-00-00", Uuid::from_u128(2))
            .expect("archived import");
    let archived_contents = fs::read_to_string(&archived_path).expect("archive contents");
    let archived_header = archived_contents.lines().next().expect("archive metadata");
    fs::write(&archived_path, format!("{archived_header}\n")).expect("empty archived thread");
    let active = store
        .list_threads(params())
        .await
        .expect("refresh active imports");
    let mut archived_request = params();
    archived_request.archived = true;
    let archived = store
        .list_threads(archived_request)
        .await
        .expect("refresh archive imports");
    assert_eq!(
        active
            .items
            .iter()
            .map(|thread| thread.thread_id)
            .collect::<Vec<_>>(),
        vec![id(1)]
    );
    assert_eq!(
        archived
            .items
            .iter()
            .map(|thread| thread.thread_id)
            .collect::<Vec<_>>(),
        vec![id(2)]
    );
    assert_eq!(
        runtime
            .get_thread_memory_mode(id(1))
            .await
            .expect("memory mode")
            .as_deref(),
        Some("disabled")
    );
}

#[tokio::test]
async fn resumed_cursor_discovers_missing_imports_in_a_new_store() {
    let home = TempDir::new().expect("temporary home");
    let (store, runtime) = store(home.path()).await;
    for number in 1..=2 {
        let path = write_session_file(home.path(), "2025-01-03T12-00-00", Uuid::from_u128(number))
            .expect("initial rollout");
        modified_at(&path, 1_700_000_000 + number as u64);
    }
    let mut request = params();
    request.page_size = 1;
    let first = store
        .list_threads(request.clone())
        .await
        .expect("first page");
    request.cursor = Some(first.next_cursor.expect("continuation"));
    request.page_size = 64;
    let path = write_session_file(home.path(), "2025-01-03T11-00-00", Uuid::from_u128(3))
        .expect("import after original listing");
    modified_at(&path, 946_684_800);
    let restarted = LocalThreadStore::new(test_config(home.path()), Some(runtime));
    let remainder = restarted
        .list_threads(request)
        .await
        .expect("resume after restart");
    let mut ids = first
        .items
        .into_iter()
        .map(|thread| thread.thread_id)
        .collect::<Vec<_>>();
    ids.extend(remainder.items.into_iter().map(|thread| thread.thread_id));
    assert_eq!(
        ids.into_iter().collect::<HashSet<_>>(),
        HashSet::from([id(1), id(2), id(3)])
    );
}

#[tokio::test]
async fn continuations_discover_later_imports_only_when_the_listing_is_refreshed() {
    let home = TempDir::new().expect("temporary home");
    let (store, _) = store(home.path()).await;
    for number in 1..=2 {
        let path = write_session_file(home.path(), "2025-01-03T12-00-00", Uuid::from_u128(number))
            .expect("initial rollout");
        modified_at(&path, 1_700_000_000 + number as u64);
    }
    let mut request = params();
    request.page_size = 1;
    let first = store
        .list_threads(request.clone())
        .await
        .expect("first page");
    assert_eq!(first.items[0].thread_id, id(2));
    request.cursor = Some(first.next_cursor.expect("continuation"));
    request.page_size = 64;
    let path = write_session_file(home.path(), "2025-01-03T11-00-00", Uuid::from_u128(3))
        .expect("import between pages");
    modified_at(&path, 946_684_800);
    let remainder = store
        .list_threads(request)
        .await
        .expect("continue indexed generation");
    assert_eq!(
        remainder
            .items
            .into_iter()
            .map(|thread| thread.thread_id)
            .collect::<Vec<_>>(),
        vec![id(1)]
    );
    let refreshed = store
        .list_threads(params())
        .await
        .expect("refresh imported session");
    assert_eq!(
        refreshed
            .items
            .into_iter()
            .map(|thread| thread.thread_id)
            .collect::<HashSet<_>>(),
        HashSet::from([id(1), id(2), id(3)])
    );
}

#[tokio::test]
async fn discovery_selects_the_newest_unindexed_archived_revert() {
    let home = TempDir::new().expect("temporary home");
    let (store, runtime) = store(home.path()).await;
    let original = write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T12-00-00",
        Uuid::from_u128(1),
        ThreadHistoryMode::Paginated,
    )
    .expect("original rollout");
    let archive = home.path().join("archived_sessions");
    fs::create_dir_all(&archive).expect("archive directory");
    let archived_original = archive.join(original.file_name().expect("rollout basename"));
    fs::rename(original, &archived_original).expect("archive original");
    let selected = archive.join(format!(
        "rollout-2025-01-03T13-00-00-{}_{}.jsonl",
        id(1),
        Uuid::from_u128(2)
    ));
    fs::copy(archived_original, &selected).expect("imported revert");
    session_meta_field(&selected, "memory_mode", serde_json::json!("disabled"));
    let mut request = params();
    request.archived = true;
    let page = store.list_threads(request).await.expect("discover revert");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].rollout_path.as_ref(), Some(&selected));
    assert_eq!(
        runtime
            .get_thread_memory_mode(id(1))
            .await
            .expect("memory preference")
            .as_deref(),
        Some("disabled")
    );
}

#[tokio::test]
async fn discovery_preserves_the_selected_paginated_rollout_and_sqlite_metadata() {
    let home = TempDir::new().expect("temporary home");
    let (store, runtime) = store(home.path()).await;
    let original = write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T12-00-00",
        Uuid::from_u128(1),
        ThreadHistoryMode::Paginated,
    )
    .expect("original paginated rollout");
    let selected = original.with_file_name(format!(
        "rollout-2025-01-03T13-00-00-{}_{}.jsonl",
        id(1),
        Uuid::from_u128(2)
    ));
    fs::copy(&original, &selected).expect("reverted rollout with stable thread id");
    let mut builder = codex_state::ThreadMetadataBuilder::new(
        id(1),
        selected.clone(),
        DateTime::from_timestamp(946_684_800, 0).expect("timestamp"),
        SessionSource::Cli,
    );
    builder.history_mode = ThreadHistoryMode::Paginated;
    let mut metadata = builder.build("test-provider");
    metadata.name = Some("Selected name".to_string());
    metadata.model = Some("selected-model".to_string());
    metadata.reasoning_effort = Some(ReasoningEffort::High);
    metadata.cwd = home.path().join("selected-workspace");
    runtime
        .upsert_thread(&metadata)
        .await
        .expect("seed selected rollout");
    let before = runtime
        .get_thread(id(1))
        .await
        .expect("read selected metadata");
    let page = store
        .list_threads(params())
        .await
        .expect("discover sibling rollouts");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].rollout_path.as_ref(), Some(&selected));
    assert_eq!(
        runtime
            .get_thread(id(1))
            .await
            .expect("metadata after discovery"),
        before
    );
}

#[tokio::test]
async fn failed_enumeration_can_be_retried_after_the_root_is_repaired() {
    let home = TempDir::new().expect("temporary home");
    let (store, _) = store(home.path()).await;
    fs::write(home.path().join("sessions"), "not a directory").expect("invalid sessions root");
    assert!(store.list_threads(params()).await.is_err());
    fs::remove_file(home.path().join("sessions")).expect("repair sessions root");
    write_session_file(home.path(), "2025-01-03T12-00-00", Uuid::from_u128(1))
        .expect("valid import");
    let page = store.list_threads(params()).await.expect("retry discovery");
    assert_eq!(
        page.items
            .into_iter()
            .map(|thread| thread.thread_id)
            .collect::<Vec<_>>(),
        vec![id(1)]
    );
}
