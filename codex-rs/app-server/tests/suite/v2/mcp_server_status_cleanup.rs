use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use anyhow::bail;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::ListMcpServerStatusParams;
use codex_app_server_protocol::ListMcpServerStatusResponse;
use codex_app_server_protocol::McpServerStatusDetail;
use codex_app_server_protocol::RequestId;
use codex_core::config::ConfigBuilder;
use codex_core::plugins_manager_for_config;
use codex_exec_server::EnvironmentManager;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_mcp::CodexAppsToolsCache;
use codex_mcp::McpRuntimeContext;
use codex_mcp::McpSnapshotDetail;
use codex_mcp::McpToolCatalogCache;
use codex_mcp::collect_mcp_server_status_snapshot_with_detail;
use core_test_support::stdio_server_bin;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;

const SERVER_COUNT: usize = 4;
const TEST_TIMEOUT: Duration = Duration::from_secs(40);

struct ProcessRecord {
    leader: u32,
    descendant: u32,
    process_group: u32,
}

#[derive(Clone, Copy)]
enum InventoryBlock {
    NoBlock,
    Initialize,
    ResourceList,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_and_concurrent_status_lists_drain_resistant_stdio_process_groups() -> Result<()> {
    let responses_server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    let records_root = codex_home.path().join("mcp-processes");
    write_resistant_mcp_config(
        codex_home.path(),
        &responses_server.uri(),
        &records_root,
        InventoryBlock::NoBlock,
    )?;

    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized_with_timeout(TEST_TIMEOUT)
        .await?;

    for completed_requests in 1..=2 {
        let response: ListMcpServerStatusResponse = app_server
            .request(
                |request_id| codex_app_server_protocol::ClientRequest::McpServerStatusList {
                    request_id,
                    params: status_params(/*cursor*/ None),
                },
            )
            .await?;
        assert_eq!(response.data.len(), SERVER_COUNT);
        let records =
            wait_for_process_records(&records_root, completed_requests * SERVER_COUNT).await?;
        assert_process_groups_drained(&records)?;
    }

    let mut request_ids = Vec::new();
    for _ in 0..4 {
        request_ids.push(
            app_server
                .send_list_mcp_server_status_request(status_params(/*cursor*/ None))
                .await?,
        );
    }
    for request_id in request_ids {
        let response: ListMcpServerStatusResponse =
            timeout(TEST_TIMEOUT, app_server.read_response(request_id)).await??;
        assert_eq!(response.data.len(), SERVER_COUNT);
    }
    let expected_records = 6 * SERVER_COUNT;
    let records = wait_for_process_records(&records_root, expected_records).await?;
    assert_process_groups_drained(&records)?;

    let error_request_id = app_server
        .send_list_mcp_server_status_request(status_params(Some("not-a-cursor".to_string())))
        .await?;
    let error: JSONRPCError = timeout(
        TEST_TIMEOUT,
        app_server.read_stream_until_error_message(RequestId::Integer(error_request_id)),
    )
    .await??;
    assert!(error.error.message.contains("invalid cursor"));
    let records = wait_for_process_records(&records_root, expected_records + SERVER_COUNT).await?;
    assert_process_groups_drained(&records)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_during_startup_drains_resistant_stdio_process_groups() -> Result<()> {
    assert_cancelling_snapshot_drains(InventoryBlock::Initialize).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_during_inventory_drains_resistant_stdio_process_groups() -> Result<()> {
    assert_cancelling_snapshot_drains(InventoryBlock::ResourceList).await
}

async fn assert_cancelling_snapshot_drains(block: InventoryBlock) -> Result<()> {
    let responses_server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    let records_root = codex_home.path().join("mcp-processes");
    write_resistant_mcp_config(
        codex_home.path(),
        &responses_server.uri(),
        &records_root,
        block,
    )?;

    let config = ConfigBuilder::default()
        .codex_home(codex_home.path().to_path_buf())
        .build()
        .await?;
    let auth_manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("test"));
    let plugins_manager = plugins_manager_for_config(&config, auth_manager);
    let mcp_config = config.to_mcp_config(&plugins_manager).await;
    let runtime_context = McpRuntimeContext::new(
        Arc::new(EnvironmentManager::default_for_tests()),
        config.cwd.to_path_buf(),
    );

    let snapshot_task = tokio::spawn(async move {
        collect_mcp_server_status_snapshot_with_detail(
            &mcp_config,
            /*auth*/ None,
            "cancelled-status-snapshot".to_string(),
            runtime_context,
            CodexAppsToolsCache::default(),
            McpToolCatalogCache::default(),
            McpSnapshotDetail::Full,
        )
        .await
    });

    let records = wait_for_process_records(&records_root, SERVER_COUNT).await?;
    if matches!(block, InventoryBlock::ResourceList) {
        timeout(TEST_TIMEOUT, async {
            while !(0..SERVER_COUNT).all(|server_index| {
                records_root
                    .join(format!("server-{server_index}/resource-list-started"))
                    .is_file()
            }) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
    }
    snapshot_task.abort();
    let cancellation = snapshot_task
        .await
        .expect_err("snapshot task should be cancelled");
    assert!(cancellation.is_cancelled());
    wait_for_process_groups_drained(&records).await
}

fn status_params(cursor: Option<String>) -> ListMcpServerStatusParams {
    ListMcpServerStatusParams {
        cursor,
        limit: None,
        detail: Some(McpServerStatusDetail::Full),
        thread_id: None,
    }
}

fn write_resistant_mcp_config(
    codex_home: &Path,
    responses_server_uri: &str,
    records_root: &Path,
    block: InventoryBlock,
) -> Result<()> {
    let server_bin = stdio_server_bin()?;
    let mut mcp_config = String::new();
    for server_index in 0..SERVER_COUNT {
        let record_dir = records_root.join(format!("server-{server_index}"));
        let started_file = record_dir.join("resource-list-started");
        let resource_barrier_file = record_dir.join("release-resource-list");
        let initialize_barrier_file = record_dir.join("release-initialize");
        mcp_config.push_str(&format!(
            r#"
[mcp_servers.resistant-{server_index}]
command = {}
startup_timeout_sec = 10

[mcp_servers.resistant-{server_index}.env]
MCP_TEST_RESISTANT_DESCENDANT_RECORD_DIR = {}
"#,
            toml::Value::String(server_bin.clone()),
            toml::Value::String(record_dir.to_string_lossy().into_owned()),
        ));
        match block {
            InventoryBlock::NoBlock => {}
            InventoryBlock::Initialize => mcp_config.push_str(&format!(
                "MCP_TEST_INITIALIZE_BARRIER_FILE = {}\n",
                toml::Value::String(initialize_barrier_file.to_string_lossy().into_owned()),
            )),
            InventoryBlock::ResourceList => mcp_config.push_str(&format!(
                "MCP_TEST_RESOURCE_LIST_STARTED_FILE = {}\nMCP_TEST_RESOURCE_LIST_BARRIER_FILE = {}\n",
                toml::Value::String(started_file.to_string_lossy().into_owned()),
                toml::Value::String(resource_barrier_file.to_string_lossy().into_owned()),
            )),
        }
    }
    MockResponsesConfig::new(responses_server_uri)
        .with_extra_config(&mcp_config)
        .write(codex_home)?;
    Ok(())
}

async fn wait_for_process_records(root: &Path, expected: usize) -> Result<Vec<ProcessRecord>> {
    timeout(TEST_TIMEOUT, async {
        loop {
            let records = process_records(root)?;
            if records.len() == expected {
                return Ok(records);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}

fn process_records(root: &Path) -> Result<Vec<ProcessRecord>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut record_files = Vec::new();
    for server_dir in std::fs::read_dir(root)? {
        let server_dir = server_dir?.path();
        for entry in std::fs::read_dir(server_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.bytes().all(|byte| byte.is_ascii_digit()))
            {
                record_files.push(path);
            }
        }
    }
    record_files.sort();
    record_files
        .into_iter()
        .map(|path| {
            let contents = std::fs::read_to_string(&path)?;
            let mut fields = contents.split_whitespace();
            let invalid_record =
                || anyhow::anyhow!("invalid process record in {}: {contents:?}", path.display());
            Ok(ProcessRecord {
                leader: fields.next().ok_or_else(&invalid_record)?.parse()?,
                descendant: fields.next().ok_or_else(&invalid_record)?.parse()?,
                process_group: fields.next().ok_or_else(invalid_record)?.parse()?,
            })
        })
        .collect()
}

fn assert_process_groups_drained(records: &[ProcessRecord]) -> Result<()> {
    assert_eq!(live_tracked_processes(records)?, Vec::<String>::new());
    Ok(())
}

async fn wait_for_process_groups_drained(records: &[ProcessRecord]) -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        loop {
            let live = live_tracked_processes(records)?;
            if live.is_empty() {
                return Ok(());
            }
            if live.iter().any(|process| process.contains("state=Z")) {
                bail!("cancelled status snapshot left zombie MCP processes: {live:?}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}

fn live_tracked_processes(records: &[ProcessRecord]) -> Result<Vec<String>> {
    let process_ids = records
        .iter()
        .flat_map(|record| [record.leader, record.descendant])
        .collect::<BTreeSet<_>>();
    let process_groups = records
        .iter()
        .map(|record| record.process_group)
        .collect::<BTreeSet<_>>();
    let output = Command::new("/bin/ps")
        .args(["-axo", "pid=,pgid=,stat="])
        .output()
        .map_err(|error| anyhow::anyhow!("failed to read POSIX process table: {error}"))?;
    if !output.status.success() {
        bail!("ps exited with {}", output.status);
    }
    Ok(String::from_utf8(output.stdout)?
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let process_id = fields.next()?.parse::<u32>().ok()?;
            let process_group = fields.next()?.parse::<u32>().ok()?;
            (process_ids.contains(&process_id) || process_groups.contains(&process_group))
                .then(|| line.trim().to_string())
        })
        .collect())
}
