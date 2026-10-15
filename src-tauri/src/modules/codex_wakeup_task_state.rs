//! Durable scheduler claims and cancellation. Configuration saves and task
//! completion both merge against the current state under the same short lock.
use super::*;
use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::future::Future;

fn try_file_lock(path: &Path) -> Result<Option<fs::File>, String> {
    let parent = path.parent().ok_or("无法定位 Codex 唤醒锁目录")?;
    fs::create_dir_all(parent).map_err(|e| format!("创建 Codex 唤醒锁目录失败: {}", e))?;
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| format!("打开 Codex 唤醒锁失败: {}", e))?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(file)),
        Err(error)
            if error.kind() == io::ErrorKind::WouldBlock
                || error.raw_os_error() == fs2::lock_contended_error().raw_os_error() =>
        {
            Ok(None)
        }
        Err(error) => Err(format!("获取 Codex 唤醒锁失败: {}", error)),
    }
}

pub(super) fn lock_task_state() -> Result<fs::File, String> {
    try_file_lock(&tasks_path()?.with_extension("json.lock"))?
        .ok_or_else(|| "Codex 唤醒任务状态正在更新，请重试".to_string())
}

pub fn try_task_run_lease(task_id: &str) -> Result<Option<fs::File>, String> {
    let filename = format!("{:x}.lock", Sha256::digest(task_id.as_bytes()));
    try_file_lock(&data_dir()?.join("codex_wakeup_run_locks").join(filename))
}

pub(super) fn preserve_run_metadata(next: &mut CodexWakeupState, current: &CodexWakeupState) {
    for task in &mut next.tasks {
        if let Some(saved) = current.tasks.iter().find(|saved| saved.id == task.id) {
            task.last_run_at = saved.last_run_at;
            task.quota_reset_detected_at = if task.account_ids == saved.account_ids
                && task.schedule.kind == saved.schedule.kind
                && task.schedule.quota_reset_window == saved.schedule.quota_reset_window
            {
                saved.quota_reset_detected_at
            } else {
                None
            };
            task.last_status = saved.last_status.clone();
            task.last_message = saved.last_message.clone();
            task.last_success_count = saved.last_success_count;
            task.last_failure_count = saved.last_failure_count;
            task.last_duration_ms = saved.last_duration_ms;
        }
    }
}

// A recovery can happen before the advertised reset deadline (for example,
// after redeeming a reset credit). Observe only successful, saved snapshots.
fn recovered_quota_windows(
    previous: Option<&crate::models::codex::CodexQuota>,
    current: &crate::models::codex::CodexQuota,
    observed_at: i64,
) -> (bool, bool) {
    let Some(previous) = previous else {
        return (false, false);
    };
    // Expired windows already have a scheduled trigger. Do not re-arm them
    // when the post-run refresh finally reports their replenished quota.
    let primary = previous.hourly_reset_time.is_some_and(|reset| reset > observed_at)
        && previous.hourly_window_present != Some(false)
        && current.hourly_window_present != Some(false)
        && previous.hourly_window_minutes == current.hourly_window_minutes
        && (0..=100).contains(&previous.hourly_percentage)
        && (0..=100).contains(&current.hourly_percentage)
        && current.hourly_percentage > previous.hourly_percentage;
    let secondary = previous.weekly_reset_time.is_some_and(|reset| reset > observed_at)
        && previous.weekly_window_present != Some(false)
        && current.weekly_window_present != Some(false)
        && previous.weekly_window_minutes == current.weekly_window_minutes
        && (0..=100).contains(&previous.weekly_percentage)
        && (0..=100).contains(&current.weekly_percentage)
        && current.weekly_percentage > previous.weekly_percentage;
    (primary, secondary)
}

fn record_quota_recovery_in_state(
    state: &mut CodexWakeupState,
    account_id: &str,
    recovered: (bool, bool),
    observed_at: i64,
) -> bool {
    if !state.enabled {
        return false;
    }
    let mut changed = false;
    for task in &mut state.tasks {
        if !task.enabled
            || task.schedule.kind != "quota_reset"
            || !task.account_ids.iter().any(|id| id == account_id)
        {
            continue;
        }
        let matches = match task.schedule.quota_reset_window.as_deref().unwrap_or("either") {
            "primary_window" => recovered.0,
            "secondary_window" => recovered.1,
            "either" => recovered.0 || recovered.1,
            _ => false,
        };
        if matches {
            // Keep a pending event newer than an existing claim, even if both
            // occur within the same Unix second. The scheduler will wait until due.
            task.quota_reset_detected_at = Some(
                observed_at.max(task.last_run_at.unwrap_or(task.created_at).saturating_add(1)),
            );
            changed = true;
        }
    }
    changed
}

pub fn observe_quota_refresh(
    account_id: &str,
    previous: Option<&crate::models::codex::CodexQuota>,
    current: &crate::models::codex::CodexQuota,
) {
    let observed_at = now_ts();
    let recovered = recovered_quota_windows(previous, current, observed_at);
    if !recovered.0 && !recovered.1 {
        return;
    }
    let result = (|| -> Result<bool, String> {
        let _lock = TASKS_LOCK.lock().map_err(|_| "获取 Codex 唤醒任务锁失败")?;
        let _file_lock = lock_task_state()?;
        let mut state = read_state_file()?;
        if !record_quota_recovery_in_state(&mut state, account_id, recovered, observed_at) {
            return Ok(false);
        }
        refresh_next_run_at(&mut state);
        save_json_atomic(&tasks_path()?, &state)?;
        Ok(true)
    })();
    match result {
        Ok(true) => {
            logger::log_info(&format!(
                "[CodexWakeup] 检测到配额提前恢复，触发匹配的唤醒任务: account_id={}, primary={}, secondary={}",
                account_id, recovered.0, recovered.1
            ));
            crate::modules::codex_wakeup_scheduler::notify_quota_reset();
        }
        Ok(false) => {}
        Err(error) => logger::log_warn(&format!("[CodexWakeup] 记录配额恢复事件失败: {}", error)),
    }
}

fn claim_in_state(
    state: &mut CodexWakeupState,
    task_id: &str,
    due_at: Option<i64>,
    require_enabled: bool,
    now: i64,
) -> Option<CodexWakeupTask> {
    if require_enabled && !state.enabled {
        return None;
    }
    let task = state.tasks.iter_mut().find(|task| task.id == task_id)?;
    if require_enabled && !task.enabled {
        return None;
    }
    if due_at.is_some_and(|due| due > now || task.last_run_at.unwrap_or(0) >= due) {
        return None;
    }
    task.last_run_at = Some(now);
    task.last_status = Some(
        if require_enabled && task.execution_mode.as_deref() == Some("confirm") {
            "pending_confirmation"
        } else {
            "running"
        }
        .to_string(),
    );
    task.last_message = None;
    task.last_success_count = None;
    task.last_failure_count = None;
    task.last_duration_ms = None;
    task.updated_at = now;
    Some(task.clone())
}

/// Called only while holding this task's run lease. A durable claim is written
/// before dispatch, so failures and restarts cannot blindly replay a paid call.
pub fn claim_task_run(
    task_id: &str,
    due_at: Option<i64>,
    require_enabled: bool,
) -> Result<Option<CodexWakeupTask>, String> {
    let _lock = TASKS_LOCK.lock().map_err(|_| "获取 Codex 唤醒任务锁失败")?;
    let _file_lock = lock_task_state()?;
    let mut state = read_state_file()?;
    if let Some(expected_due) = due_at {
        let still_due = state
            .tasks
            .iter()
            .find(|task| task.id == task_id)
            .and_then(|task| {
                crate::modules::codex_wakeup_scheduler::current_due_at(task, chrono::Local::now())
            });
        if still_due != Some(expected_due) {
            return Ok(None);
        }
    }
    let Some(task) = claim_in_state(&mut state, task_id, due_at, require_enabled, now_ts()) else {
        return Ok(None);
    };
    refresh_next_run_at(&mut state);
    save_json_atomic(&tasks_path()?, &state)?;
    Ok(Some(task))
}

fn dispatch_settings(task: &CodexWakeupTask) -> serde_json::Value {
    serde_json::json!({"account_ids":task.account_ids,"prompt":task.prompt,"model":task.model,
        "model_display_name":task.model_display_name,"model_reasoning_effort":task.model_reasoning_effort,
        "schedule":task.schedule,"execution_mode":task.execution_mode,"timeout":task.confirm_timeout_minutes})
}

/// Revalidate the exact claimed configuration after approval, before model I/O.
pub fn authorize_task_dispatch(expected: &CodexWakeupTask, automatic: bool) -> Result<(), String> {
    let _lock = TASKS_LOCK.lock().map_err(|_| "CODEX_WAKEUP_LOCK")?;
    let _file_lock = lock_task_state()?;
    let mut state = read_state_file()?;
    authorize_in_state(&mut state, expected, automatic)?;
    save_json_atomic(&tasks_path()?, &state)
}

fn authorize_in_state(
    state: &mut CodexWakeupState,
    expected: &CodexWakeupTask,
    automatic: bool,
) -> Result<(), String> {
    if automatic && !state.enabled {
        return Err(cancelled_error());
    }
    let task = state
        .tasks
        .iter_mut()
        .find(|task| task.id == expected.id)
        .ok_or_else(cancelled_error)?;
    if (automatic && !task.enabled)
        || task.last_run_at != expected.last_run_at
        || task.last_status != expected.last_status
        || dispatch_settings(task) != dispatch_settings(expected)
    {
        return Err(cancelled_error());
    }
    task.last_status = Some("running".into());
    Ok(())
}

fn record_dispatch_failure(task: &mut CodexWakeupTask, error: Option<&str>) {
    task.last_status = Some("error".to_string());
    task.last_message = error.map(str::to_string);
    task.last_success_count = Some(0);
    task.last_failure_count = Some(1);
    task.last_duration_ms = None;
    task.updated_at = now_ts();
    // Keep last_run_at: failure never re-arms the same due event.
}

pub fn mark_task_run_failed(task_id: &str, error: &str) -> Result<(), String> {
    let _lock = TASKS_LOCK.lock().map_err(|_| "获取 Codex 唤醒任务锁失败")?;
    let _file_lock = lock_task_state()?;
    let mut state = read_state_file()?;
    if let Some(task) = state.tasks.iter_mut().find(|task| task.id == task_id) {
        record_dispatch_failure(task, Some(error));
        refresh_next_run_at(&mut state);
        save_json_atomic(&tasks_path()?, &state)?;
    }
    Ok(())
}

pub(super) fn recover_interrupted_runs(state: &mut CodexWakeupState) -> Result<bool, String> {
    recover_interrupted_runs_with(state, try_task_run_lease)
}

fn recover_interrupted_runs_with(
    state: &mut CodexWakeupState,
    mut acquire: impl FnMut(&str) -> Result<Option<fs::File>, String>,
) -> Result<bool, String> {
    let mut changed = false;
    for task in &mut state.tasks {
        if !matches!(
            task.last_status.as_deref(),
            Some("running" | "pending_confirmation")
        ) {
            continue;
        }
        let Some(_lease) = acquire(&task.id)? else {
            continue;
        };
        // The process holding the run lease exited. The upstream may already
        // have accepted the request; expose failure and leave manual retry
        // available instead of automatically spending quota again.
        record_dispatch_failure(task, None);
        logger::log_warn(&format!(
            "[CodexWakeup] 恢复中断任务，保留已认领事件并等待下次计划或手动重试: task_id={}",
            task.id
        ));
        changed = true;
    }
    Ok(changed)
}

pub(super) async fn await_with_cancel<T, F>(
    flag: Option<&Arc<AtomicBool>>,
    future: F,
) -> Result<T, String>
where
    F: Future<Output = Result<T, String>>,
{
    let timed_request = async {
        tokio::time::timeout(Duration::from_secs(180), future)
            .await
            .map_err(|_| "Codex 唤醒请求超时".to_string())?
    };
    let Some(flag) = flag else {
        return timed_request.await;
    };
    tokio::select! {
        biased;
        _ = async {
            loop {
                if flag.load(Ordering::SeqCst) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(CODEX_WAKEUP_CANCEL_POLL_MS)).await;
            }
        } => Err(cancelled_error()),
        result = timed_request => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> CodexWakeupState {
        serde_json::from_value(serde_json::json!({
            "enabled": true,
            "tasks": [{
                "id": "task-1", "name": "Task", "enabled": true,
                "accountIds": ["account-1"], "createdAt": 1, "updatedAt": 1,
                "schedule": { "kind": "interval", "weeklyDays": [], "intervalHours": 1 }
            }]
        }))
        .unwrap()
    }

    fn quota() -> crate::models::codex::CodexQuota {
        serde_json::from_value(serde_json::json!({
            "hourly_percentage": 20, "hourly_reset_time": 1000,
            "hourly_window_present": true, "hourly_window_minutes": 300,
            "weekly_percentage": 30, "weekly_reset_time": 2000,
            "weekly_window_present": true, "weekly_window_minutes": 10080
        }))
        .unwrap()
    }

    #[test]
    fn early_quota_recovery_detects_each_window_without_requiring_a_new_deadline() {
        let before = quota();
        let mut after = before.clone();
        after.hourly_percentage = 100;
        after.hourly_reset_time = None;
        assert_eq!(recovered_quota_windows(Some(&before), &after, 100), (true, false));
        after.weekly_percentage = 90; // Some quota may already be used after a reset.
        assert_eq!(recovered_quota_windows(Some(&before), &after, 100), (true, true));
        assert_eq!(recovered_quota_windows(None, &after, 100), (false, false));
        assert_eq!(recovered_quota_windows(Some(&after), &after, 100), (false, false));
        assert_eq!(recovered_quota_windows(Some(&before), &after, 2000), (false, false));
        assert_eq!(recovered_quota_windows(Some(&before), &before, 100), (false, false));
        after.hourly_percentage = 10;
        after.weekly_percentage = 10;
        assert_eq!(recovered_quota_windows(Some(&before), &after, 100), (false, false));
    }

    #[test]
    fn missing_or_changed_windows_cannot_trigger_early_recovery() {
        let before = quota();
        let mut after = before.clone();
        after.hourly_percentage = 100;
        after.weekly_percentage = 100;
        after.hourly_window_present = Some(false);
        after.weekly_window_minutes = Some(300);
        assert_eq!(recovered_quota_windows(Some(&before), &after, 100), (false, false));
    }

    #[test]
    fn quota_recovery_respects_accounts_windows_and_enable_switches() {
        let mut state = state();
        state.tasks[0].schedule.kind = "quota_reset".into();
        for (window, recovered, expected) in [
            ("primary_window", (true, false), true),
            ("primary_window", (false, true), false),
            ("secondary_window", (true, false), false),
            ("secondary_window", (false, true), true),
            ("either", (true, true), true),
        ] {
            state.tasks[0].schedule.quota_reset_window = Some(window.into());
            state.tasks[0].quota_reset_detected_at = None;
            assert_eq!(record_quota_recovery_in_state(&mut state, "account-1", recovered, 100), expected);
        }
        assert!(!record_quota_recovery_in_state(&mut state, "other", (true, true), 200));
        state.enabled = false;
        assert!(!record_quota_recovery_in_state(&mut state, "account-1", (true, true), 200));
        state.enabled = true;
        state.tasks[0].enabled = false;
        assert!(!record_quota_recovery_in_state(&mut state, "account-1", (true, true), 200));
        state.tasks[0].enabled = true;
        state.tasks[0].schedule.kind = "interval".into();
        assert!(!record_quota_recovery_in_state(&mut state, "account-1", (true, true), 200));
    }

    #[test]
    fn quota_recovery_survives_restart_and_stale_ui_save_and_is_claimed_once() {
        let mut saved = state();
        saved.tasks[0].schedule.kind = "quota_reset".into();
        // No selected account exists on disk: only the recovery event can be due.
        let mut stale_ui = saved.clone();
        assert!(record_quota_recovery_in_state(&mut saved, "account-1", (true, true), 100));
        let mut restored: CodexWakeupState =
            serde_json::from_str(&serde_json::to_string(&saved).unwrap()).unwrap();
        preserve_run_metadata(&mut stale_ui, &restored);
        assert_eq!(stale_ui.tasks[0].quota_reset_detected_at, Some(100));
        let now = chrono::TimeZone::timestamp_opt(&chrono::Local, 120, 0).unwrap();
        assert_eq!(crate::modules::codex_wakeup_scheduler::current_due_at(&restored.tasks[0], now), Some(100));
        assert!(claim_in_state(&mut restored, "task-1", Some(100), true, 120).is_some());
        assert_eq!(crate::modules::codex_wakeup_scheduler::current_due_at(&restored.tasks[0], now), None);
        assert!(claim_in_state(&mut restored, "task-1", Some(100), true, 120).is_none());
        // A real subsequent reset within the claim's second is not lost.
        assert!(record_quota_recovery_in_state(&mut restored, "account-1", (true, false), 120));
        assert_eq!(restored.tasks[0].quota_reset_detected_at, Some(121));
        assert!(claim_in_state(&mut restored, "task-1", Some(121), true, 120).is_none());
        assert!(claim_in_state(&mut restored, "task-1", Some(121), true, 121).is_some());
        stale_ui.tasks[0].schedule.quota_reset_window = Some("secondary_window".into());
        preserve_run_metadata(&mut stale_ui, &restored);
        assert_eq!(stale_ui.tasks[0].quota_reset_detected_at, None);
    }

    #[test]
    fn stability_confirm_claim_waits_before_dispatch_and_survives_restart() {
        let mut state = state();
        state.tasks[0].execution_mode = Some("confirm".into());
        let task = claim_in_state(&mut state, "task-1", Some(100), true, 120).unwrap();
        assert_eq!(task.last_status.as_deref(), Some("pending_confirmation"));
        let mut restored: CodexWakeupState =
            serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
        assert!(claim_in_state(&mut restored, "task-1", Some(100), true, 180).is_none());
        let dir =
            std::env::temp_dir().join(format!("confirmation-restart-{}", uuid::Uuid::new_v4()));
        assert!(
            recover_interrupted_runs_with(&mut restored, |_| try_file_lock(&dir.join("lease")))
                .unwrap()
        );
        assert_eq!(restored.tasks[0].last_status.as_deref(), Some("error"));
        assert!(claim_in_state(&mut restored, "task-1", Some(100), true, 180).is_none());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn changed_disabled_deleted_or_replayed_confirmation_cannot_authorize_dispatch() {
        let mut saved = state();
        saved.tasks[0].execution_mode = Some("confirm".into());
        let claimed = claim_in_state(&mut saved, "task-1", Some(100), true, 120).unwrap();
        for change in [
            "prompt",
            "account",
            "mode",
            "disabled-task",
            "disabled-global",
            "deleted",
            "reclaimed",
        ] {
            let mut edited = saved.clone();
            match change {
                "prompt" => edited.tasks[0].prompt = Some("different".into()),
                "account" => edited.tasks[0].account_ids = vec!["different".into()],
                "mode" => edited.tasks[0].execution_mode = Some("auto".into()),
                "disabled-task" => edited.tasks[0].enabled = false,
                "disabled-global" => edited.enabled = false,
                "deleted" => edited.tasks.clear(),
                _ => edited.tasks[0].last_run_at = Some(180),
            }
            assert!(
                authorize_in_state(&mut edited, &claimed, true).is_err(),
                "{change}"
            );
        }
        authorize_in_state(&mut saved, &claimed, true).unwrap();
        assert_eq!(saved.tasks[0].last_status.as_deref(), Some("running"));
        assert!(authorize_in_state(&mut saved, &claimed, true).is_err());
    }

    #[test]
    fn durable_claim_prevents_replay_after_serialization_and_failure() {
        let mut state = state();
        assert!(claim_in_state(&mut state, "task-1", Some(100), true, 120).is_some());
        let encoded = serde_json::to_string(&state).unwrap();
        let mut restored: CodexWakeupState = serde_json::from_str(&encoded).unwrap();
        record_dispatch_failure(&mut restored.tasks[0], Some("network failed"));
        assert!(claim_in_state(&mut restored, "task-1", Some(100), true, 180).is_none());
        assert_eq!(restored.tasks[0].last_run_at, Some(120));
        assert!(claim_in_state(&mut restored, "task-1", Some(200), true, 220).is_some());
    }

    #[test]
    fn claims_recheck_disabled_state_but_allow_explicit_manual_retry() {
        let mut state = state();
        state.enabled = false;
        assert!(claim_in_state(&mut state, "task-1", None, true, 100).is_none());
        assert!(claim_in_state(&mut state, "task-1", None, false, 100).is_some());
        state.enabled = true;
        state.tasks[0].enabled = false;
        assert!(claim_in_state(&mut state, "task-1", Some(200), true, 220).is_none());
    }

    #[test]
    fn stale_ui_save_cannot_undo_claim_or_discard_failure() {
        let mut saved = state();
        let mut stale_ui = saved.clone();
        claim_in_state(&mut saved, "task-1", Some(100), true, 120).unwrap();
        record_dispatch_failure(&mut saved.tasks[0], Some("cancelled"));
        stale_ui.enabled = false;
        stale_ui.tasks[0].name = "Edited name".to_string();
        preserve_run_metadata(&mut stale_ui, &saved);
        assert!(!stale_ui.enabled);
        assert_eq!(stale_ui.tasks[0].name, "Edited name");
        assert_eq!(stale_ui.tasks[0].last_run_at, Some(120));
        assert_eq!(stale_ui.tasks[0].last_status.as_deref(), Some("error"));
    }

    #[test]
    fn cancellation_before_scope_resolution_is_retained_until_release() {
        let id = uuid::Uuid::new_v4().to_string();
        cancel_wakeup_scope(&id).unwrap();
        let flag = resolve_cancel_flag(Some(&id)).unwrap().unwrap();
        assert!(is_scope_cancelled(Some(&flag)));
        release_wakeup_scope(&id).unwrap();
        let fresh = resolve_cancel_flag(Some(&id)).unwrap().unwrap();
        assert!(!is_scope_cancelled(Some(&fresh)));
        release_wakeup_scope(&id).unwrap();
    }

    #[test]
    fn task_file_lock_is_nonblocking_and_recovers_after_owner_drops() {
        let dir = std::env::temp_dir().join(format!("wakeup-lock-{}", uuid::Uuid::new_v4()));
        let path = dir.join("task.lock");
        let lease = try_file_lock(&path).unwrap().unwrap();
        assert!(try_file_lock(&path).unwrap().is_none());
        drop(lease);
        drop(try_file_lock(&path).unwrap().unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn interrupted_run_recovers_without_replaying_and_leaves_active_run_intact() {
        let mut state = state();
        claim_in_state(&mut state, "task-1", Some(100), true, 120).unwrap();
        assert!(!recover_interrupted_runs_with(&mut state, |_| Ok(None)).unwrap());
        assert_eq!(state.tasks[0].last_status.as_deref(), Some("running"));
        let dir = std::env::temp_dir().join(format!("wakeup-recover-{}", uuid::Uuid::new_v4()));
        let path = dir.join("task.lock");
        assert!(recover_interrupted_runs_with(&mut state, |_| try_file_lock(&path)).unwrap());
        assert_eq!(state.tasks[0].last_status.as_deref(), Some("error"));
        assert_eq!(state.tasks[0].last_run_at, Some(120));
        assert!(claim_in_state(&mut state, "task-1", Some(100), true, 180).is_none());
        assert!(claim_in_state(&mut state, "task-1", None, false, 180).is_some());
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn cancelled_scope_never_polls_the_request() {
        let flag = Arc::new(AtomicBool::new(true));
        let result = await_with_cancel(Some(&flag), async {
            panic!("cancelled request must not start");
            #[allow(unreachable_code)]
            Ok::<(), String>(())
        })
        .await;
        assert_eq!(result, Err(cancelled_error()));
    }

    #[tokio::test]
    async fn cancellation_drops_an_inflight_request_future() {
        struct DropSignal(Arc<AtomicBool>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let cancel = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let pending_signal = DropSignal(dropped.clone());
        let request = async move {
            let _signal = pending_signal;
            std::future::pending::<Result<(), String>>().await
        };
        let cancel_for_timer = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cancel_for_timer.store(true, Ordering::SeqCst);
        });
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            await_with_cancel(Some(&cancel), request),
        )
        .await
        .unwrap();
        assert_eq!(result, Err(cancelled_error()));
        assert!(dropped.load(Ordering::SeqCst));
    }
}
