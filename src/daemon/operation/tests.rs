use super::*;
use std::sync::Mutex as StdMutex;

fn coordinator(complete: bool) -> Arc<OperationCoordinator> {
    OperationCoordinator::new(crate::daemon::tests::test_backend(
        complete,
        false,
        Arc::new(StdMutex::new(Vec::new())),
    ))
}

async fn start(c: &Arc<OperationCoordinator>, key: &str, owner: &str) -> Value {
    c.start_owned(
        json!({"key": key, "operation": "connect"}),
        Some(owner.into()),
    )
    .await
}

#[tokio::test]
async fn status_is_atomic_owned_and_identical_to_each_published_revision() {
    let c = coordinator(false);
    let mut events = c.subscribe();
    let receipt = start(&c, "device", "owner").await;
    let queued = &receipt["data"]["operation"];
    assert_eq!(queued["revision"], 1);
    let id = queued["request_id"].as_str().unwrap();
    let params = json!({"request_id": id});
    let mut previous = 1;
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(event.revision > previous);
        previous = event.revision;
        assert!(event.visible_to(Some("owner")));
        assert!(!event.visible_to(Some("foreign")));
    }
    let running = c.status(&params, Some("owner")).await;
    assert_eq!(running["data"]["operation_status"]["revision"], previous);
    assert_eq!(running["data"]["operation_status"]["stage"], "connecting");
    assert!(running["data"]["operation_status"].get("owner").is_none());
    for owner in [None, Some("foreign")] {
        assert_eq!(
            c.status(&params, owner).await["error"]["code"],
            "request-not-found"
        );
        assert_eq!(c.snapshot(owner).await, json!({"active": [], "recent": []}));
        assert!(!c.cancel_owned(id, owner).await);
    }
    assert!(c.cancel_owned(id, Some("owner")).await);
    let terminal = events.recv().await.unwrap();
    assert_eq!(terminal.revision, previous + 1);
    assert_eq!(terminal.state, "cancelled");
    assert_eq!(
        c.status(&params, Some("owner")).await["data"]["operation_status"],
        json!(terminal)
    );
    assert_eq!(c.snapshot(Some("owner")).await["recent"], json!([terminal]));
    assert!(!c.cancel_owned(id, Some("owner")).await);
    assert_eq!(
        c.status(&params, Some("foreign")).await["error"]["code"],
        "request-not-found"
    );
    assert_eq!(
        c.status(&json!({}), Some("owner")).await["error"]["code"],
        "validation-error"
    );
    assert_eq!(
        c.status(&json!({"request_id":"missing"}), Some("owner"))
            .await["error"]["code"],
        "request-not-found"
    );
}

#[tokio::test]
async fn abandoned_cancel_waiter_retains_admission_until_the_worker_and_terminal_are_reconciled() {
    let c = coordinator(false);
    let receipt = start(&c, "device", "owner").await;
    let id = receipt["data"]["operation"]["request_id"].as_str().unwrap();
    {
        let cancellation = c.cancel_owned(id, Some("owner"));
        tokio::pin!(cancellation);
        assert!(futures::poll!(&mut cancellation).is_pending());
        // Drop the caller at the first suspension after cancellation was accepted.
    }
    assert_eq!(
        c.status(&json!({"request_id":id}), Some("owner")).await["ok"],
        true
    );
    assert_eq!(
        start(&c, "device", "peer").await["error"]["code"],
        "device-busy"
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let status = c.status(&json!({"request_id":id}), Some("owner")).await;
            assert_eq!(
                status["ok"], true,
                "losing the Cancel waiter cannot lose the accepted operation"
            );
            if status["data"]["operation_status"]["state"] == "cancelled" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(start(&c, "device", "peer").await["ok"], true);
    c.cancel_owner("peer").await;
}

#[tokio::test]
async fn quiet_terminal_loss_and_completion_status_race_never_report_a_missing_active_id() {
    let c = coordinator(true);
    let mut ids = std::collections::HashSet::new();
    for _ in 0..20 {
        let receipt = start(&c, "device", "owner").await;
        let id = receipt["data"]["operation"]["request_id"].as_str().unwrap();
        assert!(ids.insert(id.to_owned()));
        // Intentionally no subscription: completion must be recoverable in silence.
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let status = c.status(&json!({"request_id": id}), Some("owner")).await;
                assert_eq!(
                    status["ok"], true,
                    "active -> terminal has no missing-result window"
                );
                if status["data"]["operation_status"]["state"] == "completed" {
                    assert!(
                        status["data"]["operation_status"]["revision"]
                            .as_u64()
                            .unwrap()
                            >= 3
                    );
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn terminals_are_bounded_expire_and_do_not_release_other_active_work() {
    let c = coordinator(false);
    // Zero retention for a deterministic expiry witness (RecentResults uses Instant,
    // not Tokio's paused clock). Production policy is asserted independently below.
    c.state.lock().await.recent = RecentResults::new(1, Some(Duration::ZERO));
    let a = start(&c, "a", "owner").await;
    let b = start(&c, "b", "peer").await;
    let a_id = a["data"]["operation"]["request_id"].as_str().unwrap();
    let b_id = b["data"]["operation"]["request_id"].as_str().unwrap();
    c.cancel_owner("owner").await;
    assert_eq!(
        c.status(&json!({"request_id": a_id}), Some("owner")).await["error"]["code"],
        "request-not-found"
    );
    assert_eq!(
        c.status(&json!({"request_id": b_id}), Some("peer")).await["ok"],
        true
    );
    assert_eq!(
        c.snapshot(Some("peer")).await["active"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    c.state.lock().await.recent = RecentResults::new(1, Some(TERMINAL_RETENTION));
    assert!(c.cancel_owned(b_id, Some("peer")).await);
    let d = start(&c, "d", "owner").await;
    let d_id = d["data"]["operation"]["request_id"].as_str().unwrap();
    assert!(c.cancel_owned(d_id, Some("owner")).await);
    assert_eq!(
        c.status(&json!({"request_id": b_id}), Some("peer")).await["error"]["code"],
        "request-not-found"
    );
    assert_eq!(
        c.status(&json!({"request_id": d_id}), Some("owner")).await["ok"],
        true
    );
    assert_eq!(TERMINAL_LIMIT, 64);
    assert_eq!(TERMINAL_RETENTION, Duration::from_secs(300));
}

#[tokio::test]
async fn last_exact_progress_revision_reserves_a_terminal_and_stale_callbacks_cannot_publish() {
    let c = coordinator(false);
    let mut events = c.subscribe();
    let receipt = start(&c, "device", "owner").await;
    let id = receipt["data"]["operation"]["request_id"].as_str().unwrap();
    // Wait until backend reached its pending effect, retaining admission.
    for _ in 0..2 {
        events.recv().await.unwrap();
    }
    let event = {
        let mut state = c.state.lock().await;
        let task = state.active.get_mut(id).unwrap();
        task.event.revision = MAX_REVISION - 2;
        task.event.clone()
    };
    let progress = c.progress_reporter(&event);
    progress("waiting");
    assert_eq!(events.recv().await.unwrap().revision, MAX_REVISION - 1);
    progress("ignored");
    assert!(events.try_recv().is_err());
    assert!(c.cancel_owned(id, Some("owner")).await);
    assert_eq!(events.recv().await.unwrap().revision, MAX_REVISION);
    progress("late");
    assert!(events.try_recv().is_err());
    assert_eq!(start(&c, "device", "peer").await["ok"], true);
    c.cancel_owner("peer").await;
}
