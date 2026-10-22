use super::*;
use pir_pipeline::service::Tasks;
use std::time::Duration;
use tokio::sync::oneshot;

fn management_service() -> (JoinHandle<std::io::Result<()>>, Tasks) {
    let task = tokio::spawn(std::future::pending());
    let mut guard = Tasks::default();
    guard.own(&task);
    (task, guard)
}
async fn wait_for_phase(status: &PublisherStatus, product: PublisherProduct, phase: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if status.snapshot()[&product].phase.name() == phase {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_success_and_failure_stop_only_the_finished_owner() {
    for outcome in [Ok(()), Err("expected failure".into())] {
        let status = PublisherStatus::default();
        let mut supervisor = ProductSupervisor::new(status.clone());
        let first = Shutdown::default();
        let second = Shutdown::default();
        supervisor.start("enhance", vec![], async { outcome }, first.clone());
        let held = second.clone();
        supervisor.start(
            "status",
            vec![],
            async move {
                held.cancelled().await;
                Ok(())
            },
            second.clone(),
        );
        let shutdown = Shutdown::default();
        let requested = shutdown.clone();
        let (mut management, _guard) = management_service();
        let task =
            tokio::spawn(async move { supervisor.supervise(&mut management, &requested).await });
        wait_for_phase(&status, "enhance", "failed").await;
        assert!(first.requested());
        assert!(!second.requested());
        assert_eq!(status.snapshot()[&"status"].phase.name(), "running");
        assert!(!task.is_finished());
        shutdown.request();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(second.requested());
    }
}

#[tokio::test]
async fn shutdown_requests_all_owners_and_waits_for_their_operations() {
    let status = PublisherStatus::default();
    let mut supervisor = ProductSupervisor::new(status.clone());
    let (release, held) = oneshot::channel();
    let (first_stopped, first_observed) = oneshot::channel();
    let (second_stopped, second_observed) = oneshot::channel();
    let first = Shutdown::default();
    let second = Shutdown::default();
    let first_owner = first.clone();
    supervisor.start(
        "enhance",
        vec![],
        async move {
            first_owner.cancelled().await;
            first_stopped.send(()).unwrap();
            held.await.unwrap();
            Ok(())
        },
        first,
    );
    let second_owner = second.clone();
    supervisor.start(
        "status",
        vec![],
        async move {
            second_owner.cancelled().await;
            second_stopped.send(()).unwrap();
            Ok(())
        },
        second,
    );
    let shutdown = Shutdown::default();
    shutdown.request();
    let (mut management, _guard) = management_service();
    let task = tokio::spawn(async move { supervisor.supervise(&mut management, &shutdown).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        first_observed.await.unwrap();
        second_observed.await.unwrap();
    })
    .await
    .unwrap();
    assert!(
        !task.is_finished(),
        "host must wait for the unfinished product operation"
    );
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for product in ["enhance", "status"] {
        assert_eq!(status.snapshot()[&product].phase.name(), "stopped");
    }
}

#[tokio::test]
async fn panic_and_management_exit_stop_the_host_after_other_owners_finish() {
    for panic_product in [true, false] {
        let mut supervisor = ProductSupervisor::new(PublisherStatus::default());
        if panic_product {
            supervisor.start(
                "enhance",
                vec![],
                async { panic!("expected product panic") },
                Shutdown::default(),
            );
        }
        let other = Shutdown::default();
        let owner = other.clone();
        let (stopped, observed) = oneshot::channel();
        let (release, held) = oneshot::channel();
        supervisor.start(
            "status",
            vec![],
            async move {
                owner.cancelled().await;
                stopped.send(()).unwrap();
                held.await.unwrap();
                Ok(())
            },
            other.clone(),
        );
        let shutdown = Shutdown::default();
        let requested = shutdown.clone();
        let (mut management, _guard) = management_service();
        if !panic_product {
            management.abort();
        }
        let task =
            tokio::spawn(async move { supervisor.supervise(&mut management, &requested).await });
        tokio::time::timeout(Duration::from_secs(2), observed)
            .await
            .unwrap()
            .unwrap();
        assert!(other.requested());
        assert!(!task.is_finished());
        release.send(()).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains(if panic_product {
            "panicked"
        } else {
            "management"
        }));
        assert!(shutdown.requested());
    }
}
