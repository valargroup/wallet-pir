//! Configured local command hooks.
//!
//! A hook is a fixed argument vector run under the sidecar account in its own
//! process group, with an empty environment apart from a fixed `PATH`. It
//! prints `{"checks":[{"name","status","reason","values"}]}` on stdout. A
//! timeout kills the whole process group; any execution, size or parse failure
//! reports `hook:<name>` as unknown with a fixed reason, never raw output.
use crate::config::Hook;
use pir_host_observations::{Check, Status, Value};
use serde::Deserialize;
use std::{process::Stdio, time::Duration};
use tokio::io::AsyncReadExt;

pub const MAX_OUTPUT: usize = 64 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Output {
    checks: Vec<Reported>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reported {
    name: String,
    status: Status,
    reason: String,
    #[serde(default)]
    values: Vec<Value>,
}

fn unknown(hook: &Hook, reason: &str, now: u64) -> Vec<Check> {
    vec![Check {
        name: format!("hook:{}", hook.name),
        status: Status::Unknown,
        reason: reason.into(),
        observed_at: now,
        values: vec![],
    }]
}

fn kill_group(pid: Option<u32>) {
    if let Some(pid) = pid {
        // SAFETY: signalling a process group this sidecar created.
        unsafe {
            libc::killpg(pid as libc::pid_t, libc::SIGKILL);
        }
    }
}

pub async fn run(hook: &Hook, now: u64) -> Vec<Check> {
    let mut command = tokio::process::Command::new(&hook.argv[0]);
    command
        .args(&hook.argv[1..])
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .kill_on_drop(true);
    let Ok(mut child) = command.spawn() else {
        return unknown(hook, "hook could not be started", now);
    };
    let pid = child.id();
    let mut stdout = child.stdout.take().expect("piped stdout");
    let work = async {
        let mut buffer = Vec::new();
        (&mut stdout)
            .take(MAX_OUTPUT as u64 + 1)
            .read_to_end(&mut buffer)
            .await?;
        if buffer.len() > MAX_OUTPUT {
            return Ok::<_, std::io::Error>((buffer, None));
        }
        let status = child.wait().await?;
        Ok((buffer, Some(status)))
    };
    let result = tokio::time::timeout(Duration::from_secs(hook.timeout_seconds), work).await;
    // Descendants that outlived or detached from the hook are terminated too.
    kill_group(pid);
    let (buffer, status) = match result {
        Err(_) => return unknown(hook, "hook timed out", now),
        Ok(Err(_)) => return unknown(hook, "hook output unreadable", now),
        Ok(Ok((_, None))) => return unknown(hook, "hook output exceeds 64 KiB", now),
        Ok(Ok((buffer, Some(status)))) => (buffer, status),
    };
    if !status.success() {
        return unknown(hook, "hook exited unsuccessfully", now);
    }
    let Ok(output) = serde_json::from_slice::<Output>(&buffer) else {
        return unknown(hook, "hook output is not a valid check document", now);
    };
    let mut checks = vec![Check {
        name: format!("hook:{}", hook.name),
        status: Status::Healthy,
        reason: format!("{} checks reported", output.checks.len()),
        observed_at: now,
        values: vec![],
    }];
    for r in output.checks.into_iter().take(32) {
        checks.push(Check {
            name: format!("hook:{}:{}", hook.name, r.name),
            status: r.status,
            reason: r.reason,
            observed_at: now,
            values: r.values,
        });
    }
    // Validate as the APM will; an invalid entry makes the whole hook unknown.
    let probe = pir_host_observations::Observations {
        schema_version: pir_host_observations::SCHEMA_VERSION,
        host_id: "probe".into(),
        incarnation: "probe".into(),
        config_digest: format!("sha256:{}", "0".repeat(64)),
        generated_at: now,
        checks,
    };
    if probe.validate().is_err() {
        return unknown(hook, "hook reported invalid checks", now);
    }
    probe.checks
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn hook(script: &str, timeout: u64) -> Hook {
        Hook {
            name: "probe".into(),
            argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
            timeout_seconds: timeout,
        }
    }

    #[tokio::test]
    async fn structured_output_becomes_named_checks_without_inherited_secrets() {
        std::env::set_var("PIR_TEST_SECRET", "do-not-leak");
        let checks = run(
            &hook(
                r#"test -z "$PIR_TEST_SECRET" || exit 3; echo '{"checks":[{"name":"queue","status":"warning","reason":"deep","values":[{"name":"depth","value":9,"unit":"count"}]}]}'"#,
                5,
            ),
            9,
        )
        .await;
        assert_eq!(checks.len(), 2);
        assert_eq!(checks[0].status, Status::Healthy);
        assert_eq!(checks[1].name, "hook:probe:queue");
        assert_eq!(checks[1].status, Status::Warning);
        assert_eq!(checks[1].observed_at, 9);
    }

    #[tokio::test]
    async fn failures_are_unknown_with_fixed_reasons() {
        for (script, reason) in [
            (
                "echo secret-token >&2; exit 1",
                "hook exited unsuccessfully",
            ),
            (
                "echo not json secret-token",
                "hook output is not a valid check document",
            ),
            (
                r#"echo '{"checks":[{"name":"Bad Name","status":"healthy","reason":""}]}'"#,
                "hook reported invalid checks",
            ),
            ("head -c 70000 /dev/zero", "hook output exceeds 64 KiB"),
        ] {
            let checks = run(&hook(script, 5), 1).await;
            assert_eq!(checks.len(), 1, "{script}");
            assert_eq!(checks[0].status, Status::Unknown);
            assert_eq!(checks[0].reason, reason);
        }
        let missing = Hook {
            name: "missing".into(),
            argv: vec!["/nonexistent/hook".into()],
            timeout_seconds: 1,
        };
        assert_eq!(
            run(&missing, 1).await[0].reason,
            "hook could not be started"
        );
    }

    #[tokio::test]
    async fn timeout_kills_the_whole_process_group() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("survivor");
        let script = format!("(sleep 3; touch {}) & sleep 30", marker.display());
        let started = Instant::now();
        let checks = run(&hook(&script, 1), 1).await;
        assert!(started.elapsed() < Duration::from_secs(3));
        assert_eq!(checks[0].reason, "hook timed out");
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert!(!marker.exists(), "background child survived the timeout");
    }
}
