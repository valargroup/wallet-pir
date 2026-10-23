//! The Slack webhook from a scoped systemd credential.
//!
//! A rendered unit loads the webhook with `LoadCredential=slack-webhook:...`
//! and names it in `<PREFIX>_SLACK_WEBHOOK_FILE`; a relative name resolves
//! against `$CREDENTIALS_DIRECTORY`. The value is never printed: errors name
//! only what is wrong with it.
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// Every accepted webhook starts with this origin, as `credential-exec.py` requires.
pub const SLACK_ORIGIN: &str = "https://hooks.slack.com/";

/// The webhook from `file` (a credential reference) or, without one, `url`.
///
/// Setting both is refused: a deployment chooses one delivery path.
pub fn slack_webhook(
    url: Option<&str>,
    file: Option<&str>,
    credentials: Option<&Path>,
) -> Result<Option<String>> {
    let url = url.map(str::trim).filter(|v| !v.is_empty());
    let Some(file) = file.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(url.map(String::from));
    };
    anyhow::ensure!(
        url.is_none(),
        "set either the Slack webhook URL or its credential file, not both"
    );
    let path = Path::new(file);
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        credentials
            .map(Path::to_path_buf)
            .context("a relative Slack credential needs $CREDENTIALS_DIRECTORY")?
            .join(path)
    };
    let value = std::fs::read_to_string(&path)
        .with_context(|| format!("reading the Slack credential {}", path.display()))?;
    let value = value.trim();
    anyhow::ensure!(
        value.starts_with(SLACK_ORIGIN) && !value.chars().any(char::is_whitespace),
        "the Slack credential is not a Slack webhook URL"
    );
    Ok(Some(value.to_string()))
}

/// `$CREDENTIALS_DIRECTORY`, set by systemd for a unit with credentials.
pub fn directory() -> Option<PathBuf> {
    std::env::var_os("CREDENTIALS_DIRECTORY").map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_a_relative_credential_and_refuses_ambiguity_and_other_values() {
        let dir = tempfile::tempdir().unwrap();
        let hook = "https://hooks.slack.com/services/T0/B0/rehearsal";
        std::fs::write(dir.path().join("slack-webhook"), format!("{hook}\n")).unwrap();
        let read = |url, file| slack_webhook(url, file, Some(dir.path()));
        assert_eq!(read(None, Some("slack-webhook")).unwrap().as_deref(), Some(hook));
        assert_eq!(read(Some(hook), None).unwrap().as_deref(), Some(hook));
        assert_eq!(read(Some("  "), Some(" ")).unwrap(), None);
        assert!(read(Some(hook), Some("slack-webhook")).is_err());
        assert!(read(None, Some("missing")).is_err());
        assert!(slack_webhook(None, Some("slack-webhook"), None).is_err());
        std::fs::write(dir.path().join("other"), "https://example.com/hook").unwrap();
        let error = read(None, Some("other")).unwrap_err().to_string();
        assert!(!error.contains("example.com"), "the value is never echoed");
    }
}
