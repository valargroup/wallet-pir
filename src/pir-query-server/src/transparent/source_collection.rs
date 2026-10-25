//! Collection of exclusively owned HTTPS publication trees and their shared sources.
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

/// Collect unused publications and then revisions no remaining publication
/// references, including private HTTPS staging trees. Staging uses the same
/// file lock while linking catalog sources. Defer
/// rather than blocking control commands behind a slow transfer.
pub(crate) fn collect_publications(root: &Path, keep: &BTreeSet<PathBuf>) -> std::io::Result<bool> {
    use std::fs::{self, OpenOptions, TryLockError};
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(".sources.lock"))?;
    match lock.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Ok(false),
        Err(TryLockError::Error(error)) => return Err(error),
    }
    let digest = |name: &std::ffi::OsStr| {
        name.to_str().is_some_and(|s| {
            s.len() == 64
                && s.bytes()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        })
    };
    // A recovered record may name its tree through another spelling of the
    // same directory; compare resolved paths so it is never mistaken as unused.
    let keep: BTreeSet<PathBuf> = keep
        .iter()
        .flat_map(|path| std::iter::once(path.clone()).chain(fs::canonicalize(path).ok()))
        .collect();
    let kept = |path: &Path| {
        keep.contains(path) || fs::canonicalize(path).is_ok_and(|path| keep.contains(&path))
    };
    let mut publications = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let staged = name
            .to_str()
            .and_then(|name| name.strip_prefix("staging-"))
            .and_then(|name| name.split_once('-'))
            .is_some_and(|(revision, time)| {
                revision.parse::<u64>().is_ok() && time.parse::<u128>().is_ok()
            });
        if (digest(&name) || staged) && entry.file_type()?.is_dir() {
            publications.push(entry);
        }
    }
    publications.sort_by_key(|e| std::cmp::Reverse(e.metadata().and_then(|m| m.modified()).ok()));
    let mut unused = 0;
    let mut removed_publications = 0;
    let mut removed_revisions = 0;
    let mut referenced = BTreeSet::new();
    for publication in publications {
        if !kept(&publication.path()) {
            unused += 1;
            if unused > 3 {
                fs::remove_dir_all(publication.path())?;
                removed_publications += 1;
                continue;
            }
        }
        for revision in fs::read_dir(publication.path())? {
            let revision = revision?;
            // A malformed or partially staged link cannot make a referenced
            // revision disappear. Loading it still requires full validation.
            if digest(&revision.file_name()) {
                referenced.insert(revision.file_name());
            }
        }
    }
    let catalog = root.join(".sources");
    if fs::symlink_metadata(&catalog).is_ok_and(|m| m.is_dir()) {
        for entry in fs::read_dir(catalog)? {
            let entry = entry?;
            if digest(&entry.file_name())
                && entry.file_type()?.is_dir()
                && !referenced.contains(&entry.file_name())
                && !kept(&entry.path())
            {
                fs::remove_dir_all(entry.path())?;
                removed_revisions += 1;
            }
        }
    }
    if removed_publications > 0 || removed_revisions > 0 {
        tracing::info!(
            removed_publications,
            removed_revisions,
            "unused publication sources collected"
        );
    }
    Ok(true)
}

#[cfg(test)]
mod collection_tests {
    use super::*;
    #[test]
    fn collection_keeps_https_staging_sources() {
        let root = tempfile::tempdir().unwrap();
        let catalog = root.path().join(".sources");
        std::fs::create_dir(&catalog).unwrap();
        let mut keep = BTreeSet::new();
        // Active, candidate and retained publications all use HTTPS names.
        for n in 0..8 {
            let revision = format!("{n:064x}");
            std::fs::create_dir(catalog.join(&revision)).unwrap();
            std::fs::write(catalog.join(&revision).join("rows.bin"), b"checked rows").unwrap();
            let publication = root.path().join(format!("staging-{n}-100"));
            std::fs::create_dir(&publication).unwrap();
            std::os::unix::fs::symlink(
                format!("../.sources/{revision}"),
                publication.join(&revision),
            )
            .unwrap();
            if n < 3 {
                keep.insert(publication);
            }
        }
        let orphan = catalog.join("f".repeat(64));
        std::fs::create_dir(&orphan).unwrap();
        collect_publications(root.path(), &keep).unwrap();
        for (n, publication) in keep.iter().enumerate() {
            assert_eq!(
                std::fs::read(publication.join(format!("{n:064x}")).join("rows.bin")).unwrap(),
                b"checked rows"
            );
        }
        assert!(!orphan.exists());
        assert_eq!(
            std::fs::read_dir(root.path())
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().starts_with("staging-"))
                .count(),
            keep.len() + 3
        );
    }

    #[test]
    fn collection_keeps_a_tree_named_through_another_path() {
        let real = tempfile::tempdir().unwrap();
        let alias = tempfile::tempdir().unwrap();
        let spelled = alias.path().join("provider");
        std::os::unix::fs::symlink(real.path(), &spelled).unwrap();
        let epoch = std::time::SystemTime::UNIX_EPOCH;
        for n in 0..5u64 {
            let publication = real.path().join(format!("staging-{n}-100"));
            std::fs::create_dir(&publication).unwrap();
            std::fs::File::open(&publication)
                .unwrap()
                .set_modified(epoch + std::time::Duration::from_secs(1_000 + n))
                .unwrap();
        }
        // The oldest tree would be the first unused one removed.
        let keep = BTreeSet::from([spelled.join("staging-0-100")]);
        assert!(collect_publications(real.path(), &keep).unwrap());
        assert!(real.path().join("staging-0-100").is_dir());
        assert!(!real.path().join("staging-1-100").exists());
        for n in 2..5 {
            assert!(real.path().join(format!("staging-{n}-100")).is_dir());
        }
    }

    #[test]
    fn collection_keeps_referenced_sources_and_defers_during_staging() {
        let root = tempfile::tempdir().unwrap();
        let catalog = root.path().join(".sources");
        std::fs::create_dir(&catalog).unwrap();
        let revision = "a".repeat(64);
        let orphan = "b".repeat(64);
        let staged = catalog.join("c".repeat(64));
        std::fs::create_dir(catalog.join(&revision)).unwrap();
        std::fs::create_dir(catalog.join(&orphan)).unwrap();
        std::fs::create_dir(&staged).unwrap();
        let mut keep = BTreeSet::from([staged.clone()]);
        for n in 0..8 {
            let publication = root.path().join(format!("{n:064x}"));
            std::fs::create_dir(&publication).unwrap();
            std::os::unix::fs::symlink(
                format!("../.sources/{revision}"),
                publication.join(&revision),
            )
            .unwrap();
            if n == 0 {
                keep.insert(publication);
            }
        }
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.path().join(".sources.lock"))
            .unwrap();
        lock.lock().unwrap();
        assert!(!collect_publications(root.path(), &keep).unwrap());
        assert!(catalog.join(&orphan).exists());
        drop(lock);
        assert!(collect_publications(root.path(), &keep).unwrap());
        assert!(catalog.join(&revision).exists());
        assert!(!catalog.join(&orphan).exists());
        assert!(
            staged.exists(),
            "a staged revision survives before any map names it"
        );
        assert!(root.path().join(format!("{:064x}", 0)).exists());
        assert_eq!(
            std::fs::read_dir(root.path())
                .unwrap()
                .filter_map(Result::ok)
                .filter(|e| e.file_name().to_str().is_some_and(|s| s.len() == 64))
                .count(),
            4
        );
        for entry in std::fs::read_dir(root.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name().to_str().is_some_and(|s| s.len() == 64) {
                std::fs::remove_dir_all(entry.path()).unwrap();
            }
        }
        collect_publications(root.path(), &BTreeSet::new()).unwrap();
        assert!(!catalog.join(&revision).exists());
        assert!(!staged.exists());
    }
}
