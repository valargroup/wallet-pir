//! Large public artifacts are consumed into resident runtime state. Keeping an
//! extra kernel file-cache copy competes with query/build admission under the
//! worker cgroup. Advise away only files we have finished consuming, or snapshots
//! whose durability barrier has completed. This never substitutes for integrity
//! checks or changes the admission ceiling. The kernel may ignore the advice.

pub(crate) fn consumed(file: &std::fs::File) {
    #[cfg(target_os = "linux")]
    if let Err(error) = rustix::fs::fadvise(file, 0, None, rustix::fs::Advice::DontNeed) {
        tracing::debug!(%error, "public artifact cache advice unavailable");
    }
    #[cfg(not(target_os = "linux"))]
    let _ = file;
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};

    #[test]
    fn advice_preserves_contents_and_file_position() {
        let mut file = tempfile::tempfile().unwrap();
        let bytes: Vec<u8> = (0..16387).map(|i| (i % 251) as u8).collect();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
        file.seek(SeekFrom::Start(37)).unwrap();
        super::consumed(&file);
        assert_eq!(file.stream_position().unwrap(), 37);
        file.rewind().unwrap();
        let mut actual = Vec::new();
        file.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, bytes);
    }
}
