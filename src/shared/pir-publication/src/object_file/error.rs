use std::{fmt, io, path::PathBuf};

/// Why complete, unchanged object bytes could not be established.
#[derive(Debug)]
pub enum ObjectFileError {
    Io(io::Error),
    Cancelled,
    InvalidDigest,
    NotRegularFile(PathBuf),
    FileChanged(PathBuf),
    DigestMismatch {
        path: PathBuf,
        expected: String,
        actual: String,
    },
    LengthMismatch {
        expected: u64,
        actual: u64,
    },
}
impl fmt::Display for ObjectFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::Cancelled => f.write_str("object file operation cancelled"),
            Self::InvalidDigest => f.write_str("object digest is not lowercase SHA-256"),
            Self::NotRegularFile(path) => {
                write!(f, "{} is not a regular object file", path.display())
            }
            Self::FileChanged(path) => {
                write!(f, "{} changed during object verification", path.display())
            }
            Self::DigestMismatch {
                path,
                expected,
                actual,
            } => write!(
                f,
                "{} has digest {actual}, expected {expected}",
                path.display()
            ),
            Self::LengthMismatch { expected, actual } => {
                write!(f, "object length {actual} differs from expected {expected}")
            }
        }
    }
}
impl std::error::Error for ObjectFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}
impl From<io::Error> for ObjectFileError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
