//! Private transport encoding; public query geometry and row digests are unchanged.
use enhance_pir::status::{Error, Hash, ROWS, ROW_BYTES};
use sha2::{Digest, Sha256};
const MAGIC: &[u8; 4] = b"SRD1";
const HEADER: usize = 40;

/// A delta is used only when it is smaller than the canonical full snapshot.
pub fn encode(rows: &[u8], previous: Option<(Hash, &[u8])>) -> Result<Vec<u8>, Error> {
    if rows.len() != ROWS * ROW_BYTES {
        return Err(Error::Malformed);
    }
    let Some((digest, base)) = previous else {
        return Ok(rows.to_vec());
    };
    if base.len() != rows.len() {
        return Err(Error::Malformed);
    }
    let changed: Vec<_> = rows
        .chunks_exact(ROW_BYTES)
        .zip(base.chunks_exact(ROW_BYTES))
        .enumerate()
        .filter_map(|(i, (new, old))| (new != old).then_some(i))
        .collect();
    if HEADER + changed.len() * (4 + ROW_BYTES) >= rows.len() {
        return Ok(rows.to_vec());
    }
    let mut out = Vec::with_capacity(HEADER + changed.len() * (4 + ROW_BYTES));
    out.extend(MAGIC);
    out.extend(digest);
    out.extend((changed.len() as u32).to_le_bytes());
    for index in changed {
        out.extend((index as u32).to_le_bytes());
        out.extend(&rows[index * ROW_BYTES..(index + 1) * ROW_BYTES]);
    }
    Ok(out)
}

pub fn decode(
    bytes: Vec<u8>,
    expected: Hash,
    base: Option<(Hash, Vec<u8>)>,
) -> Result<Vec<u8>, Error> {
    let rows = if bytes.len() == ROWS * ROW_BYTES {
        bytes
    } else {
        if bytes.len() < HEADER || &bytes[..4] != MAGIC {
            return Err(Error::Malformed);
        }
        let count = u32::from_le_bytes(bytes[36..40].try_into().unwrap()) as usize;
        if count > ROWS || bytes.len() != HEADER + count * (4 + ROW_BYTES) {
            return Err(Error::Malformed);
        }
        let (digest, mut rows) = base.ok_or(Error::Unavailable)?;
        if digest != bytes[4..36] || rows.len() != ROWS * ROW_BYTES {
            return Err(Error::Unavailable);
        }
        let mut previous = None;
        for record in bytes[HEADER..].chunks_exact(4 + ROW_BYTES) {
            let index = u32::from_le_bytes(record[..4].try_into().unwrap()) as usize;
            if index >= ROWS || previous.is_some_and(|old| old >= index) {
                return Err(Error::Malformed);
            }
            rows[index * ROW_BYTES..(index + 1) * ROW_BYTES].copy_from_slice(&record[4..]);
            previous = Some(index);
        }
        rows
    };
    if <Hash>::from(Sha256::digest(&rows)) != expected {
        return Err(Error::Malformed);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sparse_roundtrip_is_bound_to_base_and_result() {
        let base = vec![0; ROWS * ROW_BYTES];
        let digest = Sha256::digest(&base).into();
        let mut rows = base.clone();
        rows[0] = 7;
        rows[(ROWS - 1) * ROW_BYTES] = 8;
        let expected = Sha256::digest(&rows).into();
        let bytes = encode(&rows, Some((digest, &base))).unwrap();
        assert_eq!(bytes.len(), HEADER + 2 * (4 + ROW_BYTES));
        assert!(decode(bytes.clone(), expected, None).is_err());
        assert!(decode(bytes.clone(), expected, Some(([9; 32], base.clone()))).is_err());
        let mut invalid = bytes.clone();
        invalid[HEADER..HEADER + 4].copy_from_slice(&(ROWS as u32).to_le_bytes());
        assert!(decode(invalid, expected, Some((digest, base.clone()))).is_err());
        assert_eq!(decode(bytes, expected, Some((digest, base))).unwrap(), rows);
    }
}
