use ipir_sp::server::CrsBlock;
use std::io::{self, Read, Write};

const HINT_MAGIC: &[u8; 4] = b"MPH1";

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("malformed wire message: {0}")]
    Malformed(String),
}

/// Writes the existing MPH1 encoding without buffering the complete hint.
pub fn write_crs_blocks(mut output: impl Write, blocks: &[CrsBlock]) -> io::Result<()> {
    output.write_all(HINT_MAGIC)?;
    output.write_all(&(blocks.len() as u32).to_le_bytes())?;
    for block in blocks {
        output.write_all(&(block.rows.len() as u32).to_le_bytes())?;
        for row in &block.rows {
            output.write_all(&(row.len() as u32).to_le_bytes())?;
            for coefficient in row {
                output.write_all(&coefficient.to_le_bytes())?;
            }
        }
    }
    Ok(())
}

pub fn encode_crs_blocks(blocks: &[CrsBlock]) -> Vec<u8> {
    let mut output = Vec::new();
    write_crs_blocks(&mut output, blocks).expect("writing to a vector cannot fail");
    output
}

pub fn decode_crs_blocks(
    bytes: &[u8],
    expected_blocks: usize,
    degree: usize,
) -> Result<Vec<CrsBlock>, WireError> {
    read_crs_blocks(bytes, expected_blocks, degree).map_err(|e| WireError::Malformed(e.to_string()))
}

/// Decodes directly into the final CRS representation, requiring clean EOF.
pub fn read_crs_blocks(
    input: impl Read,
    expected_blocks: usize,
    degree: usize,
) -> io::Result<Vec<CrsBlock>> {
    scan_crs_blocks(input, expected_blocks, degree, true)
}

/// Checks the encoding without retaining its coefficients.
pub fn validate_crs_blocks(
    input: impl Read,
    expected_blocks: usize,
    degree: usize,
) -> io::Result<()> {
    scan_crs_blocks(input, expected_blocks, degree, false).map(|_| ())
}

pub fn crs_encoded_len(blocks: usize, degree: usize) -> io::Result<u64> {
    degree
        .checked_mul(8)
        .and_then(|n| n.checked_add(4))
        .and_then(|n| n.checked_mul(degree))
        .and_then(|n| n.checked_add(4))
        .and_then(|n| n.checked_mul(blocks))
        .and_then(|n| n.checked_add(8))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "CRS length overflow"))
}

fn scan_crs_blocks(
    mut input: impl Read,
    expected_blocks: usize,
    degree: usize,
    retain: bool,
) -> io::Result<Vec<CrsBlock>> {
    crs_encoded_len(expected_blocks, degree)?;
    fn u32(input: &mut impl Read) -> io::Result<usize> {
        let mut bytes = [0; 4];
        input.read_exact(&mut bytes)?;
        Ok(u32::from_le_bytes(bytes) as usize)
    }
    let invalid = |message| io::Error::new(io::ErrorKind::InvalidData, message);
    let mut magic = [0; 4];
    input.read_exact(&mut magic)?;
    if &magic != HINT_MAGIC {
        return Err(invalid("wrong message magic"));
    }
    if u32(&mut input)? != expected_blocks {
        return Err(invalid("unexpected CRS block count"));
    }
    let mut output = Vec::new();
    // Validation needs the framing and exact payload length, not individual
    // coefficients. Digest verification remains in the underlying reader.
    let mut scratch = [0u8; 64 * 1024];
    for _ in 0..expected_blocks {
        if u32(&mut input)? != degree {
            return Err(invalid("unexpected CRS row count"));
        }
        let mut rows = Vec::new();
        for _ in 0..degree {
            if u32(&mut input)? != degree {
                return Err(invalid("unexpected CRS coefficient count"));
            }
            let mut row = if retain {
                Vec::with_capacity(degree)
            } else {
                Vec::new()
            };
            if retain {
                for _ in 0..degree {
                    let mut bytes = [0; 8];
                    input.read_exact(&mut bytes)?;
                    row.push(u64::from_le_bytes(bytes));
                }
            } else {
                let mut remaining = degree * 8; // checked by crs_encoded_len above
                while remaining != 0 {
                    let n = remaining.min(scratch.len());
                    input.read_exact(&mut scratch[..n])?;
                    remaining -= n;
                }
            }
            if retain {
                rows.push(row);
            }
        }
        if retain {
            output.push(CrsBlock { rows });
        }
    }
    let mut trailing = [0];
    if input.read(&mut trailing)? != 0 {
        return Err(invalid("trailing message bytes"));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crs_hint_round_trips_and_is_shape_strict() {
        let degree = 4;
        let blocks = vec![
            CrsBlock {
                rows: (0..degree).map(|r| vec![r as u64; degree]).collect(),
            },
            CrsBlock {
                rows: (0..degree).map(|r| vec![10 + r as u64; degree]).collect(),
            },
        ];
        let bytes = encode_crs_blocks(&blocks);
        let decoded = decode_crs_blocks(&bytes, 2, degree).expect("decode");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[1].rows[3], vec![13; degree]);
        assert!(decode_crs_blocks(&bytes, 1, degree).is_err());
        assert!(decode_crs_blocks(&bytes, 2, degree + 1).is_err());
        assert!(decode_crs_blocks(&bytes[..bytes.len() - 8], 2, degree).is_err());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode_crs_blocks(&trailing, 2, degree).is_err());
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;

    #[test]
    fn validation_reads_payload_in_bounded_chunks() {
        struct Counting<'a> {
            bytes: &'a [u8],
            calls: usize,
            largest: usize,
        }
        impl Read for Counting<'_> {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                self.calls += 1;
                self.largest = self.largest.max(out.len());
                assert!(out.len() <= 64 * 1024);
                self.bytes.read(out)
            }
        }
        let degree = 128;
        let encoded = encode_crs_blocks(&[CrsBlock {
            rows: vec![vec![7; degree]; degree],
        }]);
        let mut reader = Counting {
            bytes: &encoded,
            calls: 0,
            largest: 0,
        };
        validate_crs_blocks(&mut reader, 1, degree).unwrap();
        assert!(
            reader.calls < 3 * degree,
            "per-coefficient reads returned: {}",
            reader.calls
        );
        assert_eq!(reader.largest, degree * 8);

        // A row crossing the chunk boundary must consume both chunks and still
        // reject the following missing row header rather than accepting EOF.
        let degree = 8193usize;
        let mut truncated = Vec::from(*HINT_MAGIC);
        truncated.extend_from_slice(&1u32.to_le_bytes());
        truncated.extend_from_slice(&(degree as u32).to_le_bytes());
        truncated.extend_from_slice(&(degree as u32).to_le_bytes());
        truncated.resize(truncated.len() + degree * 8, 0);
        let mut reader = Counting {
            bytes: &truncated,
            calls: 0,
            largest: 0,
        };
        assert!(validate_crs_blocks(&mut reader, 1, degree).is_err());
        assert_eq!(reader.largest, 64 * 1024);
        assert!(reader.bytes.is_empty());
    }

    #[test]
    fn streaming_codec_matches_legacy_mph1_bytes() {
        let blocks = vec![CrsBlock {
            rows: vec![vec![1, 2], vec![3, 4]],
        }];
        let expected = hex::decode(concat!(
            "4d504831010000000200000002000000",
            "01000000000000000200000000000000",
            "0200000003000000000000000400000000000000"
        ))
        .unwrap();
        let mut encoded = Vec::new();
        write_crs_blocks(&mut encoded, &blocks).unwrap();
        assert_eq!(encoded, expected);
        assert_eq!(crs_encoded_len(1, 2).unwrap(), expected.len() as u64);
        struct Fragmented<'a>(&'a [u8]);
        impl Read for Fragmented<'_> {
            fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
                let n = bytes.len().min(3);
                self.0.read(&mut bytes[..n])
            }
        }
        let decoded = read_crs_blocks(Fragmented(&expected), 1, 2).unwrap();
        assert_eq!(decoded[0].rows, blocks[0].rows);
        validate_crs_blocks(Fragmented(&expected), 1, 2).unwrap();
        for end in 0..expected.len() {
            assert!(read_crs_blocks(&expected[..end], 1, 2).is_err());
            assert!(validate_crs_blocks(&expected[..end], 1, 2).is_err());
        }
        let mut trailing = expected.clone();
        trailing.push(0);
        assert!(validate_crs_blocks(trailing.as_slice(), 1, 2).is_err());
        for offset in [0, 4, 8, 12, 32] {
            let mut malformed = expected.clone();
            malformed[offset] ^= 1;
            assert!(validate_crs_blocks(malformed.as_slice(), 1, 2).is_err());
        }
        assert!(crs_encoded_len(usize::MAX, usize::MAX).is_err());
    }
}
