//! Prepared databases and evaluation contributions.

use inspiring::InspiringError;
use std::io::{self, Read};

/// Add an evaluation contribution, such as one unit's or one worker's
/// SimplePIR intermediate, into an accumulator modulo `modulus`.
///
/// Both sides must have the same width and already be reduced: an
/// unreduced coefficient is refused rather than silently folded in.
pub fn add_intermediate_assign_mod(
    accumulator: &mut [u64],
    contribution: &[u64],
    modulus: u64,
) -> Result<(), InspiringError> {
    if modulus < 2 {
        return Err(InspiringError::PreprocessMismatch(
            "intermediate modulus must be at least two".to_string(),
        ));
    }
    if accumulator.len() != contribution.len() {
        return Err(InspiringError::LweShape(format!(
            "intermediate widths differ: {} and {}",
            accumulator.len(),
            contribution.len()
        )));
    }

    for (left, right) in accumulator.iter_mut().zip(contribution) {
        if *left >= modulus || *right >= modulus {
            return Err(InspiringError::PreprocessMismatch(
                "intermediate coefficient is not reduced modulo q".to_string(),
            ));
        }
        let sum = u128::from(*left) + u128::from(*right);
        *left = (sum % u128::from(modulus)) as u64;
    }

    Ok(())
}

/// Build a database from exactly `count` stored little-endian u16
/// coefficients read from `reader`, so the coefficients are never held twice.
///
/// Upstream database constructors take an infallible iterator of exactly the
/// declared size. On a read failure the iterator finishes with placeholder
/// zeros and the built value is discarded: the error is returned instead, so
/// no partially read database ever reaches a caller. Nothing past the
/// `count` coefficients is read.
pub fn build_from_le_u16<R: Read, T>(
    reader: &mut R,
    count: usize,
    build: impl FnOnce(LeU16Coefficients<'_, R>) -> T,
) -> io::Result<T> {
    let mut error = None;
    let built = build(LeU16Coefficients {
        reader,
        error: &mut error,
        buffer: [0; 8192],
        index: 0,
        count,
    });
    match error {
        Some(error) => Err(error),
        None => Ok(built),
    }
}

/// The coefficients [`build_from_le_u16`] hands its constructor.
pub struct LeU16Coefficients<'a, R> {
    reader: &'a mut R,
    error: &'a mut Option<io::Error>,
    buffer: [u8; 8192],
    index: usize,
    count: usize,
}

impl<R: Read> Iterator for LeU16Coefficients<'_, R> {
    type Item = u16;

    fn next(&mut self) -> Option<u16> {
        if self.index == self.count {
            return None;
        }
        let offset = self.index % (self.buffer.len() / 2);
        if offset == 0 && self.error.is_none() {
            let chunk = (self.count - self.index).min(self.buffer.len() / 2);
            if let Err(e) = self.reader.read_exact(&mut self.buffer[..chunk * 2]) {
                *self.error = Some(e);
            }
        }
        self.index += 1;
        if self.error.is_some() {
            return Some(0);
        }
        Some(u16::from_le_bytes([
            self.buffer[offset * 2],
            self.buffer[offset * 2 + 1],
        ]))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count - self.index;
        (remaining, Some(remaining))
    }
}

impl<R: Read> ExactSizeIterator for LeU16Coefficients<'_, R> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streams_exactly_the_declared_coefficients() {
        let bytes: Vec<u8> = (0..10_000u16)
            .flat_map(u16::to_le_bytes)
            .chain([7])
            .collect();
        let mut reader = bytes.as_slice();
        let values = build_from_le_u16(&mut reader, 10_000, |c| c.collect::<Vec<_>>()).unwrap();
        assert_eq!(values, (0..10_000).collect::<Vec<_>>());
        assert_eq!(reader, [7]);
    }

    #[test]
    fn a_short_read_fails_after_the_full_count() {
        let bytes = vec![1u8; 9_000];
        let mut seen = 0;
        let error = build_from_le_u16(&mut bytes.as_slice(), 5_000, |c| seen = c.count());
        assert_eq!(error.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(seen, 5_000);
    }

    #[test]
    fn accumulation_is_shape_strict_and_reduced() {
        let mut acc = vec![5, 6];
        add_intermediate_assign_mod(&mut acc, &[6, 1], 7).unwrap();
        assert_eq!(acc, [4, 0]);
        assert!(add_intermediate_assign_mod(&mut acc, &[1], 7).is_err());
        assert!(add_intermediate_assign_mod(&mut acc, &[7, 0], 7).is_err());
        assert!(add_intermediate_assign_mod(&mut acc, &[0, 0], 1).is_err());
    }
}
