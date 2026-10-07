//! Public receiver filters that a wallet tests locally before any lookup.
//!
//! A publication carries three Golomb-coded sets: receivers with a payment in it
//! (`paid`), receivers a swap provider was given recently (`recent`), and receivers a
//! swap provider was given as a payout address (`seen`). Every wallet downloads the same
//! bytes, so a test reveals nothing. A match means only that a lookup or a watch may be
//! worthwhile: a receiver outside a set matches it about once in 2^[`P`] tests, and the
//! publisher is trusted for completeness, as for the directory rows.
use crate::{Error, Hash, Receiver};
use sha2::{Digest, Sha256};

/// Golomb-Rice parameter; see the module docs for the false match rate.
pub const P: u32 = 10;
/// Largest encoded [`Filters`] a client downloads.
pub const MAX_FILTERS_BYTES: usize = 8 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"IWFLT1\0\0";

/// One Golomb-coded set of receivers, keyed by the publication salt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Filter {
    count: u32,
    bits: Vec<u8>,
}

impl Filter {
    /// Encodes the distinct `receivers` under `key`.
    pub fn build<'a>(key: &Hash, receivers: impl IntoIterator<Item = &'a Receiver>) -> Self {
        let mut receivers: Vec<_> = receivers.into_iter().collect();
        receivers.sort_unstable();
        receivers.dedup();
        let count = u32::try_from(receivers.len()).expect("filter size");
        let range = u64::from(count) << P;
        let mut values: Vec<_> = receivers.iter().map(|r| place(key, r, range)).collect();
        values.sort_unstable();
        let mut bits = BitWriter::default();
        let mut previous = 0;
        for value in values {
            let delta = value - previous;
            previous = value;
            bits.unary(delta >> P);
            bits.write(delta & ((1 << P) - 1), P);
        }
        Self {
            count,
            bits: bits.finish(),
        }
    }

    /// Which of `receivers` may be in the set, in the same order.
    pub fn matches(&self, key: &Hash, receivers: &[Receiver]) -> Vec<bool> {
        let range = u64::from(self.count) << P;
        let mut wanted: Vec<_> = receivers
            .iter()
            .enumerate()
            .map(|(i, r)| (place(key, r, range), i))
            .collect();
        wanted.sort_unstable();
        let mut found = vec![false; receivers.len()];
        let mut values = self.values();
        let mut value = values.next();
        for (target, i) in wanted {
            while value.is_some_and(|v| v < target) {
                value = values.next();
            }
            found[i] = value == Some(target);
        }
        found
    }

    /// Decodes the set's values, which [`Self::decode`] has already checked.
    fn values(&self) -> impl Iterator<Item = u64> + '_ {
        let mut bits = BitReader::new(&self.bits);
        let mut value = 0;
        (0..self.count).map(move |_| {
            value += (bits.unary().expect("checked filter") << P)
                + bits.read(P).expect("checked filter");
            value
        })
    }

    /// Reads one filter, checking every value and the padding.
    fn decode(count: u32, bits: &[u8]) -> Result<Self, Error> {
        let range = u64::from(count) << P;
        let mut reader = BitReader::new(bits);
        let mut value = 0u64;
        for _ in 0..count {
            // A quotient is bounded by the bit count, so the shift cannot overflow.
            let quotient = reader.unary().ok_or(Error::Malformed)?;
            let remainder = reader.read(P).ok_or(Error::Malformed)?;
            value = value
                .checked_add((quotient << P) + remainder)
                .filter(|v| *v < range)
                .ok_or(Error::Malformed)?;
        }
        if !reader.padding() {
            return Err(Error::Malformed);
        }
        Ok(Self {
            count,
            bits: bits.to_vec(),
        })
    }
}

/// The three filters of one publication; see the module docs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Filters {
    pub paid: Filter,
    pub recent: Filter,
    pub seen: Filter,
}

impl Filters {
    /// The publication's filter file.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = MAGIC.to_vec();
        for filter in [&self.paid, &self.recent, &self.seen] {
            out.extend(filter.count.to_le_bytes());
            out.extend(
                u32::try_from(filter.bits.len())
                    .expect("filter size")
                    .to_le_bytes(),
            );
            out.extend(&filter.bits);
        }
        out
    }

    /// Strictly decodes a filter file.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut rest = bytes.strip_prefix(MAGIC).ok_or(Error::Malformed)?;
        let mut next = || -> Result<Filter, Error> {
            let (count, tail) = rest.split_first_chunk::<4>().ok_or(Error::Malformed)?;
            let (len, tail) = tail.split_first_chunk::<4>().ok_or(Error::Malformed)?;
            let len = u32::from_le_bytes(*len) as usize;
            let (bits, tail) = tail.split_at_checked(len).ok_or(Error::Malformed)?;
            rest = tail;
            Filter::decode(u32::from_le_bytes(*count), bits)
        };
        let filters = Self {
            paid: next()?,
            recent: next()?,
            seen: next()?,
        };
        if !rest.is_empty() {
            return Err(Error::Malformed);
        }
        Ok(filters)
    }
}

/// The receiver's value in `[0, range)`.
fn place(key: &Hash, receiver: &Receiver, range: u64) -> u64 {
    let mut h = Sha256::new();
    h.update(b"ironwood-receiver/v1/filter\0");
    h.update(key);
    h.update(receiver.as_bytes());
    let hash = u64::from_le_bytes(h.finalize()[..8].try_into().unwrap());
    ((u128::from(hash) * u128::from(range)) >> 64) as u64
}

#[derive(Default)]
/// Writes bits most significant first, as [`BitReader`] reads them.
struct BitWriter {
    bytes: Vec<u8>,
    used: u32,
}

impl BitWriter {
    /// Appends one bit.
    fn bit(&mut self, bit: bool) {
        if self.used.is_multiple_of(8) {
            self.bytes.push(0);
        }
        if bit {
            *self.bytes.last_mut().unwrap() |= 0x80 >> (self.used % 8);
        }
        self.used += 1;
    }

    /// Appends `n` in unary: `n` one bits, then a zero.
    fn unary(&mut self, n: u64) {
        (0..n).for_each(|_| self.bit(true));
        self.bit(false);
    }

    /// Appends the low `bits` bits of `value`.
    fn write(&mut self, value: u64, bits: u32) {
        (0..bits).rev().for_each(|i| self.bit(value >> i & 1 == 1));
    }

    /// The written bytes, the last one padded with zero bits.
    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

/// Reads bits written by [`BitWriter`].
struct BitReader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> BitReader<'a> {
    /// A reader at the start of `bytes`.
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    /// The next bit, or `None` past the end.
    fn bit(&mut self) -> Option<bool> {
        let byte = *self.bytes.get(self.at / 8)?;
        let bit = byte & (0x80 >> (self.at % 8)) != 0;
        self.at += 1;
        Some(bit)
    }

    /// A value written by [`BitWriter::unary`].
    fn unary(&mut self) -> Option<u64> {
        let mut n = 0;
        while self.bit()? {
            n += 1;
        }
        Some(n)
    }

    /// A `bits`-bit value written by [`BitWriter::write`].
    fn read(&mut self, bits: u32) -> Option<u64> {
        (0..bits).try_fold(0, |v, _| Some(v << 1 | u64::from(self.bit()?)))
    }

    /// Whether only zero bits of the last byte remain.
    fn padding(&mut self) -> bool {
        self.bytes.len() == self.at.div_ceil(8) && std::iter::from_fn(|| self.bit()).all(|b| !b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receivers(n: u32, seed: u8) -> Vec<Receiver> {
        (0..n)
            .map(|i| {
                let sk = orchard::keys::SpendingKey::from_bytes([seed; 32]).unwrap();
                let fvk = orchard::keys::FullViewingKey::from(&sk);
                let address = fvk.address_at(i, orchard::keys::Scope::External);
                Receiver::from_bytes(address.to_raw_address_bytes()).unwrap()
            })
            .collect()
    }

    #[test]
    fn members_match_and_others_rarely_do() {
        let key = [7; 32];
        let members = receivers(200, 1);
        let filters = Filters {
            paid: Filter::build(&key, &members),
            recent: Filter::build(&key, &members[..3]),
            seen: Filter::build(&key, []),
        };
        let decoded = Filters::decode(&filters.encode()).unwrap();
        assert_eq!(decoded, filters);
        assert!(decoded.paid.matches(&key, &members).iter().all(|m| *m));
        assert_eq!(
            decoded.recent.matches(&key, &members[..4]),
            [true, true, true, false]
        );
        assert_eq!(decoded.seen.matches(&key, &members[..2]), [false, false]);
        // About two of 2,000 others match falsely.
        let others = receivers(2000, 2);
        let false_matches = decoded
            .paid
            .matches(&key, &others)
            .iter()
            .filter(|m| **m)
            .count();
        assert!(false_matches < 15, "{false_matches} false matches");
        // About 1.4 bytes per receiver.
        assert!(filters.paid.bits.len() < 320);
    }

    #[test]
    fn decoding_rejects_corruption() {
        let key = [7; 32];
        let paid = Filter::build(&key, &receivers(50, 1));
        let empty = Filter::build(&key, []);
        let filters = Filters {
            paid,
            recent: empty.clone(),
            seen: empty,
        };
        let bytes = filters.encode();
        assert!(Filters::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(Filters::decode(&longer).is_err());
        // A larger count no longer fits the bits.
        let mut count = bytes.clone();
        count[8] += 1;
        assert!(Filters::decode(&count).is_err());
        let mut magic = bytes;
        magic[0] ^= 1;
        assert!(Filters::decode(&magic).is_err());
    }
}
