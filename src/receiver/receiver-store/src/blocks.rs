//! Block hints rebuilt from the existing chain index. Payment history is never pruned.
use crate::{Error, Store};
use zakura_pir_publisher::receiver::{
    blocks::{HeightSet, MAX_RECEIVERS},
    Hash,
};

/// The most blocks a window may retain: seven days at NU7's 25-second target spacing,
/// the shortest. The indexer sizes the window by target time, which is fewer blocks
/// wherever earlier 75-second blocks fall inside it.
pub const MAX_FILTER_BLOCKS: u32 = 7 * 24 * 3600 / 25;

/// The most encoded hint bytes a window holds; older blocks beyond it are left out.
pub const MAX_FILTER_BYTES: usize = 64 * 1024 * 1024;

/// Consecutive blocks' hints from `first`: the hash before it, every block's hash, and
/// the encoded sets of the blocks that pay a receiver, by height.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hints {
    pub first: u32,
    pub parent: Hash,
    pub hashes: Vec<Hash>,
    pub sets: Vec<(u32, Vec<u8>)>,
}

impl Store {
    /// Reads the hints for `from` through `to` in one SQLite snapshot, leaving out the
    /// oldest blocks whose sets would take the total past `max_bytes`, so the result
    /// may start later than `from`.
    pub fn hints(&self, from: u32, to: u32, max_bytes: usize) -> Result<Hints, Error> {
        if from < self.config.start_height || from > to {
            return Err(Error::Malformed);
        }
        let tx = self.db.unchecked_transaction()?;
        let mut hashes = Vec::with_capacity((to - from + 1) as usize);
        {
            let mut query = tx.prepare(
                "SELECT height, hash FROM blocks WHERE height BETWEEN ?1 AND ?2 ORDER BY height",
            )?;
            let rows = query.query_map([from, to], |r| {
                Ok((r.get::<_, u32>(0)?, r.get::<_, Vec<u8>>(1)?))
            })?;
            for row in rows {
                let (height, hash) = row?;
                if height != from + hashes.len() as u32 {
                    return Err(Error::Coverage);
                }
                hashes.push(hash.try_into().map_err(|_| Error::Malformed)?);
            }
        }
        if hashes.len() != (to - from + 1) as usize {
            return Err(Error::Coverage);
        }
        // Newest first, so the byte bound drops the oldest blocks.
        let mut query = tx.prepare(
            "SELECT height, receiver FROM payments WHERE height BETWEEN ?1 AND ?2 ORDER BY height DESC",
        )?;
        let mut rows = query.query([from, to])?;
        let mut sets = Vec::new();
        let (mut bytes, mut first) = (0usize, from);
        let mut block: Option<(u32, Vec<[u8; 43]>)> = None;
        loop {
            let row = rows.next()?;
            let next = row
                .map(|r| -> Result<_, Error> {
                    let receiver: Vec<u8> = r.get(1)?;
                    Ok((
                        r.get::<_, u32>(0)?,
                        <[u8; 43]>::try_from(receiver).map_err(|_| Error::Malformed)?,
                    ))
                })
                .transpose()?;
            if let Some((height, receivers)) =
                block.take_if(|(height, _)| next.is_none_or(|(h, _)| h != *height))
            {
                let set = HeightSet::build(&self.config.genesis, height, &receivers)?
                    .expect("a block with payments has a set");
                bytes += set.encoded_len();
                if bytes > max_bytes {
                    first = height + 1;
                    break;
                }
                sets.push((height, set.bytes().to_vec()));
            }
            let Some((height, receiver)) = next else {
                break;
            };
            let receivers = &mut block.get_or_insert_with(|| (height, Vec::new())).1;
            // Bound a pathological block before collecting all of its receivers.
            if receivers.len() > MAX_RECEIVERS as usize {
                return Err(Error::Capacity);
            }
            receivers.push(receiver);
        }
        sets.reverse();
        let skipped = (first - from) as usize;
        let parent = match skipped {
            0 if from == self.config.start_height => self.config.start_parent,
            0 => crate::checkpoint_at(&tx, &self.config, from - 1)?.hash,
            n => hashes[n - 1],
        };
        hashes.drain(..skipped);
        Ok(Hints {
            first,
            parent,
            hashes,
            sets,
        })
    }
}
