use super::{Hash, Height, Input, OutPoint, Output, Script, Utxo};
use std::{collections::HashMap, io};
use zcash_protocol::value::{ZatBalance, Zatoshis};
use zcash_script::opcode::Evaluable as _;
use zcash_transaction_wire::TransactionView;

/// Only the action fields ingestion consumes, in their canonical byte encodings.
#[derive(Debug)]
pub(crate) struct Action {
    pub cv: [u8; 32],
    pub nullifier: [u8; 32],
    pub cm_x: [u8; 32],
    pub ephemeral_key: [u8; 32],
    pub enc_ciphertext: [u8; 580],
    pub out_ciphertext: [u8; 80],
}
/// A Sapling output's compact fields: note commitment, ephemeral key and the first
/// 52 bytes of its note ciphertext.
#[derive(Debug)]
pub(crate) struct SaplingOutput {
    pub cmu: [u8; 32],
    pub ephemeral_key: [u8; 32],
    pub compact_ciphertext: [u8; 52],
}
/// The value balances the fee needs, as read from the wire. `sprout` is `None` when
/// a partial sum of its JoinSplits leaves the money range.
#[derive(Debug)]
struct Balances {
    sprout: Option<ZatBalance>,
    sapling: ZatBalance,
    orchard: ZatBalance,
    ironwood: ZatBalance,
}
/// The fields ingestion reads from one transaction, copied out of its wire encoding.
///
/// Decoding checks the wire grammar, amount ranges, the transaction boundary and the
/// coinbase rules below; it does not check that shielded fields are valid curve points,
/// proofs or signatures, which remains the node's responsibility.
#[derive(Debug)]
pub(crate) struct Transaction {
    txid: Hash,
    inputs: Vec<Input>,
    outputs: Vec<Output>,
    expiry_height: u32,
    has_shielded_data: bool,
    balances: Balances,
    sapling_nullifiers: Vec<[u8; 32]>,
    sapling_outputs: Vec<SaplingOutput>,
    orchard_actions: Vec<Action>,
    ironwood_actions: Vec<Action>,
}
impl Transaction {
    /// Reads one transaction from the start of `input` and advances it past the
    /// transaction. At most [`super::MAX_BLOCK_BYTES`] are read.
    pub(crate) fn zcash_deserialize(input: &mut &[u8]) -> io::Result<Self> {
        let limited = &input[..input.len().min(super::MAX_BLOCK_BYTES as usize)];
        let (view, rest) = TransactionView::parse_prefix(limited).map_err(io::Error::other)?;
        let transaction = Self::from_view(&view)?;
        *input = &input[limited.len() - rest.len()..];
        Ok(transaction)
    }
    fn from_view(view: &TransactionView<'_>) -> io::Result<Self> {
        // Versions 1 and 2 have no expiry height; the node reports it as zero.
        let expiry_height = view.expiry_height().unwrap_or(0);
        if expiry_height > i32::MAX as u32 {
            return Err(io::Error::other("expiry height out of range"));
        }
        let mut inputs = Vec::with_capacity(view.transparent_inputs().len());
        for input in view.transparent_inputs() {
            let null_hash = input.prevout_txid == &[0; 32];
            inputs.push(if null_hash && input.prevout_index == u32::MAX {
                Input::Coinbase {
                    height: Some(coinbase_height(input.script_sig).ok_or_else(|| {
                        io::Error::other("invalid coinbase height or script length")
                    })?),
                }
            } else if null_hash {
                return Err(io::Error::other("invalid null input index"));
            } else {
                Input::PrevOut {
                    outpoint: OutPoint {
                        hash: Hash(*input.prevout_txid),
                        index: input.prevout_index,
                    },
                }
            });
        }
        let outputs = view
            .transparent_outputs()
            .iter()
            .map(|output| Output {
                value: output.value,
                lock_script: Script::new(output.script_pubkey),
            })
            .collect();
        let sapling = view.sapling();
        let actions = |bundle: Option<&zcash_transaction_wire::ActionBundle<'_>>| {
            bundle
                .into_iter()
                .flat_map(|b| &b.actions)
                .map(|a| Action {
                    cv: *a.cv,
                    nullifier: *a.nullifier,
                    cm_x: *a.cmx,
                    ephemeral_key: *a.ephemeral_key,
                    enc_ciphertext: *a.enc_ciphertext,
                    out_ciphertext: *a.out_ciphertext,
                })
                .collect()
        };
        Ok(Self {
            txid: Hash(view.txid()),
            inputs,
            outputs,
            expiry_height,
            has_shielded_data: view.has_shielded_components(),
            balances: Balances {
                sprout: view.sprout_value_balance(),
                sapling: view.sapling_value_balance(),
                orchard: view.orchard_value_balance(),
                ironwood: view.ironwood_value_balance(),
            },
            sapling_nullifiers: sapling
                .into_iter()
                .flat_map(|b| &b.spends)
                .map(|spend| *spend.nullifier)
                .collect(),
            sapling_outputs: sapling
                .into_iter()
                .flat_map(|b| &b.outputs)
                .map(|output| SaplingOutput {
                    cmu: *output.cmu,
                    ephemeral_key: *output.ephemeral_key,
                    compact_ciphertext: output.enc_ciphertext[..52]
                        .try_into()
                        .expect("fixed compact ciphertext"),
                })
                .collect(),
            orchard_actions: actions(view.orchard()),
            ironwood_actions: actions(view.ironwood()),
        })
    }
    pub(crate) fn hash(&self) -> Hash {
        self.txid
    }
    pub(crate) fn inputs(&self) -> &[Input] {
        &self.inputs
    }
    pub(crate) fn outputs(&self) -> &[Output] {
        &self.outputs
    }
    pub(crate) fn is_coinbase(&self) -> bool {
        self.inputs
            .iter()
            .any(|i| matches!(i, Input::Coinbase { .. }))
    }
    pub(crate) fn has_transparent_inputs(&self) -> bool {
        !self.inputs.is_empty()
    }
    pub(crate) fn has_transparent_outputs(&self) -> bool {
        !self.outputs.is_empty()
    }
    pub(crate) fn has_shielded_data(&self) -> bool {
        self.has_shielded_data
    }
    pub(crate) fn expiry_height(&self) -> Option<Height> {
        Some(Height(self.expiry_height))
    }
    pub(crate) fn sapling_nullifiers(&self) -> &[[u8; 32]] {
        &self.sapling_nullifiers
    }
    pub(crate) fn sapling_outputs(&self) -> &[SaplingOutput] {
        &self.sapling_outputs
    }
    pub(crate) fn ironwood_actions(&self) -> impl Iterator<Item = &Action> + '_ {
        self.ironwood_actions.iter()
    }
    pub(crate) fn orchard_actions(&self) -> impl Iterator<Item = &Action> + '_ {
        self.orchard_actions.iter()
    }
    pub(crate) fn fee(&self, utxos: &HashMap<OutPoint, Utxo>) -> Result<u64, String> {
        let inputs = self
            .inputs
            .iter()
            .filter_map(Input::outpoint)
            .map(|o| {
                utxos
                    .get(&o)
                    .map(|u| u.output.value)
                    .ok_or_else(|| format!("missing spent output {}:{}", o.hash, o.index))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let input_value = inputs
            .into_iter()
            .sum::<Option<Zatoshis>>()
            .ok_or("transparent input value out of range")?;
        let output_value = self
            .outputs
            .iter()
            .map(|o| o.value)
            .sum::<Option<Zatoshis>>()
            .ok_or("transparent output value out of range")?;
        let transparent = (ZatBalance::from(input_value) - output_value)
            .ok_or("transparent balance out of range")?;
        let sprout = self.balances.sprout.ok_or("Sprout balance out of range")?;
        let balance = [
            transparent,
            sprout,
            self.balances.sapling,
            self.balances.orchard,
            self.balances.ironwood,
        ]
        .into_iter()
        .sum::<Option<ZatBalance>>()
        .ok_or("transaction balance out of range")?;
        u64::try_from(balance).map_err(|e| e.to_string())
    }
}

/// BIP34 script numbers, including the small integer opcodes used at early heights.
fn coinbase_height(script: &[u8]) -> Option<Height> {
    if !(2..=100).contains(&script.len()) {
        return None;
    }
    // The shared mainnet, testnet and regtest genesis script has no BIP34 height.
    const GENESIS_SCRIPT: &[u8] = b"\x04\xff\xff\x07\x1f\x01\x04\x45Zcash0b9c4eef8b7cc417ee5001e3500984b6fea35683a7cac141a043c42064835d34";
    if script == GENESIS_SCRIPT {
        return Some(Height(0));
    }
    let (height, size) = match *script.first()? {
        opcode @ 0x51..=0x60 => (u32::from(opcode - 0x50), 1),
        size @ 1..=5 => {
            let bytes = script.get(1..=usize::from(size))?;
            let mut value = 0u64;
            for (i, byte) in bytes.iter().enumerate() {
                value |= u64::from(*byte) << (8 * i);
            }
            (u32::try_from(value).ok()?, 1 + bytes.len())
        }
        _ => return None,
    };
    if height > i32::MAX as u32
        || script[..size] != zcash_script::pattern::push_num(i64::from(height)).to_bytes()
    {
        return None;
    }
    Some(Height(height))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coinbase_heights_require_canonical_positive_script_numbers() {
        for (script, height) in [
            (&[0x51, 0][..], 1),
            (&[0x60, 0][..], 16),
            (&[1, 17][..], 17),
            (&[2, 0x80, 0][..], 128),
        ] {
            assert_eq!(coinbase_height(script).map(|h| h.0), Some(height));
        }
        for script in [
            &[][..],
            &[0, 0],
            &[1, 1],
            &[2, 17, 0],
            &[1, 0x80],
            &[2, 0x80],
            &[5, 0, 0, 0, 0x80, 0],
        ] {
            assert!(
                coinbase_height(script).is_none(),
                "accepted invalid height {script:?}"
            );
        }
        assert!(coinbase_height(&[0x51; 101]).is_none());
    }
}
