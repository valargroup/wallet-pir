//! The receiver directory's chain reads: Ironwood blocks checked against their
//! headers and the node's tree sizes, every Action's note position, and the payments
//! recoverable with the zero outgoing viewing key.
use crate::NodeError;
use crate::chain::Block;
use zakura_pir_protocol::receiver::crypto::recovery::Action;

/// Bounds retained block memory and the amount of work before an anchor check.
pub const MAX_RECEIVER_BATCH_BLOCKS: u32 = 64;
/// Bounds concurrent load on the canonical RPC.
pub const MAX_RECEIVER_CONCURRENCY: u32 = 16;

/// A block's height and hash (protocol byte order), with the Ironwood note position
/// after it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub height: u32,
    pub hash: [u8; 32],
    pub position: u64,
}

/// A payment's compact context and location, using the library-owned wire record.
/// All hashes use protocol byte order. Recovery authenticates the receiver before
/// the adapter returns this record; parsing wire bytes alone does not.
pub use zakura_pir_protocol::receiver::wire::Payment;

/// One block's Ironwood Actions: every commitment in position order, the count from
/// coinbase transactions, whose recipients the directory excludes although their
/// Actions take positions, and each recovered payment with its 43-byte receiver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiverBlock {
    pub height: u32,
    pub hash: [u8; 32],
    pub parent: [u8; 32],
    pub start_position: u64,
    pub end_position: u64,
    pub coinbase_actions: u64,
    pub payments: Vec<([u8; 43], Payment)>,
    pub commitments: Vec<[u8; 32]>,
}

/// The node reads, for hosts with the `rpc` client.
#[cfg(feature = "rpc")]
mod rpc {
    use super::*;
    use crate::{Network, NodeClient};
    use serde::Deserialize;
    use serde_json::json;

    /// The tree sizes from a verbose `getblock` response.
    #[derive(Deserialize)]
    struct VerboseBlock {
        trees: BlockTrees,
    }

    /// The note commitment tree sizes after a block.
    #[derive(Deserialize)]
    struct BlockTrees {
        ironwood: Option<TreeSize>,
    }

    /// One note commitment tree's size.
    #[derive(Deserialize)]
    struct TreeSize {
        size: u64,
    }

    impl NodeClient {
        /// A mainnet block at `height` and the Ironwood tree size after it, read by that block's
        /// hash so a height reorg cannot mix two block versions. Before mainnet Ironwood
        /// activation the size is zero. Testnet backfill is unsupported.
        pub async fn receiver_boundary(&self, height: u32) -> Result<Checkpoint, NodeError> {
            let displayed = self.block_hash_display(u64::from(height)).await?;
            let hash = crate::hash::parse_display(&displayed)?;
            let position = if height < Network::Mainnet.ironwood_activation() {
                0
            } else {
                let result: VerboseBlock = self.call("getblock", json!([displayed, 1])).await?;
                result
                    .trees
                    .ironwood
                    .ok_or(NodeError::MissingTreeSize(u64::from(height)))?
                    .size
            };
            if self.block_hash_display(u64::from(height)).await? != displayed {
                return Err(NodeError::Block(
                    "chain changed while reading boundary".into(),
                ));
            }
            Ok(Checkpoint {
                height,
                hash,
                position,
            })
        }

        /// The root of completed Ironwood subtree `index`, the 2^16 leaves from position
        /// `index << 16`, and the height of the block completing it, from
        /// `z_getsubtreesbyindex`; `None` while it is incomplete. The node does not
        /// reverse subtree roots, so the root is in the encoding a witness payload uses.
        /// An answer for another pool or index, or a root that is not a canonical field
        /// element, is refused.
        pub async fn ironwood_subtree(
            &self,
            index: u16,
        ) -> Result<Option<([u8; 32], u32)>, NodeError> {
            #[derive(Deserialize)]
            struct Subtrees {
                pool: String,
                start_index: u16,
                subtrees: Vec<Subtree>,
            }
            #[derive(Deserialize)]
            struct Subtree {
                root: String,
                end_height: u32,
            }
            let answer: Subtrees = self
                .call("z_getsubtreesbyindex", json!(["ironwood", index, 1]))
                .await?;
            if answer.pool != "ironwood" || answer.start_index != index || answer.subtrees.len() > 1
            {
                return Err(NodeError::Block(
                    "subtrees are for another pool or index".into(),
                ));
            }
            let Some(subtree) = answer.subtrees.into_iter().next() else {
                return Ok(None);
            };
            let root: [u8; 32] = hex::decode(subtree.root)?
                .try_into()
                .map_err(|_| NodeError::Block("Ironwood subtree root is not 32 bytes".into()))?;
            if !bool::from(orchard::Anchor::from_bytes(root).is_some()) {
                return Err(NodeError::Block(
                    "non-canonical Ironwood subtree root".into(),
                ));
            }
            Ok(Some((root, subtree.end_height)))
        }

        /// The time in the header of the block `hash` (protocol byte order), in Unix
        /// seconds.
        pub async fn block_time(&self, hash: [u8; 32]) -> Result<i64, NodeError> {
            #[derive(Deserialize)]
            struct Header {
                time: i64,
            }
            let displayed = crate::hash::display(&hash);
            let header: Header = self
                .call("getblockheader", json!([displayed, true]))
                .await?;
            Ok(header.time)
        }

        /// The `count` mainnet blocks after `previous`, fetched at most `concurrency` at a time,
        /// then checked as one chain before any is returned: each extends the last, its
        /// transactions match its header, and the node's tree size at the terminal block,
        /// read by its hash, equals `previous`'s position plus every Action in the batch.
        /// The caller must append every returned block in order from `previous`.
        pub async fn receiver_batch(
            &self,
            previous: &Checkpoint,
            count: u32,
            concurrency: u32,
        ) -> Result<Vec<ReceiverBlock>, NodeError> {
            if count == 0
                || count > MAX_RECEIVER_BATCH_BLOCKS
                || concurrency == 0
                || concurrency > MAX_RECEIVER_CONCURRENCY
            {
                return Err(NodeError::Block("invalid receiver batch limits".into()));
            }
            let end = previous
                .height
                .checked_add(count)
                .ok_or_else(|| NodeError::Block("receiver height overflow".into()))?;
            let mut pending = tokio::task::JoinSet::new();
            let mut next = u64::from(previous.height) + 1;
            let mut blocks = Vec::with_capacity(count as usize);
            while next <= u64::from(end) || !pending.is_empty() {
                while next <= u64::from(end) && pending.len() < concurrency as usize {
                    let rpc = self.clone();
                    let height = next as u32;
                    pending
                        .spawn(async move { rpc.decoded_block(height).await.map(|b| (height, b)) });
                    next += 1;
                }
                let result = pending
                    .join_next()
                    .await
                    .expect("nonempty receiver fetch set");
                blocks.push(result.map_err(|e| NodeError::Block(e.to_string()))??);
            }
            blocks.sort_by_key(|(height, _)| *height);
            let mut parent = previous.hash;
            let mut position = previous.position;
            let mut positions = Vec::with_capacity(blocks.len());
            for (height, block) in &blocks {
                if block.coinbase_height().map(|h| h.0) != Some(*height)
                    || block.header.previous_block_hash.0 != parent
                {
                    return Err(NodeError::Block(
                        "receiver batch does not extend saved chain".into(),
                    ));
                }
                check_transactions(block)?;
                // Count every Action, including coinbase and outputs that cannot be recovered.
                position = position
                    .checked_add(action_count(block))
                    .ok_or_else(|| NodeError::Block("receiver position overflow".into()))?;
                positions.push(position);
                parent = block.hash().0;
            }
            // Linked headers bind every downloaded block to this canonical terminal hash.
            // Check tree size once per batch, rather than downloading verbose transactions per block.
            let anchor = self.receiver_boundary(end).await?;
            if anchor.hash != parent || anchor.position != position {
                return Err(NodeError::Block(
                    "receiver batch anchor or tree size mismatch".into(),
                ));
            }
            blocks
                .into_iter()
                .zip(positions)
                .map(|((height, block), position)| extract(&block, height, position))
                .collect()
        }

        /// The block at `height`, decoded from the node's raw `getblock` bytes.
        async fn decoded_block(&self, height: u32) -> Result<Block, NodeError> {
            let bytes = self.raw_block(height.to_string()).await?;
            decode_block(&bytes)
        }
    }
}

/// A block from its exact serialized bytes, refusing trailing bytes.
fn decode_block(bytes: &[u8]) -> Result<Block, NodeError> {
    let mut input = bytes;
    let block =
        Block::zcash_deserialize(&mut input).map_err(|e| NodeError::Block(e.to_string()))?;
    if !input.is_empty() {
        return Err(NodeError::Block("trailing receiver block bytes".into()));
    }
    Ok(block)
}

/// Checks `block`'s transactions against its header's merkle root, which binds every
/// transaction ID and their order. A v5 ID covers every Action field the indexer reads,
/// so a node cannot alter them under a real header.
#[cfg(feature = "rpc")]
fn check_transactions(block: &Block) -> Result<(), NodeError> {
    if block.merkle_root() != block.header.merkle_root {
        return Err(NodeError::Block(
            "receiver block transactions do not match its header".into(),
        ));
    }
    Ok(())
}

/// The receiver payments in the serialized block `raw` at `height`, after which the
/// Ironwood tree holds `end_position` commitments. Coinbase recipients are excluded
/// while their Actions keep their positions.
pub fn extract_block(
    raw: &[u8],
    height: u32,
    end_position: u64,
) -> Result<ReceiverBlock, NodeError> {
    extract(&decode_block(raw)?, height, end_position)
}

/// Exclude coinbase recipients while preserving their contribution to global positions.
fn extract(block: &Block, height: u32, end_position: u64) -> Result<ReceiverBlock, NodeError> {
    if block.coinbase_height().map(|h| h.0) != Some(height) {
        return Err(NodeError::Block("receiver block height mismatch".into()));
    }
    let actions = action_count(block);
    let start_position = end_position
        .checked_sub(actions)
        .ok_or_else(|| NodeError::Block("receiver tree size underflow".into()))?;
    let mut indexed = ReceiverBlock {
        height,
        hash: block.hash().0,
        parent: block.header.previous_block_hash.0,
        start_position,
        end_position,
        coinbase_actions: 0,
        payments: Vec::new(),
        commitments: Vec::new(),
    };
    let mut position = start_position;
    for (tx_index, tx) in block.transactions.iter().enumerate() {
        let coinbase = tx.is_coinbase();
        let txid = tx.hash().0;
        for (action_index, a) in tx.ironwood_actions().enumerate() {
            indexed.commitments.push(a.cm_x);
            if coinbase {
                indexed.coinbase_actions += 1;
            } else {
                let action = Action {
                    cv: a.cv,
                    nullifier: a.nullifier,
                    cmx: a.cm_x,
                    ephemeral_key: a.ephemeral_key,
                    enc_ciphertext: a.enc_ciphertext,
                    out_ciphertext: a.out_ciphertext,
                };
                if let Some(receiver) = action
                    .recover_receiver()
                    .map_err(|e| NodeError::Block(e.to_string()))?
                {
                    indexed.payments.push((
                        *receiver.as_bytes(),
                        Payment {
                            height,
                            block_hash: indexed.hash,
                            txid,
                            tx_index: u32::try_from(tx_index)
                                .map_err(|e| NodeError::Block(e.to_string()))?,
                            action_index: u32::try_from(action_index)
                                .map_err(|e| NodeError::Block(e.to_string()))?,
                            position,
                            action_nullifier: action.nullifier,
                            cmx: action.cmx,
                            ephemeral_key: action.ephemeral_key,
                            ciphertext_prefix: action.enc_ciphertext[..52]
                                .try_into()
                                .expect("fixed ciphertext prefix"),
                        },
                    ));
                }
            }
            position += 1;
        }
    }
    Ok(indexed)
}

/// The number of Ironwood actions in `block`.
fn action_count(block: &Block) -> u64 {
    block
        .transactions
        .iter()
        .map(|t| t.ironwood_actions().count() as u64)
        .sum()
}

#[cfg(test)]
mod tests {
    use crate::Network;

    #[test]
    fn ironwood_activates_where_enhance_expects() {
        assert_eq!(Network::Mainnet.ironwood_activation(), 3_428_143);
    }

    /// The node reads, against an HTTP node double.
    #[cfg(feature = "rpc")]
    mod node {
        use crate::NodeClient;
        use axum::{Json, Router, routing::post};
        use serde_json::{Value, json};

        /// A node that answers every call with `answer`. Returns a client.
        async fn answering_node(answer: Value) -> NodeClient {
            let url = crate::rpc::tests::serve(Router::new().route(
                "/",
                post(move |Json(_): Json<Value>| {
                    let answer = answer.clone();
                    async move { Json(json!({"result": answer, "error": null})) }
                }),
            ))
            .await;
            NodeClient::new(url, None).unwrap()
        }

        /// A completed subtree's root is the bytes the node gives, with its completing
        /// height; an empty list is an incomplete subtree, and an answer for another pool
        /// or index, or a noncanonical root, is refused.
        #[tokio::test]
        async fn ironwood_subtrees_are_read_as_the_node_encodes_them() {
            let root = zakura_chain::orchard::tree::NoteCommitmentTree::default()
                .root()
                .bytes_in_display_order();
            let read = |answer: Value| async move {
                answering_node(answer).await.ironwood_subtree(3).await
            };
            let answer = |pool: &str, index: u16, subtrees: Value| json!({"pool": pool, "start_index": index, "subtrees": subtrees});
            let one = |root: [u8; 32]| json!([{"root": hex::encode(root), "end_height": 9}]);
            assert_eq!(
                read(answer("ironwood", 3, one(root))).await.unwrap(),
                Some((root, 9))
            );
            assert_eq!(read(answer("ironwood", 3, json!([]))).await.unwrap(), None);
            for refused in [
                answer("orchard", 3, one(root)),
                answer("ironwood", 4, one(root)),
                answer("ironwood", 3, one([0xff; 32])),
            ] {
                assert!(read(refused).await.is_err());
            }
        }
    }
}
