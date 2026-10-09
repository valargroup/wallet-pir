//! Canonical RPC adapter. The shared crate owns recovery, storage and row encoding.
use crate::zakura::{
    Treestate, VerboseBlock, ZakuraClient, ZakuraError, RAW_BLOCK_RESPONSE_BYTES,
    TREESTATE_RESPONSE_BYTES, VERBOSE_BLOCK_RESPONSE_BYTES,
};
use receiver_directory::{
    extract::Action,
    store::{Checkpoint, IndexedBlock},
    Payment,
};
use serde_json::json;
use zakura_chain::{
    block::{merkle, Block},
    parameters::{Network, NetworkUpgrade},
    serialization::ZcashDeserialize,
};

/// Ironwood's mainnet activation height; earlier blocks have no Ironwood tree.
pub fn ironwood_activation() -> u32 {
    NetworkUpgrade::Nu6_3
        .activation_height(&Network::Mainnet)
        .expect("Ironwood activates on mainnet")
        .0
}

/// Bounds retained block memory and the amount of work before an anchor check.
pub const MAX_RECEIVER_BATCH_BLOCKS: u32 = 64;
/// Bounds concurrent load on the canonical RPC.
pub const MAX_RECEIVER_CONCURRENCY: u32 = 16;

impl ZakuraClient {
    /// Fetch tree size by block hash so a height reorg cannot mix two block versions.
    pub async fn receiver_boundary(&self, height: u32) -> Result<Checkpoint, ZakuraError> {
        let displayed = self.block_hash(u64::from(height)).await?;
        let hash = displayed
            .parse::<zakura_chain::block::Hash>()
            .map_err(|e| ZakuraError::Block(e.to_string()))?
            .0;
        let position = if height < ironwood_activation() {
            0
        } else {
            let result: VerboseBlock = self
                .call(
                    "getblock",
                    json!([displayed, 1]),
                    VERBOSE_BLOCK_RESPONSE_BYTES,
                )
                .await?;
            result
                .trees
                .ironwood
                .ok_or(ZakuraError::MissingTreeSize(u64::from(height)))?
                .size
        };
        if self.block_hash(u64::from(height)).await? != displayed {
            return Err(ZakuraError::Block(
                "chain changed while reading boundary".into(),
            ));
        }
        Ok(Checkpoint {
            height,
            hash,
            position,
        })
    }

    /// The Ironwood note commitment tree root after the block `hash` (protocol byte
    /// order) at `height`, from `z_gettreestate` read by that hash. The answer must name
    /// that block and height, and its root must be a canonical field element. The root
    /// is in the encoding the witness file and `MerkleHashOrchard::to_bytes` use: unlike
    /// Sapling's, the node does not reverse it (`Root::bytes_in_display_order`).
    pub async fn ironwood_root(
        &self,
        hash: [u8; 32],
        height: u32,
    ) -> Result<[u8; 32], ZakuraError> {
        let displayed = zakura_chain::block::Hash(hash).to_string();
        let state: Treestate = self
            .call(
                "z_gettreestate",
                json!([displayed]),
                TREESTATE_RESPONSE_BYTES,
            )
            .await?;
        let named = state
            .hash
            .parse::<zakura_chain::block::Hash>()
            .map_err(|e| ZakuraError::Block(e.to_string()))?;
        if named.0 != hash || state.height != u64::from(height) {
            return Err(ZakuraError::Block("tree state is for another block".into()));
        }
        let root = state
            .ironwood
            .and_then(|pool| pool.commitments.final_root)
            .ok_or(ZakuraError::MissingTreeRoot(u64::from(height)))?;
        let root: [u8; 32] = hex::decode(root)?
            .try_into()
            .map_err(|_| ZakuraError::Block("Ironwood root is not 32 bytes".into()))?;
        zakura_chain::orchard::tree::Root::try_from(root)
            .map_err(|e| ZakuraError::Block(e.to_string()))?;
        Ok(root)
    }

    /// Fetch a bounded range concurrently, then validate its complete chain before returning it.
    /// The caller must append every returned block in order from `previous`.
    pub async fn receiver_batch(
        &self,
        previous: &Checkpoint,
        count: u32,
        concurrency: u32,
    ) -> Result<Vec<IndexedBlock>, ZakuraError> {
        if count == 0
            || count > MAX_RECEIVER_BATCH_BLOCKS
            || concurrency == 0
            || concurrency > MAX_RECEIVER_CONCURRENCY
        {
            return Err(ZakuraError::Block("invalid receiver batch limits".into()));
        }
        let end = previous
            .height
            .checked_add(count)
            .ok_or_else(|| ZakuraError::Block("receiver height overflow".into()))?;
        let mut pending = tokio::task::JoinSet::new();
        let mut next = u64::from(previous.height) + 1;
        let mut blocks = Vec::with_capacity(count as usize);
        while next <= u64::from(end) || !pending.is_empty() {
            while next <= u64::from(end) && pending.len() < concurrency as usize {
                let rpc = self.clone();
                let height = next as u32;
                pending.spawn(
                    async move { rpc.receiver_raw_block(height).await.map(|b| (height, b)) },
                );
                next += 1;
            }
            let result = pending
                .join_next()
                .await
                .expect("nonempty receiver fetch set");
            blocks.push(result.map_err(|e| ZakuraError::Block(e.to_string()))??);
        }
        blocks.sort_by_key(|(height, _)| *height);
        let mut parent = previous.hash;
        let mut position = previous.position;
        let mut positions = Vec::with_capacity(blocks.len());
        for (height, block) in &blocks {
            if block.coinbase_height().map(|h| h.0) != Some(*height)
                || block.header.previous_block_hash.0 != parent
            {
                return Err(ZakuraError::Block(
                    "receiver batch does not extend saved chain".into(),
                ));
            }
            check_transactions(block)?;
            // Count every Action, including coinbase and outputs that cannot be recovered.
            position = position
                .checked_add(action_count(block))
                .ok_or_else(|| ZakuraError::Block("receiver position overflow".into()))?;
            positions.push(position);
            parent = block.hash().0;
        }
        // Linked headers bind every downloaded block to this canonical terminal hash.
        // Check tree size once per batch, rather than downloading verbose transactions per block.
        let anchor = self.receiver_boundary(end).await?;
        if anchor.hash != parent || anchor.position != position {
            return Err(ZakuraError::Block(
                "receiver batch anchor or tree size mismatch".into(),
            ));
        }
        blocks
            .into_iter()
            .zip(positions)
            .map(|((height, block), position)| extract_block(&block, height, position))
            .collect()
    }

    /// The block at `height`, decoded from the node's raw `getblock` bytes.
    async fn receiver_raw_block(&self, height: u32) -> Result<Block, ZakuraError> {
        self.raw_block(height.to_string()).await
    }

    /// The block whose hash is `hash` (protocol byte order), with its transactions in
    /// the order the chain commits to: the node's raw block must hash to `hash` and its
    /// transactions must match its header's merkle root.
    pub async fn receiver_block(&self, hash: [u8; 32]) -> Result<Block, ZakuraError> {
        let block = self
            .raw_block(zakura_chain::block::Hash(hash).to_string())
            .await?;
        if block.hash().0 != hash {
            return Err(ZakuraError::Block("block does not match its hash".into()));
        }
        check_transactions(&block)?;
        Ok(block)
    }

    /// The block `id` names, a height or a displayed hash, decoded from the node's raw
    /// `getblock` bytes.
    async fn raw_block(&self, id: String) -> Result<Block, ZakuraError> {
        let raw: String = self
            .call("getblock", json!([id, 0]), RAW_BLOCK_RESPONSE_BYTES)
            .await?;
        let bytes = hex::decode(raw)?;
        let mut input = bytes.as_slice();
        let block =
            Block::zcash_deserialize(&mut input).map_err(|e| ZakuraError::Block(e.to_string()))?;
        if !input.is_empty() {
            return Err(ZakuraError::Block("trailing receiver block bytes".into()));
        }
        Ok(block)
    }
}

/// Checks `block`'s transactions against its header's merkle root, which binds every
/// transaction ID and their order. A v5 ID covers every Action field the indexer reads,
/// so a node cannot alter them under a real header.
fn check_transactions(block: &Block) -> Result<(), ZakuraError> {
    if block.transactions.iter().collect::<merkle::Root>() != block.header.merkle_root {
        return Err(ZakuraError::Block(
            "receiver block transactions do not match its header".into(),
        ));
    }
    Ok(())
}

/// Exclude coinbase recipients while preserving their contribution to global positions.
pub fn extract_block(
    block: &Block,
    height: u32,
    end_position: u64,
) -> Result<IndexedBlock, ZakuraError> {
    if block.coinbase_height().map(|h| h.0) != Some(height) {
        return Err(ZakuraError::Block("receiver block height mismatch".into()));
    }
    let actions = action_count(block);
    let start_position = end_position
        .checked_sub(actions)
        .ok_or_else(|| ZakuraError::Block("receiver tree size underflow".into()))?;
    let mut indexed = IndexedBlock {
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
            indexed.commitments.push(a.cm_x.into());
            if coinbase {
                indexed.coinbase_actions += 1;
            } else {
                let action = Action {
                    cv: a.cv.into(),
                    nullifier: a.nullifier.into(),
                    cmx: a.cm_x.into(),
                    ephemeral_key: a.ephemeral_key.into(),
                    enc_ciphertext: a.enc_ciphertext.into(),
                    out_ciphertext: a.out_ciphertext.into(),
                };
                if let Some(receiver) = action
                    .recover_receiver()
                    .map_err(|e| ZakuraError::Block(e.to_string()))?
                {
                    indexed.payments.push((
                        receiver,
                        Payment {
                            height,
                            block_hash: indexed.hash,
                            txid,
                            tx_index: u32::try_from(tx_index)
                                .map_err(|e| ZakuraError::Block(e.to_string()))?,
                            action_index: u32::try_from(action_index)
                                .map_err(|e| ZakuraError::Block(e.to_string()))?,
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
    use super::*;
    use axum::{routing::post, Json, Router};
    use serde_json::Value;

    #[test]
    fn ironwood_activates_where_enhance_expects() {
        assert_eq!(super::ironwood_activation(), 3_428_143);
    }

    /// A node whose every `z_gettreestate` answer is `answer`. Returns a client.
    async fn treestate_node(answer: Value) -> ZakuraClient {
        let app = Router::new().route(
            "/",
            post(move |Json(_): Json<Value>| {
                let answer = answer.clone();
                async move { Json(json!({"result": answer, "error": null})) }
            }),
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        ZakuraClient::unauthenticated(vec![url]).unwrap()
    }

    /// The root is the bytes the node gives, unreversed: the pinned node encodes its
    /// tree's root with `Root::bytes_in_display_order`, which for Ironwood, unlike
    /// Sapling, is the canonical encoding (see the probe's witness test for the match
    /// with the witness file). An answer for another block or height, a root that is
    /// not a canonical field element or no root at all is refused.
    #[tokio::test]
    async fn the_ironwood_root_is_read_as_the_node_encodes_it() {
        let raw = hex::decode(include_str!("../tests/fixtures/receiver-refund.hex").trim());
        let refund =
            zakura_chain::transaction::Transaction::zcash_deserialize(raw.unwrap().as_slice())
                .unwrap();
        let mut tree = zakura_chain::orchard::tree::NoteCommitmentTree::default();
        tree.append(refund.ironwood_actions().next().unwrap().cm_x)
            .unwrap();
        let node_root = tree.root().bytes_in_display_order();
        let mut reversed = node_root;
        reversed.reverse();
        assert_ne!(reversed, node_root);
        let (height, hash) = (3_500_000, [3; 32]);
        let displayed = zakura_chain::block::Hash(hash).to_string();
        let answer = |hash: &str, height: u32, ironwood: Value| {
            json!({"hash": hash, "height": height, "sapling": {"commitments": {}},
                "orchard": {"commitments": {}}, "ironwood": ironwood})
        };
        let root = |bytes: [u8; 32]| json!({"commitments": {"finalRoot": hex::encode(bytes)}});
        let read = |answer: Value| async move {
            treestate_node(answer)
                .await
                .ironwood_root(hash, height)
                .await
        };
        assert_eq!(
            read(answer(&displayed, height, root(node_root)))
                .await
                .unwrap(),
            node_root
        );
        let other = zakura_chain::block::Hash([4; 32]).to_string();
        for refused in [
            answer(&other, height, root(node_root)),
            answer(&displayed, height + 1, root(node_root)),
            answer(&displayed, height, root([0xff; 32])),
            answer(
                &displayed,
                height,
                json!({"commitments": {"finalRoot": "00"}}),
            ),
        ] {
            assert!(matches!(read(refused).await, Err(ZakuraError::Block(_))));
        }
        for missing in [
            answer(&displayed, height, json!({"commitments": {}})),
            json!({"hash": displayed, "height": height}),
        ] {
            assert!(matches!(
                read(missing).await,
                Err(ZakuraError::MissingTreeRoot(h)) if h == u64::from(height)
            ));
        }
    }
}
