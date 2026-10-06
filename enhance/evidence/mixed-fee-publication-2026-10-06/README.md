# Mixed-transaction fee publication — October 6, 2026

Local correctness evidence for exact whole-transaction fees on Ironwood records,
the `rebuild-journal` subcommand and coordinator adoption of a staged journal.
Source `ab1ddeb` (base `031a23c`), package `enhance-pir-server`, `release-fast`
profile with locked dependencies, default features; `native-reinspiring` was built
but not tested. Machine-readable metadata is in [manifest.json](manifest.json),
raw logs are under [raw/](raw/), checksums in [SHA256SUMS](SHA256SUMS).

This is not a deployment, a production measurement or a qualification. No host was
contacted. No production journal was rebuilt; the repair of a live journal remains
an operator step ([deployment](../../docs/deployment.md#repairing-historical-fees)).

## What was exercised

All transactions go through the real producer (`ZakuraClient::block` /
`block_records`) from serialized bytes served by a JSON-RPC test double that
answers JSON-RPC 1.0 single calls and 2.0 batches (in reverse order, so results
must be placed by id). Mixed transactions reuse the public fixture's two Ironwood
actions and set hand-chosen values, so expected fees are independent of the code:

| Case | Values (zatoshis) | Expected |
| --- | --- | --- |
| transparent into Ironwood | 420,000 in, 400,000 Ironwood | fee 20,000 |
| transparent into Ironwood | 200,000 in, 180,000 Ironwood | fee 20,000 |
| multiple inputs and change | 300,000 + 150,000 in, 100,000 change, 330,000 Ironwood | fee 20,000 |
| same-block, then cached spend | 420,000 → 400,000; next block 50,000 → 45,000 | 20,000, 5,000; no `getrawtransaction` |
| pure-Ironwood public fixture | value balance 10,000 | fee 10,000; only `getblock` calls |
| Orchard + Ironwood | 100,000 transparent + 50,000 Orchard, 130,000 Ironwood | fee 20,000 |
| Orchard + Ironwood, no transparent | 60,000 Orchard, 40,000 Ironwood | fee 20,000, no lookup |
| Sapling + Ironwood | 70,000 transparent + 30,000 Sapling, 80,000 Ironwood | fee 20,000 |
| coinbase with Ironwood actions | — | fee absent, no batch |

Error cases each fail the block with an explicit error: transaction unknown to the
node (-5), output index out of range, another RPC error code, a whole-batch error
object, a short batch, a duplicate id, an out-of-range id, a returned transaction
with the wrong txid, a duplicate input, and outputs exceeding inputs. Records keep
the fixture's `enc_ciphertext` suffix, `cv_net`, `out_ciphertext`, expiry and both
transparent flags; they are decoded with `EnhanceRecord::from_bytes`.

The Sapling bundle is a public mainnet V5 transaction copied from the zakura test
vectors at the pinned revision (see `tests/fixtures/sapling-v5-1687106.md`); only
its bundle and a hand-set value balance enter the synthetic V6 transaction.

The existing canonical CLI test now serves a block with the public transaction plus
a same-block spend and a multi-input change transaction whose second input is
fetched in a batch. The real coordinator binary publishes it; the real wallet
client queries all six positions over HTTP PIR before and after the reorg,
kill-during-prepare and kill-after-commit restarts, with identical answers.

The rebuild test builds an "old" journal from producer output with the fee fields
cleared wherever the old binary omitted them, serves it with the real coordinator,
runs `rebuild-journal` against the live data directory, interrupts it while a block
fetch is held, and resumes it (already staged heights are not fetched again). The
receipt reports height, hash, tree size 10, 6 changed and 2 coinbase records, and
digests that match the staged files. A second run refuses to overwrite it.
Adoption is then checked: a sealed shard covering a changed record refuses and a
crash between the two renames completes (library level, on copies); a corrupt
receipt digest, a changed ciphertext byte and a non-prefix block list are each
rejected by the real coordinator, which keeps its live journal byte-identical and
keeps publishing. The valid staged journal is adopted, the previous journal is
preserved as `enhance.before-rebuild-<receipt id>`, and after the next block all
twelve positions answer over HTTP PIR with the expected fees.

## Results

| Command | Result |
| --- | --- |
| `make check-fast BASE=031a23cb` | pass; 169 lib/bin tests, fast integration tier |
| `RUST_TEST_THREADS=1 cargo test --locked --profile release-fast -p enhance-pir-server --test rpc --test canonical_record` | pass; 10 + 1 tests |
| `make check-package PACKAGE=enhance-pir-server TEST=fee` | pass; 7 tests |
| `cargo clippy --locked --profile release-fast -p enhance-pir-server --all-targets -- -D warnings` | pass |
| `cargo build --locked --profile release-fast -p enhance-pir-server --features native-reinspiring --bins` | pass (build only) |

## Limitations

- The `rpc` target is in the full CI tier, so `check-fast` does not run the new
  integration tests; they were run directly above. New test targets would have
  required changing `tools/ci/enhance-tests.json`.
- Synthetic transactions carry invalid proofs and signatures; fee derivation does
  not need them, consensus validity is trusted to the node.
- No measurement of rebuild throughput or cache hit rate on real chain history.
