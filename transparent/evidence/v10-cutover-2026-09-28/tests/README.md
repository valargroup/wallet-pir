# Compact fragment restart test

The source under test is `8e69ea75b1e0071e3b978b0c78cc0487377f9e82`, with only
the new `compact_outpoint_context_resets_across_a_sqlite_restart` integration
test added to `wallet_continuation.rs`. No production library or binary source
changed in this follow-up.

The test publishes 79 receives in the first page fragment. A spend of the last
receive starts the next fragment, where its outpoint must be encoded in full.
After two directory queries and the first page query, the wallet reaches its
query budget. The test closes and reopens SQLite, verifies the saved logical
boundary, resumes through native PIR, and compares the complete ledger against
the independent reducer. It also asserts that resumption needs exactly one
page query and no repeated directory queries.

- macOS: one test passed, 1.45 s (11.01 s compilation).
- Linux bench: one test passed, 4.34 s (2 min 20 s compilation), Rust 1.91.0
  release with `RUSTFLAGS="-C target-cpu=x86-64-v3"`, `CXXFLAGS=-mpclmul`,
  `CFLAGS=-mpclmul`, `LIBCLANG_PATH=/usr/lib/llvm-18/lib`, five build jobs.
  The bench was also running a separate v9 mixed-wallet workload, so these
  timings are validation observations, not a controlled performance comparison.

Command: `cargo test -p transparent-shard-server --test wallet_continuation
compact_outpoint_context_resets_across_a_sqlite_restart -- --exact --nocapture`;
Linux additionally used `+1.91.0` and `--release`. Logs are retained alongside
this record. An initial Linux invocation could not find `cargo` in the SSH
PATH; the successful command explicitly included `/root/.cargo/bin`.
