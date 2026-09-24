# rc.2 P16Q48 numerical review packet

Status: awaiting independent review and current production snapshot coverage. This packet records reproducible inputs and specific claims for a reviewer; it is not a sign-off.

## Exact candidate

- Full six-instance query correctness target: at most 2^-78. This is a decoding-error target under the stated sampler model, not a privacy estimate.
- Protocol v6, schema 11, query precision 48 bits, pinned `ipir-sp`/`inspiring` revision `611a29284264d844bf4dba00de2874c5b762f8c2` (rc.2), `valar-spiral-rs` 0.5.3-rc.1.
- ARM64 run: source `69840f3df91ebde3f893a77d867e40e8088a8ced`, binary SHA-256 `9baf2288f84b1c7ba3b127f24005b8d8f86db0e27bc611d4ea42d8c5bca7e9cb`, manifest SHA-256 `07f40dd15c4c57692747c1500f4222430037df388ea0962b9d7f7c69f4d5171d`.
- Native x86 Linux run: source `73233a1a6f44e4149bcf387f826ed3ba40406bc7`, binary SHA-256 `06dfc631e0fdc509713346df3578b420aca17c3cfde476acc435eefa58d43e7c`, manifest SHA-256 `ed773ece85ebb69770ac932c24e4b36e2279d8c2383ddf8874079f532395c96e`.
- `git diff 69840f3 73233a1 -- enhance/tools/noise-analysis` is empty. The platform binaries differ by architecture and build configuration, while the research source and lockfile are identical.
- Raw ARM cases are at `/tmp/wallet-pir-readiness/noise/rc2-arm64` on the operator machine. Raw Linux cases are at `/root/wallet-pir-readiness-rc2-linux` on `roman-ipir-bench-8vcpu`; transfer them after the public load before running the official comparison.

## Recorded matrix result

Each platform ran both occupancy edges of 12 production-derived layouts, six fixture patterns, and three public shard setups: 432 cases, 128 fresh queries per case, 55,296 queries per platform. Both campaigns completed with zero decoding failures. ARM verifier summaries cover all 432 cases and have no failed case; maximum threshold utilization is 0.09536915584483015 and worst reported union log2 upper bound is -78. The ARM summary SHA-256 is `82cfdfc602dd2f1af2ddbd4c00640a279454272bf97c0119c816c3939944dbe7`. Linux verifier summaries also cover all 432 cases with no failed case; maximum threshold utilization is 0.09720083724513025 and worst reported union log2 upper bound is -78. The Linux summary SHA-256 is `9f9e869c68c9838b65d06f05906bb31b54ae06d2da8b7c8c7dc3fe344e1e1882`. Both `complete.json` files explicitly set `clear=false` pending independent review and deployed snapshot coverage. The official deterministic `compare.py` cross-platform run awaits transfer of the Linux raw files after public load.

## Reviewer checks

1. Verify the rc.2 dependency pin, sampler, modulus, packing, rounding and six-instance geometry against the deployed binary, and compare the rc.2 changes with the previously reviewed `6f74a2d7` profile.
2. Review grouped weight extraction in `enhance/tools/noise-analysis/src/main.rs` against the exact backend NTT transforms, centered lifting, automorphic reuse, and both public error-vector cross-checks. A digest alone cannot prove honest extraction.
3. Review `verify.py`'s finite-CDF centered MGF inequality, rational Taylor/remainder bound, deterministic rounding allowance, Chernoff optimization and 12,288-coefficient union bound. Confirm every term is included in the 2^-78 full-query claim.
4. Confirm the independent sampler-draw model and the claimed ChaCha20/OS-entropy assumptions or identify the additional argument required. The numerical certificate is conditional on these assumptions.
5. Re-run `verify.py` over raw cases and `compare.py ARM_DIR LINUX_DIR --expected-cases 432`, and inspect any deterministic mismatch. Complete the current published-snapshot unit-content and public-setup hash check using `snapshots.py` after the public load.
6. Record the reviewed source hashes, raw artifact digests, assumptions, limitations and an explicit acceptance or rejection. A prior q48 review approved the earlier dependency profile and does not by itself sign off this rc.2 matrix.

Source hashes at packet creation: `src/main.rs` `54d443dc2e0a9ea037c0a45beb66dc8a81a34c2a80bf5a1082a59a557a7dc3fa`; `verify.py` `4ff243b8b1de24712403969d49613708a29d4ec7c12a94879f8516e7249a54ac`; `snapshots.py` `b0b8c672b76c8252d9bae60504d343cd1e6d2d17afff4024726068125a60f35d`; `compare.py` `9398a72776c2d8f705a2937a5eec6a510387935a42e14c562e97b7d538fffaf7`; `Cargo.toml` `54f8e248959f1d076fcaf75f97c41413ca5e29be0d447315751a8d5c072e8290`; `Cargo.lock` `230fe42ddfea39e636138de5321681a5e3ec86bf805581157ad093eee087870b`.
