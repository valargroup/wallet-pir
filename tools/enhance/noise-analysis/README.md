# Retained Enhance noise verification

This directory keeps the independent mathematical verifier, its tests and the
pinned sampler distribution for historical p16 noise results. Verification is
offline and never contacts or modifies a service.

```sh
python3 tools/enhance/noise-analysis/verify.py /path/to/retained/case.json
python3 -m unittest discover -s tools/enhance/noise-analysis -v
```

The verifier binds each result to its exact implementation revision, query
precision and failure target. Historical q46 and q49 profiles use a `2^-128`
target; q48 uses an explicitly selected `2^-78` full-query correctness target.
Numerical success is not certification or evidence of production activation.
Source/binary hashes and independently checked captures remain part of the
evidence trust boundary. Sampler independence and the pseudorandom generator's
replacement of independent draws remain assumptions.

The Rust extractors and campaign drivers pinned older wallet-pir and IPIR+SP
implementations. To regenerate their results, use the
[complete historical campaign and instructions](https://github.com/valargroup/wallet-pir/tree/113a7d9533cdea6f79119620128cba4f8d761b58/tools/enhance/noise-analysis)
at that immutable revision with the original inputs and source pins. The retained
verifier does not generate new weight extractions or empirical queries.

Current native snapshot certificates use the separate
[native certificate tool](../native-certificate/Cargo.toml); the historical
verifier does not establish correctness for a different backend or parameter set.
