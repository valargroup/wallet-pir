# Filter measurement utilities

These CLI tools support format-specific studies of public transparent filters.
They are not the private shard retrieval implementation. Read the current
[filter API](../../docs/filter-api.md) and
[envelope](../../docs/filter-envelope.md) before interpreting results.

- `transparent_pir_collect.py`: collect canonical block and resolved-script samples.
- `transparent_filter_build.py`: build BIP 158 filters with the Rust CLI.
- `transparent_filter_measure.py`: compare encoding and coverage on the same sample.
- `transparent_filter_validate.py`: check the recorded 1,152-block study's accounting.
- `transparent_pir_sample.py`: protobuf size and experimental Bloom comparison helpers.
- `transparent_pir_grpc.py` and `transparent_pir_verify_grpc.py`: capture/verify the format comparison.

Run each script with `--help` for inputs. The protobuf helpers require `protoc`
and the Python protobuf package; their recorded study used protobuf 6.33.5.
The Rust encoder is built with
`cargo build --release -p transparent-filter --features cli`.

[Retained baselines](../../evidence/baselines/README.md) preserve
samples, schemas and results. Old commands describe their recorded checkout;
use this directory for the current equivalent tools. Historical Bloom accounting
does not define the wallet's supported-script or privacy contract.
