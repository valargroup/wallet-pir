.PHONY: build check check-ops test run-server run-worker load-test demo-check fmt

LOAD_TEST_DURATION ?= 60s
LOAD_TEST_PARALLELISM ?= 8
LOAD_TEST_WARMUP ?= 10s
LOAD_TEST_JSON ?= load-test-summary.json
LOAD_TEST_MAX_ERROR_RATE ?= 0.01
LOAD_TEST_URL ?= https://enhance-pir.valargroup.dev

build:
	cargo build --release --workspace --bins --features enhance-pir/cli

check: check-ops
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	cargo test --workspace --release

# The deploy scripts' jq programs, compiled and run against payloads the server
# serializes. Part of `check` because both of the bugs it exists to catch got
# through a green `check`: CI exercises the scripts only in `validate` mode,
# which never parses a served document. Cheap, and it needs no build, so it runs
# first and fails in seconds rather than after the release test suite.
check-ops:
	ops/scripts/check-jq-contracts.sh

test:
	cargo test --workspace --release

run-server:
	cargo run --release -p enhance-pir-server --bin enhance-pir-server -- --help

run-worker:
	cargo run --release -p enhance-pir-server --bin enhance-pir-worker -- --help

load-test:
	cargo run --release -p enhance-pir-load-test -- \
		--server "$(LOAD_TEST_URL)" \
		--duration "$(LOAD_TEST_DURATION)" \
		--parallelism "$(LOAD_TEST_PARALLELISM)" \
		--warmup "$(LOAD_TEST_WARMUP)" \
		$(if $(strip $(LOAD_TEST_JSON)),--json-out "$(LOAD_TEST_JSON)") \
		$(if $(strip $(LOAD_TEST_SEED)),--seed "$(LOAD_TEST_SEED)") \
		$(if $(strip $(LOAD_TEST_SLO_P99_MS)),--slo-p99-ms "$(LOAD_TEST_SLO_P99_MS)") \
		--max-error-rate "$(LOAD_TEST_MAX_ERROR_RATE)"

demo-check:
	cargo check --manifest-path demos/legacy-spendability/Cargo.toml --workspace

fmt:
	cargo fmt --all
	cargo fmt --manifest-path demos/legacy-spendability/Cargo.toml --all
