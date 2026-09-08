.PHONY: build check check-ops check-docs test run-server run-worker load-test transparent-load-test demo-check fmt

LOAD_TEST_DURATION ?= 60s
LOAD_TEST_PARALLELISM ?= 8
LOAD_TEST_WARMUP ?= 10s
LOAD_TEST_JSON ?= load-test-summary.json
LOAD_TEST_MAX_ERROR_RATE ?= 0.01
LOAD_TEST_URL ?= https://enhance-pir.valargroup.dev

TRANSPARENT_LOAD_URL ?= https://transparent-pir.valargroup.dev
TRANSPARENT_LOAD_SAMPLE ?= sample.json
TRANSPARENT_LOAD_STEPS ?= 8,32,128,512
TRANSPARENT_LOAD_STEP_DURATION ?= 10m
TRANSPARENT_LOAD_JSON ?= transparent-load-report.json

build:
	cargo build --release --workspace --bins --features enhance-pir/cli

check: check-ops check-docs
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

# Every relative Markdown link must resolve. The transparent PIR documentation
# rules delete superseded prose instead of leaving stubs, so a dangling link is
# the failure this catches.
check-docs:
	tools/check-doc-links.sh

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

# Real wallet syncs through the wallet crate's HTTP adapters, at stepped
# concurrency, against a sample drawn from the journal by script-sample. Every
# sync is checked against the sample's expected event digest, so the report
# measures correct recoveries per second, not requests.
transparent-load-test:
	cargo run --release -p transparent-loadtest -- \
		--shard-url "$(TRANSPARENT_LOAD_URL)" \
		$(if $(strip $(TRANSPARENT_LOAD_FILTER_URL)),--filter-url "$(TRANSPARENT_LOAD_FILTER_URL)") \
		--sample "$(TRANSPARENT_LOAD_SAMPLE)" \
		--steps "$(TRANSPARENT_LOAD_STEPS)" \
		--step-duration "$(TRANSPARENT_LOAD_STEP_DURATION)" \
		$(if $(strip $(TRANSPARENT_LOAD_METRICS_URL)),--metrics-url "$(TRANSPARENT_LOAD_METRICS_URL)") \
		$(if $(strip $(TRANSPARENT_LOAD_STORE_DIR)),--store-dir "$(TRANSPARENT_LOAD_STORE_DIR)") \
		$(if $(strip $(TRANSPARENT_LOAD_RUN_ID)),--run-id "$(TRANSPARENT_LOAD_RUN_ID)") \
		--source-sha "$$(git rev-parse HEAD)" \
		--json-out "$(TRANSPARENT_LOAD_JSON)"

demo-check:
	cargo check --manifest-path demos/legacy-spendability/Cargo.toml --workspace

fmt:
	cargo fmt --all
	cargo fmt --manifest-path demos/legacy-spendability/Cargo.toml --all

# Correctness at wallet-accepted anchors; each run preserves its SQLite stores
# and exact mismatch evidence in a new output directory.
TRANSPARENT_REGRESSION_FIXTURE ?= server/transparent-regression/fixtures/mainnet.json
TRANSPARENT_REGRESSION_OUT ?= transparent-regression-results
TRANSPARENT_REGRESSION_FILTER_URL ?= https://enhance-pir.valargroup.dev
.PHONY: transparent-regression
transparent-regression:
	cargo run --locked --release -p transparent-regression --bin transparent-regression -- \
		--fixture "$(TRANSPARENT_REGRESSION_FIXTURE)" \
		--shard-url "$(TRANSPARENT_LOAD_URL)" \
		--filter-url "$(TRANSPARENT_REGRESSION_FILTER_URL)" \
		--source-sha "$$(git rev-parse HEAD)" \
		--out-dir "$(TRANSPARENT_REGRESSION_OUT)"
