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

check: check-ops check-docs check-reports
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	cargo test --workspace --release

# The deploy scripts' jq programs, compiled and run against payloads the server
# serializes. Part of `check` because both of the bugs it exists to catch got
# through a green `check`: CI exercises the scripts only in `validate` mode,
# which never parses a served document. Cheap, and it needs no build, so it runs
# first and fails in seconds rather than after the release test suite.
check-ops:
	python3 -m unittest discover -s ops/tests -p 'test_enhance_autoscale.py'
	ops/scripts/check-jq-contracts.sh
	python3 ops/tests/test_transparent_parents.py
	python3 ops/tests/test_transparent_fleet.py
	python3 ops/tests/test_transparent_publication.py
	python3 ops/tests/test_transparent_burst.py
	python3 ops/tests/test_regression_recut.py
	python3 ops/tests/test_regression_fixture_compare.py
	python3 ops/tests/test_observation_audit.py
	python3 -m unittest discover -s ops/tests -p 'test_transparent_stage_timing.py'

.PHONY: check-reports
check-reports:
	python3 -m unittest discover -s server/transparent-loadtest/tests -p 'test_*.py'

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

# Mixed-wallet simulations. Leave URL overrides blank to use the scenario's
# origins. Export options so the shell receives paths and labels as data.
SIM_SCENARIO ?= server/transparent-loadtest/scenarios/mixed-20-wave.json
SIM_URL ?=
SIM_FILTER_URL ?=
SIM_METRICS ?=
SIM_OUT ?=
SIM_RUN_ID ?=
SIM_PREP_CACHE ?=
SIM_PREP_CACHE_DIR ?=
SIM_PREP_CONCURRENCY ?=
SIM_HTTP_ATTEMPTS ?=
export SIM_PREP_CACHE SIM_PREP_CACHE_DIR SIM_PREP_CONCURRENCY SIM_HTTP_ATTEMPTS
export SIM_SCENARIO SIM_URL SIM_FILTER_URL SIM_METRICS SIM_OUT SIM_RUN_ID

.PHONY: transparent-sim transparent-sim-wave transparent-sim-sustained transparent-sim-help transparent-sim-open

transparent-sim-wave: SIM_SCENARIO = server/transparent-loadtest/scenarios/mixed-20-wave.json

transparent-sim-sustained: SIM_SCENARIO = server/transparent-loadtest/scenarios/mixed-20-sustained.json

# Each invocation gets a fresh report directory unless SIM_OUT is supplied.
# SIM_METRICS is a space-separated list of NAME=http(s)://host/metrics entries.
transparent-sim transparent-sim-wave transparent-sim-sustained:
	@set -eu; \
	out="$${SIM_OUT:-$${TMPDIR:-/tmp}/transparent-sim-$$(date -u +%Y%m%dT%H%M%SZ)-$$$$}"; \
	mkdir -p "$$(dirname "$$out")"; \
	set -- --scenario "$$SIM_SCENARIO" --out-dir "$$out"; \
	if [ -n "$$SIM_URL" ]; then set -- "$$@" --shard-url "$$SIM_URL"; fi; \
	if [ -n "$$SIM_FILTER_URL" ]; then set -- "$$@" --filter-url "$$SIM_FILTER_URL"; fi; \
	if [ -n "$$SIM_RUN_ID" ]; then set -- "$$@" --run-id "$$SIM_RUN_ID"; fi; \
	if [ -n "$$SIM_PREP_CACHE" ]; then set -- "$$@" --preparation-cache "$$SIM_PREP_CACHE"; fi; \
	if [ -n "$$SIM_PREP_CACHE_DIR" ]; then set -- "$$@" --preparation-cache-dir "$$SIM_PREP_CACHE_DIR"; fi; \
	if [ -n "$$SIM_PREP_CONCURRENCY" ]; then set -- "$$@" --preparation-concurrency "$$SIM_PREP_CONCURRENCY"; fi; \
	if [ -n "$$SIM_HTTP_ATTEMPTS" ]; then set -- "$$@" --measured-http-attempts "$$SIM_HTTP_ATTEMPTS"; fi; \
	set -f; \
	for target in $$SIM_METRICS; do set -- "$$@" --metrics-target "$$target"; done; \
	printf 'Scenario: %s\nReport directory: %s\n' "$$SIM_SCENARIO" "$$out"; \
	sim_status=0; \
	cargo run --locked --release -p transparent-loadtest -- "$$@" || sim_status=$$?; \
	if [ -f "$$out/report.html" ]; then python3 server/transparent-loadtest/open_report.py --remember "$$out" || :; printf '\nOpen report: %s/report.html\n' "$$out"; fi; \
	exit "$$sim_status"

transparent-sim-open:
	@python3 server/transparent-loadtest/open_report.py

transparent-sim-help:
	@printf '%s\n' \
		'make transparent-sim             Run the default 20-wallet recovery wave' \
		'make transparent-sim-wave        Run the 20-wallet recovery wave' \
		'make transparent-sim-sustained   Maintain 20 recovery slots for 10 minutes' \
		'make transparent-sim-open        Open the newest saved report in your browser' \
		'' \
		'Options (append NAME=value to the make command):' \
		'  SIM_URL=URL              Override the private retrieval origin' \
		'  SIM_FILTER_URL=URL       Override the public filter origin' \
		'  SIM_METRICS="r1=URL r2=URL"  Named full worker /metrics URLs' \
		'  SIM_PREP_CACHE=reuse|refresh|off  Preparation cache mode (default reuse)' \
		'  SIM_PREP_CACHE_DIR=PATH   Override the persistent preparation cache' \
		'  SIM_PREP_CONCURRENCY=N   Preparation workers (default 2)' \
		'  SIM_HTTP_ATTEMPTS=1..3   Measured HTTP attempts' \
		'  SIM_OUT=PATH             New report directory (default: unique temp path)' \
		'  SIM_RUN_ID=NAME          Optional report identifier' \
		'  SIM_SCENARIO=PATH        Custom scenario JSON; controls profiles and duration' \
		'' \
		'URLs default to the selected scenario. Existing report directories are refused.' \
		'Examples:' \
		'  make transparent-sim SIM_OUT=/tmp/my-wave' \
		'  make transparent-sim SIM_URL=http://localhost:8093 SIM_METRICS="local=http://localhost:8093/metrics"' \
		'  make transparent-sim SIM_SCENARIO=/path/to/custom.json' \
		'  make transparent-sim-compare  # paired PIR/block scan; then transparent-sim-open'

# Block datasets are exported independently from raw chain data; the fresh sample
# comes from sample-oracle's separate journal pass.
BLOCK_DATASET ?= /srv/transparent-sync-bench/data
BLOCK_URL ?= https://transparent-sync-bench.valargroup.dev
BLOCK_FRESH_SAMPLE ?= ops/benchmarks/transparent-comparison-fresh-sample.json
BLOCK_METRICS_URL ?=
BLOCK_ENCODING ?= gzip
SIM_COMPARISON_METADATA ?=
export SIM_COMPARISON_METADATA
export BLOCK_DATASET BLOCK_URL BLOCK_FRESH_SAMPLE BLOCK_METRICS_URL BLOCK_ENCODING
.PHONY: transparent-block-export transparent-block-serve transparent-sim-freeze transparent-sim-compare
transparent-block-export:
	cargo run --locked --release -p transparent-filter-server --bin compact-export -- $(BLOCK_EXPORT_ARGS) --out-dir "$(BLOCK_DATASET)"

transparent-block-serve:
	cargo run --locked --release -p transparent-block-server -- --dataset "$(BLOCK_DATASET)" $(BLOCK_SERVE_ARGS)

transparent-sim-freeze:
	@test -n "$(BLOCK_SELECTED_SAMPLE)" || { echo 'Set BLOCK_SELECTED_SAMPLE=/path/selected-sample.json'; exit 1; }
	cargo run --locked --release -p transparent-loadtest -- --scenario "$(SIM_SCENARIO)" --selected-sample "$(BLOCK_SELECTED_SAMPLE)"

transparent-sim-compare:
	@test -n "$(BLOCK_URL)" -a -n "$(BLOCK_FRESH_SAMPLE)" || { echo 'Set BLOCK_URL and BLOCK_FRESH_SAMPLE (see server/transparent-loadtest/README.md)'; exit 1; }
	cargo build --locked --release -p transparent-loadtest
	@set -eu; \
	out="$${SIM_OUT:-$${TMPDIR:-/tmp}/transparent-sim-compare-$$(date -u +%Y%m%dT%H%M%SZ)-$$$$}"; \
	set -- --binary "$${CARGO_TARGET_DIR:-target}/release/transparent-loadtest" --scenario "$$SIM_SCENARIO" --fresh-sample "$$BLOCK_FRESH_SAMPLE" --block-url "$$BLOCK_URL" --encoding "$$BLOCK_ENCODING" --out-dir "$$out"; \
	if [ -n "$$SIM_URL" ]; then set -- "$$@" --shard-url "$$SIM_URL"; fi; \
	if [ -n "$$SIM_FILTER_URL" ]; then set -- "$$@" --filter-url "$$SIM_FILTER_URL"; fi; \
	if [ -n "$$SIM_PREP_CACHE" ]; then set -- "$$@" --preparation-cache "$$SIM_PREP_CACHE"; fi; \
	if [ -n "$$SIM_PREP_CACHE_DIR" ]; then set -- "$$@" --preparation-cache-dir "$$SIM_PREP_CACHE_DIR"; fi; \
	if [ -n "$$SIM_PREP_CONCURRENCY" ]; then set -- "$$@" --preparation-concurrency "$$SIM_PREP_CONCURRENCY"; fi; \
	if [ -n "$$SIM_HTTP_ATTEMPTS" ]; then set -- "$$@" --measured-http-attempts "$$SIM_HTTP_ATTEMPTS"; fi; \
	if [ -n "$$SIM_COMPARISON_METADATA" ]; then set -- "$$@" --metadata "$$SIM_COMPARISON_METADATA"; fi; \
	if [ -n "$$BLOCK_METRICS_URL" ]; then set -- "$$@" --block-metrics-url "$$BLOCK_METRICS_URL"; fi; \
	set -f; for target in $$SIM_METRICS; do set -- "$$@" --pir-metrics "$$target"; done; \
	status=0; python3 server/transparent-loadtest/compare.py "$$@" || status=$$?; \
	if [ -f "$$out/report.html" ]; then python3 server/transparent-loadtest/open_report.py --remember "$$out"; fi; \
	exit "$$status"

# Local-only worker-stage burst comparison; no fleet endpoints or live state.
TRANSPARENT_BURST_REPETITIONS ?= 2
TRANSPARENT_BURST_BUILD_SLOTS ?= 1 2
TRANSPARENT_BURST_SYSTEMD ?= 0
TRANSPARENT_BURST_EXTERNAL_CLIENTS ?= 0
.PHONY: transparent-burst
transparent-burst:
	python3 ops/scripts/run-transparent-burst.py \
		--repetitions "$(TRANSPARENT_BURST_REPETITIONS)" \
		--build-slots $(TRANSPARENT_BURST_BUILD_SLOTS) \
		$(if $(strip $(TRANSPARENT_BURST_WORKER_BUDGET_SECONDS)),--worker-budget-seconds "$(TRANSPARENT_BURST_WORKER_BUDGET_SECONDS)") \
		$(if $(strip $(TRANSPARENT_BURST_FIXTURE)),--fixture "$(TRANSPARENT_BURST_FIXTURE)") \
		$(if $(strip $(TRANSPARENT_BURST_BINARY)),--test-binary "$(TRANSPARENT_BURST_BINARY)" --source-sha "$(or $(TRANSPARENT_BURST_SOURCE_SHA),$(shell git rev-parse HEAD))") \
		$(if $(filter 1,$(TRANSPARENT_BURST_SYSTEMD)),--systemd) \
		$(if $(filter 1,$(TRANSPARENT_BURST_EXTERNAL_CLIENTS)),--external-clients) \
		$(if $(strip $(TRANSPARENT_BURST_HOST_OVERHEAD_BYTES)),--host-overhead-bytes "$(TRANSPARENT_BURST_HOST_OVERHEAD_BYTES)") \
		$(if $(strip $(TRANSPARENT_BURST_OUT)),--out "$(TRANSPARENT_BURST_OUT)")
