# usage: build.sh <source sha>; builds only regression-export from an immutable git archive.
set -e
sha=$1
export PATH=/root/.cargo/bin:$PATH
export CARGO_INCREMENTAL=0
cd /srv/claude-regression-export-$sha
echo "start $(date -u +%FT%TZ)"
rustc --version
RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq" CXXFLAGS=-mpclmul CFLAGS=-mpclmul \
  cargo build --locked --profile release-fast -p transparent-regression --bin regression-export
sha256sum target/release-fast/regression-export
du -sh target
echo "BUILD_DONE $(date -u +%FT%TZ)"
