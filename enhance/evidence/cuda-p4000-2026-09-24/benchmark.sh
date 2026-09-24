#!/usr/bin/env bash
set -euo pipefail
cd /home/paperspace/wallet-pir-cuda
export LD_LIBRARY_PATH=/usr/local/cuda-12.2/targets/x86_64-linux/lib
export RAYON_NUM_THREADS=8
nvidia-smi --query-gpu=timestamp,memory.used,utilization.gpu,power.draw --format=csv -lms 100 > /tmp/wallet-gpu-benchmark-memory.csv &
monitor_pid=$!
trap 'kill "$monitor_pid" 2>/dev/null || true' EXIT
/usr/bin/time -v target/release-fast/examples/cuda_domain > /tmp/wallet-gpu-benchmark.log 2> /tmp/wallet-gpu-benchmark-time.log
