#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "$script_dir/.." && pwd)"
log_dir="$script_dir/.local/logs"
log_file="$log_dir/integration-test.log"

mkdir -p "$log_dir"
cd "$repo_dir"

echo "Running integration tests; logging to $log_file"
go test -count=1 -v ./test-integration 2>&1 | tee "$log_file"
