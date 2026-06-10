#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
log_dir="$script_dir/.local/logs"
log_file="$log_dir/start-local.log"

mkdir -p "$log_dir"
cd "$script_dir"

echo "Starting local cluster; logging to $log_file"
go run . 2>&1 | tee "$log_file"
