#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
log_dir="$script_dir/.local/logs"
log_file="$log_dir/consul.log"

if ! command -v consul >/dev/null 2>&1; then
	echo "consul is not installed or is not on PATH" >&2
	exit 1
fi

if consul members >/dev/null 2>&1; then
	echo "Consul is already running"
	exit 0
fi

mkdir -p "$log_dir"
cd "$script_dir"

echo "Starting Consul development agent; logging to $log_file"
consul agent -dev 2>&1 | tee "$log_file"
