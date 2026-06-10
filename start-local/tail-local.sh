#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
log_dir="$script_dir/.local/logs"
log_name="${1:-start-local}"

case "$log_name" in
	start-local|cluster)
		log_file="$log_dir/start-local.log"
		;;
	integration-test|integration)
		log_file="$log_dir/integration-test.log"
		;;
	consul)
		log_file="$log_dir/consul.log"
		;;
	*)
		echo "Usage: $0 [start-local|integration-test|consul]" >&2
		exit 2
		;;
esac

if [[ ! -f "$log_file" ]]; then
	echo "Log file does not exist yet: $log_file" >&2
	exit 1
fi

tail -n 200 -F "$log_file"
