#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
    printf 'Usage: %s writer|reader|inspect|cleanup [benchmark options]\n' "$0" >&2
    exit 2
fi

task=$1
shift
case "${task}" in
    writer|reader|inspect|cleanup)
        ;;
    *)
        printf 'Unknown task: %s\n' "${task}" >&2
        exit 2
        ;;
esac

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
benchmark_dir=$(cd -- "${script_dir}/.." && pwd)
binary=${META_AFFINITY_BENCH_BIN:-"${benchmark_dir}/build/meta_affinity_benchmark"}

if [[ ! -x "${binary}" ]]; then
    printf 'Benchmark binary not found: %s\n' "${binary}" >&2
    printf 'Run scripts/build_benchmark.sh first.\n' >&2
    exit 3
fi

exec "${binary}" --task "${task}" "$@"
