#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
benchmark_dir=$(cd -- "${script_dir}/.." && pwd)
repo_root=$(cd -- "${benchmark_dir}/../../.." && pwd)
sdk_root=${DATASYSTEM_SDK_ROOT:-"${repo_root}/output/cpp"}
build_dir=${META_AFFINITY_BENCH_BUILD_DIR:-"${benchmark_dir}/build"}

if [[ ! -f "${sdk_root}/include/datasystem/object_client.h" ]]; then
    printf 'C++ SDK header not found under %s\n' "${sdk_root}" >&2
    printf 'Set DATASYSTEM_SDK_ROOT or build DataSystem first.\n' >&2
    exit 2
fi

cmake -S "${benchmark_dir}" -B "${build_dir}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DDATASYSTEM_SDK_ROOT="${sdk_root}"
cmake --build "${build_dir}" --parallel "${BUILD_JOBS:-4}"
printf '%s\n' "${build_dir}/meta_affinity_benchmark"
