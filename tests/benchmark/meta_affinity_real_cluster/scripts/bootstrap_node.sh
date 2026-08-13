#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
    printf 'Usage: %s REPOSITORY_URL GIT_REF DESTINATION [build.sh options]\n' "$0" >&2
    exit 2
fi

repository_url=$1
git_ref=$2
destination=$3
shift 3

if [[ -e "${destination}" && ! -d "${destination}/.git" ]]; then
    printf 'Refusing to overwrite non-git destination: %s\n' "${destination}" >&2
    exit 3
fi

if [[ ! -d "${destination}/.git" ]]; then
    git clone "${repository_url}" "${destination}"
fi

git -C "${destination}" fetch --prune origin "${git_ref}"
git -C "${destination}" checkout --detach FETCH_HEAD

"${destination}/tests/benchmark/meta_affinity_real_cluster/scripts/build_product_real_urma.sh" "$@"
"${destination}/tests/benchmark/meta_affinity_real_cluster/scripts/build_benchmark.sh"
