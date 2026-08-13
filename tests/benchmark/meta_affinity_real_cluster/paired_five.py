#!/usr/bin/env python3
"""Validate and run the paired B0-versus-PR five-reader experiment."""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys
from typing import Any

from cluster_runner import ClusterRunner, SAFE_IDENTIFIER, load_config


BASELINE_REVISION = "f21e8db8291a5d8061f87de6d266444f6f3d61ad"
COMMON_DEFAULTS: dict[str, Any] = {
    "request_timeout_ms": 60000,
    "location_timeout_ms": 120000,
    "post_write_wait_ms": 0,
    "reader_interval_ms": 0,
    "reader_start_delay_ms": 3000,
    "max_clock_spread_ms": 100,
    "keys_files": {},
    "product_build_args": [],
}
COMMON_FIELDS = (
    "run_id",
    "comparison_id",
    "topology_id",
    "payload_bytes",
    "repeats",
    "warmup",
    "iterations",
    "request_timeout_ms",
    "location_timeout_ms",
    "post_write_wait_ms",
    "reader_interval_ms",
    "reader_start_delay_ms",
    "max_clock_spread_ms",
    "keys_files",
    "product_build_args",
    "placement",
)


def resolved(config: dict[str, Any], field: str) -> Any:
    return config.get(field, COMMON_DEFAULTS.get(field))


def client_identity(node: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(node.get("name", "")),
        str(node.get("ssh", "")),
        str(node.get("worker", "")),
        str(node.get("metrics_url", "")),
    )


def validate_placement(config: dict[str, Any], label: str) -> None:
    placement = config.get("placement")
    if not isinstance(placement, dict):
        raise ValueError(f"{label} config must define placement")
    required = {"writer_data_worker_id", "meta_owner_worker_id", "reader_worker_ids"}
    missing = sorted(required - placement.keys())
    if missing:
        raise ValueError(f"{label} placement is missing: {', '.join(missing)}")
    writer_data = str(placement["writer_data_worker_id"])
    meta_owner = str(placement["meta_owner_worker_id"])
    reader_workers = [str(value) for value in placement["reader_worker_ids"]]
    if not writer_data or not meta_owner or any(not value for value in reader_workers):
        raise ValueError(f"{label} placement worker IDs must be non-empty")
    if writer_data == meta_owner:
        raise ValueError(f"{label} B0 data worker must differ from the Meta Owner")
    if len(reader_workers) != 5 or len(set(reader_workers)) != 5:
        raise ValueError(f"{label} placement must list five distinct reader-local worker IDs")
    if writer_data in reader_workers or meta_owner in reader_workers:
        raise ValueError(f"{label} writer/data and Meta Owner workers must differ from all reader-local workers")


def validate_pair(b0: dict[str, Any], pr: dict[str, Any]) -> None:
    if str(b0.get("build_label")) != "B0":
        raise ValueError("B0 config build_label must be B0")
    if str(pr.get("build_label")) != "PR":
        raise ValueError("PR config build_label must be PR")
    for config, label in ((b0, "B0"), (pr, "PR")):
        for field in ("comparison_id", "topology_id", "expected_revision"):
            value = str(config.get(field, ""))
            if not value:
                raise ValueError(f"{label} config must define {field}")
            if field != "expected_revision" and SAFE_IDENTIFIER.fullmatch(value) is None:
                raise ValueError(f"Invalid {label} {field}: {value!r}")
        expected_revision = str(config["expected_revision"]).lower()
        if len(expected_revision) != 40 or any(ch not in "0123456789abcdef" for ch in expected_revision):
            raise ValueError(f"{label} expected_revision must be a full 40-character commit")
        if len(config.get("readers", [])) != 5:
            raise ValueError(f"{label} paired experiment requires exactly five reader hosts")
        if any(not str(reader.get("metrics_url", "")) for reader in config["readers"]):
            raise ValueError(f"{label} paired experiment requires metrics_url on all five readers")
        validate_placement(config, label)

    if str(b0["expected_revision"]) != BASELINE_REVISION:
        raise ValueError(f"B0 expected_revision must be the original baseline {BASELINE_REVISION}")

    for field in COMMON_FIELDS:
        b0_value = resolved(b0, field)
        pr_value = resolved(pr, field)
        if b0_value != pr_value:
            raise ValueError(f"Paired field {field} differs: B0={b0_value!r}, PR={pr_value!r}")

    if client_identity(b0["writer"]) != client_identity(pr["writer"]):
        raise ValueError("B0 and PR must use the same writer host and writer-local worker")
    b0_readers = [client_identity(node) for node in b0["readers"]]
    pr_readers = [client_identity(node) for node in pr["readers"]]
    if b0_readers != pr_readers:
        raise ValueError("B0 and PR must use the same five reader hosts and reader-local workers")

    placement = b0["placement"]
    if str(b0.get("expected_location", "")) != str(placement["writer_data_worker_id"]):
        raise ValueError("B0 expected_location must be placement.writer_data_worker_id")
    if str(pr.get("expected_location", "")) != str(placement["meta_owner_worker_id"]):
        raise ValueError("PR expected_location must be placement.meta_owner_worker_id")

    b0_source_hash = str(b0.get("expected_benchmark_source_sha256", ""))
    pr_source_hash = str(pr.get("expected_benchmark_source_sha256", ""))
    if len(b0_source_hash) != 64 or any(ch not in "0123456789abcdef" for ch in b0_source_hash.lower()):
        raise ValueError("Paired configs must define a full benchmark source SHA-256")
    if b0_source_hash != pr_source_hash:
        raise ValueError("B0 and PR expected_benchmark_source_sha256 values must match")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=["validate", "run-b0", "run-pr", "collect-b0", "collect-pr"],
    )
    parser.add_argument("--b0-config", type=pathlib.Path, required=True)
    parser.add_argument("--pr-config", type=pathlib.Path, required=True)
    parser.add_argument("--collect-dir", type=pathlib.Path, default=pathlib.Path("collected-five"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        b0 = load_config(args.b0_config)
        pr = load_config(args.pr_config)
        validate_pair(b0, pr)
        print(
            "paired-five validation passed: "
            f"comparison_id={b0['comparison_id']}, topology_id={b0['topology_id']}, "
            "writer=1, meta_owner=1, reader_local_workers=5"
        )
        if args.command == "validate":
            return 0

        selected = b0 if args.command.endswith("b0") else pr
        runner = ClusterRunner(selected, dry_run=args.dry_run)
        if args.command.startswith("run-"):
            runner.preflight()
            runner.run_matrix("five")
        else:
            runner.collect(args.collect_dir)
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
