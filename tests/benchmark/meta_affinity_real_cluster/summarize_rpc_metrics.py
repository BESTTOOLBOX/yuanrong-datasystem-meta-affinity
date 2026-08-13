#!/usr/bin/env python3
"""Diff the RPC-related Prometheus snapshots emitted by cluster_runner.py."""

from __future__ import annotations

import argparse
import csv
import math
import pathlib
import re
from typing import Iterable


SNAPSHOT_SUFFIX = re.compile(r"^(?P<case>.+)_metrics_(?P<phase>before|after)\.prom$")
FIVE_READER_CASE = re.compile(
    r"^(?P<build>B0|PR)_five_readers_(?P<payload>\d+)_r(?P<repeat>\d+)_(?P<client>.+)$"
)
SAMPLE_LINE = re.compile(
    r"^(?P<name>[A-Za-z_:][A-Za-z0-9_:]*)(?P<labels>\{[^}]*\})?\s+"
    r"(?P<value>[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?|[-+]?Inf|NaN)"
    r"(?:\s+\d+)?$"
)
RELEVANT_METRICS = (
    "worker_rpc_query_meta_latency",
    "worker_rpc_remote_get_outbound_latency",
    "worker_rpc_remote_get_inbound_latency",
    "worker_get_post_query_meta_phase_latency",
    "worker_process_publish_latency",
    "worker_meta_affinity_replicate_",
    "urma_write_latency",
    "urma_wait_latency",
)


def iter_snapshot_paths(inputs: Iterable[pathlib.Path]) -> Iterable[pathlib.Path]:
    for path in inputs:
        if path.is_dir():
            yield from sorted(path.rglob("*_metrics_*.prom"))
        else:
            yield path


def is_relevant(name: str) -> bool:
    if not any(token in name for token in RELEVANT_METRICS):
        return False
    return name.endswith(("_count", "_sum", "_total")) or "queue_depth" in name


def parse_snapshot(path: pathlib.Path) -> dict[str, float]:
    samples: dict[str, float] = {}
    with path.open("r", encoding="utf-8") as source:
        for line_number, raw_line in enumerate(source, 1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            match = SAMPLE_LINE.fullmatch(line)
            if match is None:
                continue
            name = match.group("name")
            if not is_relevant(name):
                continue
            key = name + (match.group("labels") or "")
            value = float(match.group("value"))
            if not math.isfinite(value):
                raise ValueError(f"{path}:{line_number}: non-finite metric value for {key}")
            samples[key] = value
    return samples


def build_rows(inputs: list[pathlib.Path]) -> list[dict[str, object]]:
    pairs: dict[str, dict[str, pathlib.Path]] = {}
    for path in iter_snapshot_paths(inputs):
        match = SNAPSHOT_SUFFIX.fullmatch(path.name)
        if match is None:
            continue
        case_id = match.group("case")
        phase = match.group("phase")
        phase_paths = pairs.setdefault(case_id, {})
        if phase in phase_paths:
            raise ValueError(f"Duplicate {phase} snapshot for {case_id}: {phase_paths[phase]} and {path}")
        phase_paths[phase] = path

    rows: list[dict[str, object]] = []
    for case_id, phase_paths in sorted(pairs.items()):
        if set(phase_paths) != {"before", "after"}:
            raise ValueError(f"Incomplete metric snapshot pair for {case_id}: {sorted(phase_paths)}")
        before = parse_snapshot(phase_paths["before"])
        after = parse_snapshot(phase_paths["after"])
        for metric in sorted(set(before) | set(after)):
            before_value = before.get(metric, 0.0)
            after_value = after.get(metric, 0.0)
            rows.append(
                {
                    "case": case_id,
                    "metric": metric,
                    "before": before_value,
                    "after": after_value,
                    "delta": after_value - before_value,
                }
            )
    return rows


def write_csv(rows: list[dict[str, object]], path: pathlib.Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=["case", "metric", "before", "after", "delta"])
        writer.writeheader()
        writer.writerows(rows)


def format_number(value: object) -> str:
    return f"{float(value):.3f}"


def write_markdown(rows: list[dict[str, object]], path: pathlib.Path) -> None:
    with path.open("w", encoding="utf-8") as output:
        output.write("| Case | Metric | Before | After | Delta |\n")
        output.write("|---|---|---:|---:|---:|\n")
        for row in rows:
            output.write(
                f"| {row['case']} | `{row['metric']}` | {format_number(row['before'])} | "
                f"{format_number(row['after'])} | {format_number(row['delta'])} |\n"
            )


def paired_five_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    counters: dict[tuple[str, int, str], float] = {}
    for row in rows:
        match = FIVE_READER_CASE.fullmatch(str(row["case"]))
        if match is None:
            continue
        metric = str(row["metric"]).split("{", 1)[0]
        kind = ""
        if "worker_rpc_query_meta_latency" in metric and metric.endswith("_count"):
            kind = "query_meta"
        elif "worker_rpc_remote_get_outbound_latency" in metric and metric.endswith("_count"):
            kind = "remote_get"
        if not kind:
            continue
        key = (match.group("build"), int(match.group("payload")), kind)
        counters[key] = counters.get(key, 0.0) + float(row["delta"])

    payloads = sorted({payload for _, payload, _ in counters})
    if not payloads:
        raise ValueError("No paired five-reader QueryMeta/remote-Get metric counters were found")
    result: list[dict[str, object]] = []
    for payload in payloads:
        b0_query = counters.get(("B0", payload, "query_meta"), 0.0)
        b0_remote = counters.get(("B0", payload, "remote_get"), 0.0)
        pr_query = counters.get(("PR", payload, "query_meta"), 0.0)
        pr_remote = counters.get(("PR", payload, "remote_get"), 0.0)
        if b0_query <= 0 or pr_query <= 0:
            raise ValueError(f"Payload {payload} must have QueryMeta RPCs in both B0 and PR")
        if b0_remote <= 0:
            raise ValueError(f"Payload {payload} B0 did not record the required second GetObjectRemote RPC")
        if pr_remote != 0:
            raise ValueError(f"Payload {payload} PR unexpectedly recorded {pr_remote} GetObjectRemote RPCs")
        result.append(
            {
                "payload_bytes": payload,
                "b0_query_meta_count": b0_query,
                "b0_remote_get_count": b0_remote,
                "pr_query_meta_count": pr_query,
                "pr_remote_get_count": pr_remote,
            }
        )
    return result


def write_paired_five_csv(rows: list[dict[str, object]], path: pathlib.Path) -> None:
    fields = [
        "payload_bytes",
        "b0_query_meta_count",
        "b0_remote_get_count",
        "pr_query_meta_count",
        "pr_remote_get_count",
    ]
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_paired_five_markdown(rows: list[dict[str, object]], path: pathlib.Path) -> None:
    with path.open("w", encoding="utf-8") as output:
        output.write(
            "| Payload | B0 QueryMeta count | B0 GetObjectRemote count | "
            "PR QueryMeta count | PR GetObjectRemote count |\n"
        )
        output.write("|---:|---:|---:|---:|---:|\n")
        for row in rows:
            output.write(
                f"| {row['payload_bytes']} | {row['b0_query_meta_count']:.0f} | "
                f"{row['b0_remote_get_count']:.0f} | {row['pr_query_meta_count']:.0f} | "
                f"{row['pr_remote_get_count']:.0f} |\n"
            )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", type=pathlib.Path)
    parser.add_argument("--csv", type=pathlib.Path, default=pathlib.Path("rpc-metrics.csv"))
    parser.add_argument("--markdown", type=pathlib.Path, default=pathlib.Path("rpc-metrics.md"))
    parser.add_argument("--paired-five-csv", type=pathlib.Path)
    parser.add_argument("--paired-five-markdown", type=pathlib.Path)
    args = parser.parse_args()

    rows = build_rows(args.inputs)
    write_csv(rows, args.csv)
    write_markdown(rows, args.markdown)
    print(args.csv)
    print(args.markdown)
    if args.paired_five_csv is not None or args.paired_five_markdown is not None:
        paired_rows = paired_five_rows(rows)
        if args.paired_five_csv is not None:
            write_paired_five_csv(paired_rows, args.paired_five_csv)
            print(args.paired_five_csv)
        if args.paired_five_markdown is not None:
            write_paired_five_markdown(paired_rows, args.paired_five_markdown)
            print(args.paired_five_markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
