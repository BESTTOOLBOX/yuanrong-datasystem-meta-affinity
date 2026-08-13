#!/usr/bin/env python3
"""Summarize raw benchmark JSONL using mean and observed min/max."""

from __future__ import annotations

import argparse
import csv
import json
import pathlib
import statistics
from collections import defaultdict
from typing import Any


def load_records(paths: list[pathlib.Path]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in paths:
        candidates = sorted(path.rglob("*.jsonl")) if path.is_dir() else [path]
        for candidate in candidates:
            with candidate.open("r", encoding="utf-8") as source:
                for line_number, line in enumerate(source, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError as error:
                        raise ValueError(f"{candidate}:{line_number}: {error}") from error
                    record["_source"] = str(candidate)
                    records.append(record)
    return records


def summarize(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        if record.get("warmup", False) or record.get("kind") not in ("put", "cold_get"):
            continue
        key = (
            record.get("build"),
            record.get("scenario"),
            record.get("kind"),
            int(record.get("payload_bytes", 0)),
        )
        grouped[key].append(record)

    rows: list[dict[str, Any]] = []
    for (build, scenario, kind, payload), values in sorted(grouped.items()):
        successful = [value for value in values if int(value.get("status_code", -1)) == 0]
        latencies = [int(value["latency_ns"]) / 1_000_000 for value in successful]
        client_ids = {str(value.get("client_id", "")) for value in values}
        rows.append(
            {
                "build": build,
                "scenario": scenario,
                "kind": kind,
                "payload_bytes": payload,
                "clients": len(client_ids),
                "samples": len(values),
                "errors": len(values) - len(successful),
                "mean_ms": statistics.fmean(latencies) if latencies else None,
                "min_ms": min(latencies) if latencies else None,
                "max_ms": max(latencies) if latencies else None,
            }
        )
    return rows


def paired_five_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected = [
        record
        for record in records
        if record.get("kind") == "cold_get"
        and record.get("scenario") == "five_readers"
        and not record.get("warmup", False)
        and record.get("build") in ("B0", "PR")
    ]
    if not selected:
        raise ValueError("No B0/PR five_readers cold_get records were found")

    for field in (
        "run_id",
        "comparison_id",
        "topology_id",
        "key_set_digest",
        "client_id",
        "payload_bytes",
        "repeat",
        "iteration",
        "latency_ns",
        "status_code",
        "enable_local_cache",
    ):
        missing = [record["_source"] for record in selected if field not in record]
        if missing:
            raise ValueError(f"Paired five-reader records are missing {field}: {missing[0]}")

    comparison_ids = {str(record["comparison_id"]) for record in selected}
    topology_ids = {str(record["topology_id"]) for record in selected}
    if len(comparison_ids) != 1 or "" in comparison_ids:
        raise ValueError(f"Expected one non-empty comparison_id, found {sorted(comparison_ids)}")
    if len(topology_ids) != 1 or "" in topology_ids:
        raise ValueError(f"Expected one non-empty topology_id, found {sorted(topology_ids)}")
    if any(record["enable_local_cache"] is not True for record in selected):
        raise ValueError("Every paired five-reader record must have enable_local_cache=true")
    failed = [record for record in selected if int(record["status_code"]) != 0]
    if failed:
        raise ValueError(f"Paired five-reader input contains {len(failed)} failed Get records")

    by_build: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = {"B0": {}, "PR": {}}
    for record in selected:
        identity = (
            str(record["run_id"]),
            int(record["payload_bytes"]),
            int(record["repeat"]),
            str(record["client_id"]),
            int(record["iteration"]),
        )
        build = str(record["build"])
        if identity in by_build[build]:
            raise ValueError(f"Duplicate {build} sample identity: {identity}")
        by_build[build][identity] = record
    b0_identities = set(by_build["B0"])
    pr_identities = set(by_build["PR"])
    if b0_identities != pr_identities:
        only_b0 = sorted(b0_identities - pr_identities)[:3]
        only_pr = sorted(pr_identities - b0_identities)[:3]
        raise ValueError(f"B0/PR samples are not paired; only B0={only_b0}, only PR={only_pr}")

    cases: dict[tuple[str, int, int], set[str]] = defaultdict(set)
    digests: dict[tuple[str, str, int, int], set[str]] = defaultdict(set)
    for record in selected:
        case = (str(record["run_id"]), int(record["payload_bytes"]), int(record["repeat"]))
        cases[case].add(str(record["client_id"]))
        digest_key = (str(record["build"]), *case)
        digests[digest_key].add(str(record["key_set_digest"]))
    for case, clients in sorted(cases.items()):
        if len(clients) != 5:
            raise ValueError(f"Case {case} has {len(clients)} reader clients instead of 5: {sorted(clients)}")
        b0_digest = digests[("B0", *case)]
        pr_digest = digests[("PR", *case)]
        if len(b0_digest) != 1 or b0_digest != pr_digest or "" in b0_digest:
            raise ValueError(f"Case {case} key-set digest differs: B0={b0_digest}, PR={pr_digest}")

    payload_records: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for record in selected:
        payload_records[(str(record["build"]), int(record["payload_bytes"]))].append(record)
    payloads = sorted({payload for _, payload in payload_records})
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        b0_values = payload_records.get(("B0", payload), [])
        pr_values = payload_records.get(("PR", payload), [])
        if not b0_values or not pr_values:
            raise ValueError(f"Payload {payload} does not contain both B0 and PR records")
        b0_mean = statistics.fmean(int(value["latency_ns"]) / 1_000_000 for value in b0_values)
        pr_mean = statistics.fmean(int(value["latency_ns"]) / 1_000_000 for value in pr_values)
        difference = pr_mean - b0_mean
        rows.append(
            {
                "payload_bytes": payload,
                "b0_samples": len(b0_values),
                "pr_samples": len(pr_values),
                "b0_mean_ms": b0_mean,
                "pr_mean_ms": pr_mean,
                "difference_ms": difference,
                "change_percent": difference / b0_mean * 100,
            }
        )
    return rows


def write_csv(rows: list[dict[str, Any]], path: pathlib.Path) -> None:
    fields = [
        "build",
        "scenario",
        "kind",
        "payload_bytes",
        "clients",
        "samples",
        "errors",
        "mean_ms",
        "min_ms",
        "max_ms",
    ]
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def format_number(value: Any) -> str:
    return "-" if value is None else f"{float(value):.3f}"


def write_markdown(rows: list[dict[str, Any]], path: pathlib.Path) -> None:
    with path.open("w", encoding="utf-8") as output:
        output.write("| Build | Scenario | Kind | Payload | Clients | Samples | Errors | Mean ms | Min ms | Max ms |\n")
        output.write("|---|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
        for row in rows:
            output.write(
                f"| {row['build']} | {row['scenario']} | {row['kind']} | "
                f"{row['payload_bytes']} | {row['clients']} | {row['samples']} | {row['errors']} | "
                f"{format_number(row['mean_ms'])} | {format_number(row['min_ms'])} | "
                f"{format_number(row['max_ms'])} |\n"
            )


def write_paired_five_csv(rows: list[dict[str, Any]], path: pathlib.Path) -> None:
    fields = [
        "payload_bytes",
        "b0_samples",
        "pr_samples",
        "b0_mean_ms",
        "pr_mean_ms",
        "difference_ms",
        "change_percent",
    ]
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def signed_number(value: float, suffix: str = "") -> str:
    return f"{value:+.3f}{suffix}"


def write_paired_five_markdown(rows: list[dict[str, Any]], path: pathlib.Path) -> None:
    with path.open("w", encoding="utf-8") as output:
        output.write(
            "| Payload | B0 five-reader samples | PR five-reader samples | "
            "B0 mean ms | PR mean ms | PR - B0 | Latency change |\n"
        )
        output.write("|---:|---:|---:|---:|---:|---:|---:|\n")
        for row in rows:
            output.write(
                f"| {row['payload_bytes']} | {row['b0_samples']} | {row['pr_samples']} | "
                f"{row['b0_mean_ms']:.3f} | {row['pr_mean_ms']:.3f} | "
                f"{signed_number(row['difference_ms'])} | "
                f"{signed_number(row['change_percent'], '%')} |\n"
            )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", type=pathlib.Path)
    parser.add_argument("--csv", type=pathlib.Path, default=pathlib.Path("summary.csv"))
    parser.add_argument("--markdown", type=pathlib.Path, default=pathlib.Path("summary.md"))
    parser.add_argument("--paired-five-csv", type=pathlib.Path)
    parser.add_argument("--paired-five-markdown", type=pathlib.Path)
    args = parser.parse_args()

    records = load_records(args.inputs)
    rows = summarize(records)
    write_csv(rows, args.csv)
    write_markdown(rows, args.markdown)
    print(args.csv)
    print(args.markdown)
    if args.paired_five_csv is not None or args.paired_five_markdown is not None:
        paired_rows = paired_five_rows(records)
        if args.paired_five_csv is not None:
            write_paired_five_csv(paired_rows, args.paired_five_csv)
            print(args.paired_five_csv)
        if args.paired_five_markdown is not None:
            write_paired_five_markdown(paired_rows, args.paired_five_markdown)
            print(args.paired_five_markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
