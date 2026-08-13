#!/usr/bin/env python3
"""Run the same meta-affinity benchmark binary as writer or reader over SSH."""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any

SAFE_IDENTIFIER = re.compile(r"[A-Za-z0-9_.-]{1,128}\Z")


@dataclass(frozen=True)
class Node:
    name: str
    ssh: str
    worker: str
    metrics_url: str


def quote(value: object) -> str:
    return shlex.quote(str(value))


def load_config(path: pathlib.Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as source:
        config = json.load(source)
    required = {
        "repo_dir",
        "results_dir",
        "run_id",
        "build_label",
        "payload_bytes",
        "repeats",
        "warmup",
        "iterations",
        "writer",
        "readers",
    }
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError(f"Missing config fields: {', '.join(missing)}")
    return config


def parse_node(value: dict[str, Any]) -> Node:
    for field in ("name", "ssh", "worker"):
        if not value.get(field):
            raise ValueError(f"Node is missing {field}: {value}")
    name = str(value["name"])
    if SAFE_IDENTIFIER.fullmatch(name) is None:
        raise ValueError(f"Invalid node name {name!r}; use [A-Za-z0-9_.-] only")
    return Node(
        name=name,
        ssh=str(value["ssh"]),
        worker=str(value["worker"]),
        metrics_url=str(value.get("metrics_url", "")),
    )


class ClusterRunner:
    def __init__(self, config: dict[str, Any], dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        for field in ("run_id", "build_label"):
            value = str(config[field])
            if SAFE_IDENTIFIER.fullmatch(value) is None:
                raise ValueError(f"Invalid {field} {value!r}; use [A-Za-z0-9_.-] only")
        self.writer = parse_node(config["writer"])
        self.readers = [parse_node(value) for value in config["readers"]]
        identities = [self.writer.ssh, *(node.ssh for node in self.readers)]
        if len(identities) != len(set(identities)):
            raise ValueError("Writer and readers must use distinct SSH hosts")
        names = [self.writer.name, *(node.name for node in self.readers)]
        if len(names) != len(set(names)):
            raise ValueError("Every client must have a unique name")
        self.repo_dir = str(config["repo_dir"]).rstrip("/")
        self.results_dir = str(config["results_dir"]).rstrip("/")
        self.task_script = (
            f"{self.repo_dir}/tests/benchmark/meta_affinity_real_cluster/scripts/run_task.sh"
        )

    @property
    def nodes(self) -> list[Node]:
        return [self.writer, *self.readers]

    def remote(self, node: Node, command: str, *, check: bool = True) -> subprocess.CompletedProcess[str]:
        invocation = ["ssh", node.ssh, command]
        print("+", shlex.join(invocation), flush=True)
        if self.dry_run:
            return subprocess.CompletedProcess(invocation, 0, "", "")
        return subprocess.run(invocation, check=check, text=True, capture_output=False)

    def remote_output(self, node: Node, command: str) -> str:
        invocation = ["ssh", node.ssh, command]
        print("+", shlex.join(invocation), flush=True)
        if self.dry_run:
            return ""
        result = subprocess.run(invocation, check=True, text=True, capture_output=True)
        return result.stdout.strip()

    def prepare_result_directories(self) -> None:
        for node in self.nodes:
            self.remote(node, f"mkdir -p -- {quote(self.results_dir)}")

    def preflight(self) -> None:
        revisions: dict[str, str] = {}
        benchmark_hashes: dict[str, str] = {}
        benchmark_source_hashes: dict[str, str] = {}
        machine_ids: dict[str, str] = {}
        clocks: dict[str, int] = {}
        for node in self.nodes:
            benchmark = (
                self.repo_dir
                + "/tests/benchmark/meta_affinity_real_cluster/build/meta_affinity_benchmark"
            )
            benchmark_source = (
                self.repo_dir
                + "/tests/benchmark/meta_affinity_real_cluster/meta_affinity_benchmark.cpp"
            )
            metrics_check = "command -v curl >/dev/null && " if node.metrics_url else ""
            command = (
                f"test -x {quote(self.task_script)} && "
                f"test -x {quote(benchmark)} && "
                f"test -f {quote(benchmark_source)} && "
                f"{metrics_check}"
                f"git -C {quote(self.repo_dir)} diff --quiet && "
                f"git -C {quote(self.repo_dir)} diff --cached --quiet && "
                f"git -C {quote(self.repo_dir)} rev-parse HEAD && "
                f"sha256sum -- {quote(benchmark)} && "
                f"sha256sum -- {quote(benchmark_source)} && "
                f"cat /etc/machine-id && "
                f"date +%s%3N"
            )
            lines = self.remote_output(node, command).splitlines()
            if self.dry_run:
                continue
            if len(lines) != 5:
                raise RuntimeError(f"Unexpected preflight output from {node.name}: {lines}")
            revisions[node.name] = lines[0]
            benchmark_hashes[node.name] = lines[1].split()[0]
            benchmark_source_hashes[node.name] = lines[2].split()[0]
            machine_ids[node.name] = lines[3]
            clocks[node.name] = int(lines[4])
        if self.dry_run:
            return
        if len(set(revisions.values())) != 1:
            raise RuntimeError(f"Client source revisions differ: {revisions}")
        if len(set(benchmark_hashes.values())) != 1:
            raise RuntimeError(f"Client benchmark binaries differ: {benchmark_hashes}")
        if len(set(benchmark_source_hashes.values())) != 1:
            raise RuntimeError(f"Client benchmark sources differ: {benchmark_source_hashes}")
        revision = next(iter(revisions.values()))
        expected_revision = str(self.config.get("expected_revision", ""))
        if expected_revision and revision != expected_revision:
            raise RuntimeError(f"Expected revision {expected_revision}, found {revision}")
        benchmark_source_hash = next(iter(benchmark_source_hashes.values()))
        expected_source_hash = str(self.config.get("expected_benchmark_source_sha256", ""))
        if expected_source_hash and benchmark_source_hash != expected_source_hash:
            raise RuntimeError(
                f"Expected benchmark source SHA-256 {expected_source_hash}, found {benchmark_source_hash}"
            )
        if len(set(machine_ids.values())) != len(machine_ids):
            raise RuntimeError(f"Writer/readers are not on distinct machines: {machine_ids}")
        clock_spread = max(clocks.values()) - min(clocks.values())
        max_clock_spread = int(self.config.get("max_clock_spread_ms", 100))
        if clock_spread > max_clock_spread:
            raise RuntimeError(
                f"Client clock spread is {clock_spread} ms, above {max_clock_spread} ms; synchronize NTP first"
            )
        print(
            "preflight: "
            f"revision={revision}, "
            f"benchmark_sha256={next(iter(benchmark_hashes.values()))}, "
            f"benchmark_source_sha256={benchmark_source_hash}, "
            f"distinct_machines={len(machine_ids)}, "
            f"clock_spread_ms={clock_spread}"
        )

    def build_benchmark(self) -> None:
        script = (
            f"{self.repo_dir}/tests/benchmark/meta_affinity_real_cluster/scripts/build_benchmark.sh"
        )
        for node in self.nodes:
            self.remote(node, quote(script))

    def build_product(self) -> None:
        script = (
            f"{self.repo_dir}/tests/benchmark/meta_affinity_real_cluster/scripts/build_product_real_urma.sh"
        )
        extra = [str(value) for value in self.config.get("product_build_args", [])]
        command = " ".join([quote(script), *(quote(value) for value in extra)])
        for node in self.nodes:
            self.remote(node, command)

    def options(
        self,
        node: Node,
        task: str,
        scenario: str,
        payload: int,
        repeat: int,
        output: str,
        *,
        start_epoch_ms: int = 0,
    ) -> str:
        values: list[tuple[str, object]] = [
            ("worker", node.worker),
            ("run-id", self.config["run_id"]),
            ("scenario", scenario),
            ("build-label", self.config["build_label"]),
            ("client-id", node.name),
            ("payload-bytes", payload),
            ("repeat", repeat),
            ("warmup", self.config["warmup"]),
            ("iterations", self.config["iterations"]),
            ("request-timeout-ms", self.config.get("request_timeout_ms", 60000)),
            ("location-timeout-ms", self.config.get("location_timeout_ms", 120000)),
            ("post-write-wait-ms", self.config.get("post_write_wait_ms", 0)),
            ("start-epoch-ms", start_epoch_ms),
            ("interval-ms", self.config.get("reader_interval_ms", 0)),
            ("output", output),
        ]
        if self.config.get("comparison_id"):
            values.append(("comparison-id", self.config["comparison_id"]))
        if self.config.get("topology_id"):
            values.append(("topology-id", self.config["topology_id"]))
        if task == "writer" and self.config.get("expected_location"):
            values.append(("expected-location", self.config["expected_location"]))
        key_files = self.config.get("keys_files", {})
        key = f"{scenario}:{payload}:{repeat}"
        if key in key_files:
            values.append(("keys-file", key_files[key]))
        rendered = [quote(self.task_script), quote(task)]
        for name, value in values:
            rendered.extend((f"--{name}", quote(value)))
        return " ".join(rendered)

    def result_path(self, scenario: str, payload: int, repeat: int, node: Node) -> str:
        label = str(self.config["build_label"])
        filename = f"{label}_{scenario}_{payload}_r{repeat}_{node.name}.jsonl"
        return f"{self.results_dir}/{filename}"

    def metrics_path(self, scenario: str, payload: int, repeat: int, node: Node, phase: str) -> str:
        label = str(self.config["build_label"])
        filename = f"{label}_{scenario}_{payload}_r{repeat}_{node.name}_metrics_{phase}.prom"
        return f"{self.results_dir}/{filename}"

    def snapshot_metrics(
        self,
        nodes: list[Node],
        scenario: str,
        payload: int,
        repeat: int,
        phase: str,
    ) -> None:
        for node in nodes:
            if not node.metrics_url:
                continue
            output = self.metrics_path(scenario, payload, repeat, node, phase)
            self.require_new_result(node, output)
            command = (
                f"curl --fail --silent --show-error --max-time 10 "
                f"--output {quote(output)} -- {quote(node.metrics_url)}"
            )
            self.remote(node, command)

    def require_new_result(self, node: Node, path: str) -> None:
        self.remote(node, f"test ! -e {quote(path)}")

    def cleanup(self, scenario: str, payload: int, repeat: int) -> None:
        output = self.result_path(scenario, payload, repeat, self.writer)
        command = self.options(self.writer, "cleanup", scenario, payload, repeat, output)
        self.remote(self.writer, command)

    def run_put_case(self, payload: int, repeat: int) -> None:
        scenario = "foreground_put"
        output = self.result_path(scenario, payload, repeat, self.writer)
        self.require_new_result(self.writer, output)
        command = self.options(self.writer, "writer", scenario, payload, repeat, output)
        try:
            self.snapshot_metrics([self.writer], scenario, payload, repeat, "before")
            self.remote(self.writer, command)
            self.inspect_data(scenario, payload, repeat)
            self.snapshot_metrics([self.writer], scenario, payload, repeat, "after")
        finally:
            self.cleanup(scenario, payload, repeat)

    def inspect_data(self, scenario: str, payload: int, repeat: int) -> None:
        output = self.result_path(scenario, payload, repeat, self.writer)
        command = self.options(self.writer, "inspect", scenario, payload, repeat, output)
        self.remote(self.writer, command)

    def write_read_data(self, scenario: str, payload: int, repeat: int) -> None:
        output = self.result_path(scenario, payload, repeat, self.writer)
        command = self.options(self.writer, "writer", scenario, payload, repeat, output)
        self.remote(self.writer, command)
        self.inspect_data(scenario, payload, repeat)

    def run_single_case(self, payload: int, repeat: int) -> None:
        if not self.readers:
            raise RuntimeError("At least one reader is required")
        scenario = "single_reader"
        reader = self.readers[0]
        self.require_new_result(self.writer, self.result_path(scenario, payload, repeat, self.writer))
        output = self.result_path(scenario, payload, repeat, reader)
        self.require_new_result(reader, output)
        try:
            self.write_read_data(scenario, payload, repeat)
            self.snapshot_metrics([reader], scenario, payload, repeat, "before")
            start_epoch_ms = int(time.time() * 1000) + int(self.config.get("reader_start_delay_ms", 3000))
            command = self.options(
                reader, "reader", scenario, payload, repeat, output, start_epoch_ms=start_epoch_ms
            )
            self.remote(reader, command)
            self.snapshot_metrics([reader], scenario, payload, repeat, "after")
        finally:
            self.cleanup(scenario, payload, repeat)

    def run_five_case(self, payload: int, repeat: int) -> None:
        if len(self.readers) < 5:
            raise RuntimeError("Five-reader scenario requires five distinct reader client hosts")
        scenario = "five_readers"
        selected = self.readers[:5]
        self.require_new_result(self.writer, self.result_path(scenario, payload, repeat, self.writer))
        for reader in selected:
            self.require_new_result(reader, self.result_path(scenario, payload, repeat, reader))
        processes: list[tuple[Node, subprocess.Popen[str]]] = []
        try:
            self.write_read_data(scenario, payload, repeat)
            self.snapshot_metrics(selected, scenario, payload, repeat, "before")
            start_epoch_ms = int(time.time() * 1000) + int(self.config.get("reader_start_delay_ms", 3000))
            for reader in selected:
                output = self.result_path(scenario, payload, repeat, reader)
                command = self.options(
                    reader,
                    "reader",
                    scenario,
                    payload,
                    repeat,
                    output,
                    start_epoch_ms=start_epoch_ms,
                )
                invocation = ["ssh", reader.ssh, command]
                print("+", shlex.join(invocation), flush=True)
                if not self.dry_run:
                    processes.append(
                        (reader, subprocess.Popen(invocation, text=True, stdout=sys.stdout, stderr=sys.stderr))
                    )
            failures = []
            for reader, process in processes:
                return_code = process.wait()
                if return_code != 0:
                    failures.append(f"{reader.name}={return_code}")
            if failures:
                raise RuntimeError("Reader tasks failed: " + ", ".join(failures))
            self.snapshot_metrics(selected, scenario, payload, repeat, "after")
        finally:
            # If launching one reader fails after other readers have already
            # started, let those tasks finish before deleting the shared keys.
            # Otherwise cleanup can race with an in-flight Get and turn an SSH
            # launch failure into misleading benchmark failures.
            for _, process in processes:
                if process.poll() is None:
                    process.wait()
            self.cleanup(scenario, payload, repeat)

    def run_matrix(self, scenario: str) -> None:
        self.prepare_result_directories()
        payloads = [int(value) for value in self.config["payload_bytes"]]
        repeats = int(self.config["repeats"])
        for payload in payloads:
            for repeat in range(repeats):
                print(f"case: scenario={scenario} payload={payload} repeat={repeat}", flush=True)
                if scenario == "put":
                    self.run_put_case(payload, repeat)
                elif scenario == "single":
                    self.run_single_case(payload, repeat)
                elif scenario == "five":
                    self.run_five_case(payload, repeat)
                else:
                    raise ValueError(scenario)

    def collect(self, destination: pathlib.Path) -> None:
        destination.mkdir(parents=True, exist_ok=True)
        for node in self.nodes:
            remote_pattern = f"{node.ssh}:{self.results_dir}/*"
            invocation = ["scp", "-p", remote_pattern, str(destination)]
            print("+", shlex.join(invocation), flush=True)
            if not self.dry_run:
                subprocess.run(invocation, check=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=[
            "preflight",
            "build-product",
            "build-benchmark",
            "put",
            "single",
            "five",
            "all",
            "collect",
        ],
    )
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--collect-dir", type=pathlib.Path, default=pathlib.Path("collected-results"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        runner = ClusterRunner(load_config(args.config), dry_run=args.dry_run)
        if args.command == "preflight":
            runner.preflight()
        elif args.command == "build-product":
            runner.build_product()
        elif args.command == "build-benchmark":
            runner.build_benchmark()
        elif args.command in ("put", "single", "five"):
            runner.run_matrix(args.command)
        elif args.command == "all":
            runner.preflight()
            for scenario in ("put", "single", "five"):
                runner.run_matrix(scenario)
        else:
            runner.collect(args.collect_dir)
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
