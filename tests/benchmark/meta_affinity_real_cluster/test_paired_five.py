#!/usr/bin/env python3

from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import unittest

from paired_five import BASELINE_REVISION, validate_pair
from summarize import paired_five_rows
from summarize_rpc_metrics import paired_five_rows as paired_five_rpc_rows


def make_config(label: str) -> dict:
    readers = [
        {
            "name": f"reader-{index}",
            "ssh": f"user@reader-{index}",
            "worker": "127.0.0.1:31501",
            "metrics_url": "http://127.0.0.1:31501/prometheus_metrics",
        }
        for index in range(1, 6)
    ]
    placement = {
        "writer_data_worker_id": "writer-worker",
        "meta_owner_worker_id": "meta-worker",
        "reader_worker_ids": [f"reader-worker-{index}" for index in range(1, 6)],
    }
    return {
        "repo_dir": f"/opt/{label.lower()}",
        "results_dir": f"/results/{label.lower()}",
        "run_id": "run001",
        "comparison_id": "pair001",
        "topology_id": "topology001",
        "build_label": label,
        "expected_revision": BASELINE_REVISION if label == "B0" else "b" * 40,
        "expected_benchmark_source_sha256": "a" * 64,
        "payload_bytes": [4096],
        "repeats": 1,
        "warmup": 1,
        "iterations": 2,
        "request_timeout_ms": 60000,
        "post_write_wait_ms": 3000,
        "location_timeout_ms": 120000,
        "expected_location": (
            placement["writer_data_worker_id"] if label == "B0" else placement["meta_owner_worker_id"]
        ),
        "reader_interval_ms": 0,
        "reader_start_delay_ms": 3000,
        "placement": placement,
        "writer": {
            "name": "writer",
            "ssh": "user@writer",
            "worker": "127.0.0.1:31501",
            "metrics_url": "http://127.0.0.1:31501/prometheus_metrics",
        },
        "readers": readers,
    }


def make_records() -> list[dict]:
    records = []
    for build, latency_ns in (("B0", 5_000_000), ("PR", 3_000_000)):
        for reader in range(1, 6):
            for iteration in range(2):
                records.append(
                    {
                        "_source": f"{build}.jsonl",
                        "kind": "cold_get",
                        "build": build,
                        "run_id": "run001",
                        "comparison_id": "pair001",
                        "topology_id": "topology001",
                        "key_set_digest": "1234567890abcdef",
                        "scenario": "five_readers",
                        "client_id": f"reader-{reader}",
                        "enable_local_cache": True,
                        "payload_bytes": 4096,
                        "repeat": 0,
                        "iteration": iteration,
                        "warmup": False,
                        "latency_ns": latency_ns,
                        "status_code": 0,
                    }
                )
    return records


class PairedFiveConfigTest(unittest.TestCase):
    def test_example_configs_pin_the_packaged_benchmark_source(self) -> None:
        benchmark_dir = pathlib.Path(__file__).resolve().parent
        source_bytes = (benchmark_dir / "meta_affinity_benchmark.cpp").read_bytes().replace(b"\r\n", b"\n")
        source_digest = hashlib.sha256(source_bytes).hexdigest()
        for config_name in ("cluster.b0.example.json", "cluster.example.json"):
            config = json.loads((benchmark_dir / config_name).read_text(encoding="utf-8"))
            self.assertEqual(config["expected_benchmark_source_sha256"], source_digest)
            self.assertEqual(config["warmup"], 20)
            self.assertEqual(config["iterations"], 200)
            self.assertEqual(config["reader_interval_ms"], 0)

    def test_accepts_identical_topology_and_workload(self) -> None:
        validate_pair(make_config("B0"), make_config("PR"))

    def test_rejects_different_reader_worker(self) -> None:
        b0 = make_config("B0")
        pr = make_config("PR")
        pr["readers"][3]["worker"] = "127.0.0.1:31502"
        with self.assertRaisesRegex(ValueError, "same five reader"):
            validate_pair(b0, pr)

    def test_rejects_meta_owner_equal_to_b0_data_owner(self) -> None:
        b0 = make_config("B0")
        pr = make_config("PR")
        for config in (b0, pr):
            config["placement"]["meta_owner_worker_id"] = "writer-worker"
        with self.assertRaisesRegex(ValueError, "must differ"):
            validate_pair(b0, pr)

    def test_requires_reader_metrics_for_rpc_path_proof(self) -> None:
        b0 = make_config("B0")
        pr = make_config("PR")
        b0["readers"][0]["metrics_url"] = ""
        with self.assertRaisesRegex(ValueError, "requires metrics_url"):
            validate_pair(b0, pr)


class PairedFiveSummaryTest(unittest.TestCase):
    def test_compares_b0_five_readers_with_pr_five_readers(self) -> None:
        rows = paired_five_rows(make_records())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["b0_samples"], 10)
        self.assertEqual(rows[0]["pr_samples"], 10)
        self.assertAlmostEqual(rows[0]["b0_mean_ms"], 5.0)
        self.assertAlmostEqual(rows[0]["pr_mean_ms"], 3.0)
        self.assertAlmostEqual(rows[0]["difference_ms"], -2.0)
        self.assertAlmostEqual(rows[0]["change_percent"], -40.0)

    def test_rejects_unpaired_sample(self) -> None:
        records = make_records()
        records.pop()
        with self.assertRaisesRegex(ValueError, "not paired"):
            paired_five_rows(records)

    def test_rejects_different_key_manifest(self) -> None:
        records = copy.deepcopy(make_records())
        for record in records:
            if record["build"] == "PR":
                record["key_set_digest"] = "fedcba0987654321"
        with self.assertRaisesRegex(ValueError, "key-set digest differs"):
            paired_five_rows(records)


class PairedFiveRpcSummaryTest(unittest.TestCase):
    def test_requires_b0_two_hops_and_pr_one_hop(self) -> None:
        rows = []
        for build, remote_get in (("B0", 110.0), ("PR", 0.0)):
            case = f"{build}_five_readers_4096_r0_reader-1"
            rows.extend(
                [
                    {
                        "case": case,
                        "metric": "worker_rpc_query_meta_latency_count",
                        "delta": 110.0,
                    },
                    {
                        "case": case,
                        "metric": "worker_rpc_remote_get_outbound_latency_count",
                        "delta": remote_get,
                    },
                ]
            )
        result = paired_five_rpc_rows(rows)
        self.assertEqual(result[0]["b0_query_meta_count"], 110.0)
        self.assertEqual(result[0]["b0_remote_get_count"], 110.0)
        self.assertEqual(result[0]["pr_query_meta_count"], 110.0)
        self.assertEqual(result[0]["pr_remote_get_count"], 0.0)

    def test_rejects_pr_second_hop(self) -> None:
        rows = []
        for build, remote_get in (("B0", 110.0), ("PR", 1.0)):
            case = f"{build}_five_readers_4096_r0_reader-1"
            rows.extend(
                [
                    {"case": case, "metric": "worker_rpc_query_meta_latency_count", "delta": 110.0},
                    {
                        "case": case,
                        "metric": "worker_rpc_remote_get_outbound_latency_count",
                        "delta": remote_get,
                    },
                ]
            )
        with self.assertRaisesRegex(ValueError, "unexpectedly recorded"):
            paired_five_rpc_rows(rows)


if __name__ == "__main__":
    unittest.main()
