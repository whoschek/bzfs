# Copyright 2025 Wolfgang Hoschek AT mac DOT com
# Written in 2025 by Orsiris de Jong - ozy AT netpower DOT fr
#
# Licensed under the Apache License, Version 2.0 (the \"License\");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an \"AS IS\" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Tests for Prometheus metrics exporter."""

import os
import tempfile
import unittest
from unittest.mock import (
    MagicMock,
)

from bzfs_main.prometheus_exporter import (
    _determine_action,
    _escape_label,
    _generate_job_id,
    write_prometheus_metrics,
)


class TestPrometheusExporter(unittest.TestCase):
    """Test cases for Prometheus metrics exporter."""

    def test_escape_label(self) -> None:
        """Test label value escaping."""
        self.assertEqual(_escape_label('simple'), 'simple')
        self.assertEqual(_escape_label('with"quote'), 'with\\"quote')
        self.assertEqual(_escape_label('with\\backslash'), 'with\\\\backslash')
        self.assertEqual(_escape_label('with\nnewline'), 'with\\nnewline')
        self.assertEqual(_escape_label('mixed"\\n'), 'mixed\\"\\\\\\n')

    def test_determine_action(self) -> None:
        """Test action determination from CLI parameters."""
        # Test 1: Default replication
        params = MagicMock()
        params.create_src_snapshots_config.is_enabled = False
        params.skip_replication = False
        params.delete_dst_snapshots = False
        params.delete_dst_datasets = False
        params.delete_empty_dst_datasets = False
        params.compare_snapshot_lists = False
        params.monitor_snapshots_config.is_enabled = False
        self.assertEqual(_determine_action(params), "replicate")

        # Test 2: Create snapshots + replicate
        params.create_src_snapshots_config.is_enabled = True
        self.assertEqual(_determine_action(params), "create_snapshots+replicate")

        # Test 3: Create snapshots only (skip replication)
        params.skip_replication = True
        self.assertEqual(_determine_action(params), "create_snapshots")

        # Test 4: Replication with deletion
        params.create_src_snapshots_config.is_enabled = False
        params.skip_replication = False
        params.delete_dst_snapshots = True
        self.assertEqual(_determine_action(params), "replicate+del_snapshots")

        # Test 5: Compare mode
        params.delete_dst_snapshots = False
        params.skip_replication = True
        params.compare_snapshot_lists = True
        self.assertEqual(_determine_action(params), "compare")

        # Test 6: Monitor mode
        params.compare_snapshot_lists = False
        params.monitor_snapshots_config.is_enabled = True
        self.assertEqual(_determine_action(params), "monitor")

        # Test 7: Multiple actions combined
        params.create_src_snapshots_config.is_enabled = True
        params.skip_replication = False
        params.delete_dst_snapshots = True
        params.delete_dst_datasets = True
        params.compare_snapshot_lists = False
        params.monitor_snapshots_config.is_enabled = False
        action = _determine_action(params)
        self.assertIn("create_snapshots", action)
        self.assertIn("replicate", action)
        self.assertIn("del_snapshots", action)
        self.assertIn("del_datasets", action)

    def test_generate_job_id(self) -> None:
        """Test job ID generation is deterministic."""
        # Test 1: Hash-based job_id when no log_file_infix is set
        job1 = MagicMock()
        job1.params.src.basis_root_dataset = "tank1/foo"
        job1.params.dst.basis_root_dataset = "tank2/bar"
        job1.params.src.ssh_host = "host1"
        job1.params.dst.ssh_host = "host2"
        job1.params.args.recursive = True
        job1.params.args.log_file_infix = ""

        job2 = MagicMock()
        job2.params.src.basis_root_dataset = "tank1/foo"
        job2.params.dst.basis_root_dataset = "tank2/bar"
        job2.params.src.ssh_host = "host1"
        job2.params.dst.ssh_host = "host2"
        job2.params.args.recursive = True
        job2.params.args.log_file_infix = ""

        # Same configuration should yield same job_id
        job_id1 = _generate_job_id(job1)
        job_id2 = _generate_job_id(job2)
        self.assertEqual(job_id1, job_id2)
        self.assertEqual(len(job_id1), 12)  # Should be 12 characters (hash)

        # Different configuration should yield different job_id
        job2.params.src.basis_root_dataset = "tank1/different"
        job_id3 = _generate_job_id(job2)
        self.assertNotEqual(job_id1, job_id3)

        # Test 2: Explicit job_id from bzfs_jobrunner via log_file_infix
        job3 = MagicMock()
        job3.params.src.basis_root_dataset = "tank1/foo"
        job3.params.dst.basis_root_dataset = "tank2/bar"
        job3.params.src.ssh_host = "host1"
        job3.params.dst.ssh_host = "host2"
        job3.params.args.recursive = True
        job3.params.args.log_file_infix = "_myjobname_"  # From bzfs_jobrunner --job-id

        job_id4 = _generate_job_id(job3)
        self.assertEqual(job_id4, "myjobname")  # Should extract from infix

        # Test 3: Job_id with special characters gets sanitized
        job3.params.args.log_file_infix = "_my-job.name_"
        job_id5 = _generate_job_id(job3)
        self.assertEqual(job_id5, "my-job_name")  # Dots replaced with underscores

        # Test 4: Empty infix after stripping should fall back to hash
        job3.params.args.log_file_infix = "___"
        job_id6 = _generate_job_id(job3)
        self.assertEqual(len(job_id6), 12)  # Should be hash, not empty

    def test_write_prometheus_metrics_disabled(self) -> None:
        """Test that metrics writing is skipped when not configured."""
        job = MagicMock()
        job.params.prometheus_textfile_dir = None

        # Should return early without writing anything
        write_prometheus_metrics(job, exit_code=0, elapsed_nanos=1_000_000_000)
        # No assertions needed - just verify it doesn't raise

    def test_write_prometheus_metrics_success(self) -> None:
        """Test successful metrics writing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            job = MagicMock()
            job.params.prometheus_textfile_dir = tmpdir
            job.params.src.basis_root_dataset = "tank1/src"
            job.params.dst.basis_root_dataset = "tank2/dst"
            job.params.src.ssh_host = "srchost"
            job.params.dst.ssh_host = "dsthost"
            job.params.args.recursive = True
            job.params.args.log_file_infix = ""
            job.params.create_src_snapshots_config.is_enabled = False
            job.params.skip_replication = False
            job.params.delete_dst_snapshots = False
            job.params.delete_dst_datasets = False
            job.params.delete_empty_dst_datasets = False
            job.params.compare_snapshot_lists = False
            job.params.monitor_snapshots_config.is_enabled = False
            job.params.log.info = MagicMock()
            job.num_snapshots_found = 10
            job.num_snapshots_replicated = 8
            job.num_cache_hits = 5
            job.num_cache_misses = 3

            write_prometheus_metrics(
                job,
                exit_code=0,
                elapsed_nanos=5_000_000_000,  # 5 seconds
                sent_bytes=1024 * 1024 * 100,  # 100 MB
            )

            # Verify metrics file was created
            job_id = _generate_job_id(job)
            prom_file = os.path.join(tmpdir, f"bzfs_{job_id}.prom")
            self.assertTrue(os.path.exists(prom_file), f"Metrics file not found: {prom_file}")

            # Read and verify contents
            with open(prom_file, encoding="utf-8") as f:
                content = f.read()

            # Verify key metrics are present
            self.assertIn("bzfs_job_status", content)
            self.assertIn("bzfs_job_exit_code", content)
            self.assertIn("bzfs_job_duration_seconds", content)
            self.assertIn("bzfs_snapshots_found_total", content)
            self.assertIn("bzfs_snapshots_replicated_total", content)
            self.assertIn("bzfs_bytes_sent_total", content)
            self.assertIn("bzfs_throughput_bytes_per_second", content)
            self.assertIn("bzfs_cache_hits_total", content)
            self.assertIn("bzfs_cache_misses_total", content)

            # Verify label values
            self.assertIn('action="replicate"', content)
            self.assertIn('src_dataset="tank1/src"', content)
            self.assertIn('dst_dataset="tank2/dst"', content)
            self.assertIn('src_host="srchost"', content)
            self.assertIn('dst_host="dsthost"', content)

            # Verify metric values
            self.assertIn("bzfs_snapshots_found_total", content)
            self.assertIn(" 10 ", content)  # num_snapshots_found
            self.assertIn(" 8 ", content)  # num_snapshots_replicated

    def test_write_prometheus_metrics_failure(self) -> None:
        """Test metrics writing on failure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            job = MagicMock()
            job.params.prometheus_textfile_dir = tmpdir
            job.params.src.basis_root_dataset = "tank1/src"
            job.params.dst.basis_root_dataset = "tank2/dst"
            job.params.src.ssh_host = "srchost"
            job.params.dst.ssh_host = "dsthost"
            job.params.args.recursive = False
            job.params.args.log_file_infix = "_daily_"
            job.params.create_src_snapshots_config.is_enabled = True
            job.params.skip_replication = True
            job.params.delete_dst_snapshots = False
            job.params.delete_dst_datasets = False
            job.params.delete_empty_dst_datasets = False
            job.params.compare_snapshot_lists = False
            job.params.monitor_snapshots_config.is_enabled = False
            job.params.log.info = MagicMock()
            job.num_snapshots_found = 0
            job.num_snapshots_replicated = 0

            write_prometheus_metrics(
                job,
                exit_code=1,  # Failure
                elapsed_nanos=1_000_000_000,
            )

            # Verify metrics file was created
            job_id = _generate_job_id(job)
            prom_file = os.path.join(tmpdir, f"bzfs_{job_id}.prom")
            self.assertTrue(os.path.exists(prom_file))

            with open(prom_file, encoding="utf-8") as f:
                content = f.read()

            # Verify exit code is 1 (failure)
            self.assertIn("bzfs_job_exit_code", content)
            lines = [line for line in content.split("\n") if "bzfs_job_exit_code{" in line]
            self.assertTrue(any(" 1 " in line for line in lines))


if __name__ == "__main__":
    unittest.main()
