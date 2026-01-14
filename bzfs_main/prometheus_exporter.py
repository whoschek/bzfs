# Copyright 2025 Wolfgang Hoschek AT mac DOT com
# Written in 2025-2026 by Orsiris de Jong - ozy AT netpower DOT com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Prometheus metrics exporter for bzfs; outputs metrics in text format for node_exporter textfile collector."""

from __future__ import (
    annotations,
)
from logging import Logger
import os
import tempfile
import time
from pathlib import (
    Path,
)
import re
import unicodedata
from typing import (
    TYPE_CHECKING,
    Final,
)

from bzfs_main.util.utils import (
    FILE_PERMISSIONS,
    sha256_hex,
)

from bzfs_main.progress_reporter import (
    count_num_bytes_transferred_by_zfs_send,
)

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from bzfs_main.bzfs import (
        Job,
    )


def write_prometheus_metrics(job: Job, exit_code: int, elapsed_nanos: int, sent_bytes: int = 0) -> None:
    """Write Prometheus metrics to a .prom file for node_exporter textfile collector.

    Metrics are written atomically by writing to a temporary file first, then renaming it.
    The metrics file is placed in the directory specified by --prometheus-textfile-dir if provided,
    otherwise this function does nothing.

    Args:
        job: The Job instance containing execution state and parameters.
        exit_code: The exit code of the bzfs run (0 for success, non-zero for failure).
        elapsed_nanos: Elapsed time in nanoseconds for the job execution.
        sent_bytes: Total bytes transferred via zfs send (0 if not available).
    """
    p = job.params
    if not hasattr(p, "prometheus_textfile_dir") or not p.prometheus_textfile_dir:
        return  # Feature not enabled

    if p.dry_run:
        p.log.info("Not writing prometheus metrics in dry-run mode")
        return

    textfile_dir: str = p.prometheus_textfile_dir
    os.makedirs(textfile_dir, mode=0o755, exist_ok=True)

    # Generate a unique job_id from the job configuration
    job_id: str = _generate_job_id(job)

    # Compute elapsed time in seconds
    elapsed_secs: float = elapsed_nanos / 1_000_000_000

    # Compute throughput if bytes were sent
    throughput_bytes_per_sec: float = sent_bytes / elapsed_secs if elapsed_secs > 0 and sent_bytes > 0 else 0

    # Extract source and destination info
    # The following variables are formatted as host:dataset, so we need to extract the dataset name only
    src_root: str = p.src.basis_root_dataset.split(":")[1] if ":" in p.src.basis_root_dataset else p.src.basis_root_dataset
    dst_root: str = p.dst.basis_root_dataset.split(":")[1] if ":" in p.dst.basis_root_dataset else p.dst.basis_root_dataset
    # Prefer using basis_ssh_host (the given by cli args) in order to preserve proper metric labels instead of resvoled hostnames
    src_host: str = p.src.ssh_host or p.src.basis_ssh_host or "localhost"
    dst_host: str = p.dst.ssh_host or p.dst.basis_ssh_host or "localhost"
    # Determine the primary action(s) being performed
    action: str = _determine_action(p)

    # Build metrics in Prometheus text format
    metrics: list[str] = []
    timestamp_ms: int = int(time.time() * 1000)  # milliseconds since epoch

    # Common labels for all metrics
    labels: str = (
        f'job_id="{_escape_label(job_id)}",'
        f'action="{_escape_label(action)}",'
        f'src_dataset="{_escape_label(src_root)}",'
        f'dst_dataset="{_escape_label(dst_root)}",'
        f'src_host="{_escape_label(src_host)}",'
        f'dst_host="{_escape_label(dst_host)}"'
    )

    # Add metrics with HELP and TYPE declarations
    metrics.append(
        "# HELP bzfs_job_status Indicates if the bzfs job completed successfully (0=success, anything else is the exit code)"
    )
    metrics.append("# TYPE bzfs_job_status gauge")
    metrics.append(f"bzfs_job_status{{{labels}}} {exit_code}")
    metrics.append("")

    metrics.append("# HELP bzfs_job_duration_seconds Duration of the bzfs job in seconds")
    metrics.append("# TYPE bzfs_job_duration_seconds gauge")
    metrics.append(f"bzfs_job_duration_seconds{{{labels}}} {elapsed_secs:.3f}")
    metrics.append("")

    metrics.append("# HELP bzfs_job_last_run_timestamp_seconds Unix timestamp when the bzfs job last ran")
    metrics.append("# TYPE bzfs_job_last_run_timestamp_seconds gauge")
    metrics.append(f"bzfs_job_last_run_timestamp_seconds{{{labels}}} {timestamp_ms / 1000:.3f}")
    metrics.append("")

    metrics.append("# HELP bzfs_snapshots_found_total Total number of snapshots found during the job")
    metrics.append("# TYPE bzfs_snapshots_found_total counter")
    metrics.append(f"bzfs_snapshots_found_total{{{labels}}} {job.num_snapshots_found}")
    metrics.append("")

    if action == "replicate":
        metrics.append("# HELP bzfs_snapshots_replicated_total Total number of snapshots replicated during the job")
        metrics.append("# TYPE bzfs_snapshots_replicated_total counter")
        metrics.append(f"bzfs_snapshots_replicated_total{{{labels}}} {job.num_snapshots_replicated}")
        metrics.append("")

        metrics.append("# HELP bzfs_bytes_sent_total Total bytes sent via zfs send")
        metrics.append("# TYPE bzfs_bytes_sent_total counter")
        metrics.append(f"bzfs_bytes_sent_total{{{labels}}} {sent_bytes}")
        metrics.append("")

        metrics.append("# HELP bzfs_throughput_bytes_per_second Average throughput in bytes per second")
        metrics.append("# TYPE bzfs_throughput_bytes_per_second gauge")
        metrics.append(f"bzfs_throughput_bytes_per_second{{{labels}}} {throughput_bytes_per_sec:.2f}")
        metrics.append("")

    if hasattr(job, "num_cache_hits") and hasattr(job, "num_cache_misses"):
        metrics.append("# HELP bzfs_cache_hits_total Total number of cache hits")
        metrics.append("# TYPE bzfs_cache_hits_total counter")
        metrics.append(f"bzfs_cache_hits_total{{{labels}}} {job.num_cache_hits}")
        metrics.append("")

        metrics.append("# HELP bzfs_cache_misses_total Total number of cache misses")
        metrics.append("# TYPE bzfs_cache_misses_total counter")
        metrics.append(f"bzfs_cache_misses_total{{{labels}}} {job.num_cache_misses}")
        metrics.append("")

    # Write metrics atomically to a .prom file per action so we don't overwrite other actions from same job when run from jobrunner
    # Our file needs to hold all the labels so we don't overwrite create/delete snapshot actions between src and dst
    prom_file_prefix = f"bzfs_{job_id}_{action}_{_sanitize_filename(src_host)}_{_sanitize_filename(dst_host)}_{_sanitize_filename(src_root)}_{_sanitize_filename(dst_root)}"
    prom_filename: str = f"{prom_file_prefix}.prom"
    prom_filepath: str = os.path.join(textfile_dir, prom_filename)

    try:
        # Write to a temporary file in the same directory
        fd, temp_path = tempfile.mkstemp(suffix=".prom.tmp", prefix=f"{prom_file_prefix}", dir=textfile_dir, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write("\n".join(metrics))
                f.write("\n")  # Final newline
            os.chmod(temp_path, FILE_PERMISSIONS)
            # Atomic rename
            os.replace(temp_path, prom_filepath)
            p.log.info("Prometheus metrics written to: %s", prom_filepath)
        except Exception:
            # Clean up temp file on error
            try:
                os.unlink(temp_path)
            except Exception:
                pass
            raise
    except Exception as e:
        p.log.warning("Failed to write Prometheus metrics: %s", e)


def write_prometheus_metrics_on_error(job: Job, exit_code: int, log: Logger) -> None:
    """Helper to write Prometheus metrics when an error occurs."""
    try:
        if hasattr(job, "params") and hasattr(job, "replication_start_time_nanos"):
            p = job.params
            if p.dry_run:
                log.info("Not writing prometheus file in dry-run mode")
                return
            elapsed_nanos = time.monotonic_ns() - job.replication_start_time_nanos
            sent_bytes = 0
            if p.is_program_available("pv", "local"):
                sent_bytes = count_num_bytes_transferred_by_zfs_send(p.log_params.pv_log_file)
            write_prometheus_metrics(job, exit_code=exit_code, elapsed_nanos=elapsed_nanos, sent_bytes=sent_bytes)
    except Exception as exc:
        log.error(
            f"Could not write prometheus metrics while an error occured in replication process. exit_code={exit_code}, error={exc}",
            exc_info=True,
        )


def _generate_job_id(job: Job) -> str:
    """Generate a unique, deterministic job ID based on the job configuration.

    The job ID is a hash of key configuration parameters that uniquely identify the job.
    This allows Prometheus to track the same job across multiple runs.

    If bzfs was invoked with an explicit job_id (via bzfs_jobrunner's --job-id argument),
    that will be extracted from log_file_infix and used instead of generating a hash.

    Args:
        job: The Job instance.

    Returns:
        A short hash string suitable for use as a job identifier.
    """
    p = job.params

    # Check if log_file_infix contains a job_id from bzfs_jobrunner
    # bzfs_jobrunner sets log_file_infix to something like "_jobid_" or "_myjobname_"
    log_infix = p.args.log_file_infix or ""
    if log_infix:
        # Extract job_id from infix by removing leading/trailing underscores
        extracted_id = log_infix.strip("_")
        # Only use if it looks like a valid job_id (non-empty, not just whitespace)
        if extracted_id and not extracted_id.isspace():
            # Sanitize to ensure it's safe for use in filenames
            safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in extracted_id)
            if safe_id:
                return safe_id[:64]  # Limit length to avoid overly long filenames

    # Fallback: Generate hash-based job_id from configuration
    key_parts: list[str] = [
        p.src.basis_root_dataset,
        p.dst.basis_root_dataset,
        p.src.ssh_host or "localhost",
        p.dst.ssh_host or "localhost",
        str(p.args.recursive),
        log_infix,
    ]
    key_str: str = "_".join(key_parts)
    # Use first 12 characters of hash for brevity
    return sha256_hex(key_str)[:12]


def _determine_action(p) -> str:
    """Determine the primary action(s) being performed by bzfs.

    This analyzes CLI parameters to identify what operation is being executed.
    Multiple actions may be combined with '+' if they occur together.
    Note that there is no delete_src_snapshots since it's just delete_dst_snapshots with src and dst reversed
    The same applies to create snapshots

    Args:
        p: The Params instance containing CLI arguments.

    Returns:
        A string describing the action(s): 'replicate', 'create_snapshots', 'delete_snapshots',
        'delete_datasets', 'compare', 'monitor', or combinations like 'create+replicate'.
    """
    actions: list[str] = []

    # Check for snapshot creation
    if not p.create_src_snapshots_config.skip_create_src_snapshots:
        actions.append("create_snapshots")

    # Check for replication (default unless explicitly skipped)
    if not p.skip_replication:
        actions.append("replicate")

    # Check for snapshot deletion
    if p.delete_dst_snapshots:
        actions.append("del_snapshots")

    # Check for dataset deletion
    if p.delete_dst_datasets or p.delete_empty_dst_datasets:
        actions.append("del_datasets")

    # Check for comparison mode
    if p.compare_snapshot_lists:
        actions.append("compare")

    # Check for monitoring mode
    if p.monitor_snapshots_config.enable_monitor_snapshots:
        actions.append("monitor")

    # If no specific actions detected, default to 'replicate' (the main bzfs operation)
    if not actions:
        return "replicate"

    # Join multiple actions with '+'
    return "+".join(actions)


def _escape_label(value: str) -> str:
    """Escape Prometheus label value according to text format spec.

    Backslashes, double quotes, and line feeds must be escaped.

    Args:
        value: The raw label value.

    Returns:
        The escaped label value.
    """
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _sanitize_filename(value: str) -> str:
    """Make sure a string can be used as filename

    Args:
        value: string.

    Returns:
        usable string for filename.
    """
    value = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    value = re.sub("[^\w\s-]", "", value).strip()
    return re.sub("[-\s]+", "-", value)
