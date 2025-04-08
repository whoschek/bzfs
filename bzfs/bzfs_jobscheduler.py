#!/usr/bin/env python3
#
# Copyright 2024 Wolfgang Hoschek AT mac DOT com
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

# /// script
# requires-python = ">=3.8"
# dependencies = []
# ///

"""WARNING: For now, `bzfs_jobscheduler` is work-in-progress, and as such may still change in incompatible ways."""

import argparse
import ast
import contextlib
import importlib.machinery
import importlib.util
import os
import shutil
import subprocess
import sys
import threading
import time
import types
from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from typing import List, Optional

prog_name = "bzfs_jobscheduler"


def argument_parser() -> argparse.ArgumentParser:
    # fmt: off
    parser = argparse.ArgumentParser(
        prog=prog_name,
        allow_abbrev=False,
        description=f""" 
WARNING: For now, `bzfs_jobscheduler` is work-in-progress, and as such may still change in incompatible ways.

This program simplifies the deployment of an efficient geo-replicated backup service where each of the N destination hosts
is located in a separate geographic region and pulls replicas from (the same set of) M source hosts, using the same shared
[multisrc jobconfig](bzfs_tests/bzfs_job_example_multisrc.py) script.
The number of source hosts can be large. N=2 or N=3 are typical geo-replication factors.

This scheduler program is a light-weight convenience wrapper around [bzfs_jobrunner](README_bzfs_jobrunner.md) that
simplifies the reliable and efficient scheduling of a variable number of independent worker jobs. A failure of a worker job
does not affect the operation of the other worker jobs. The scheduler is called by a
[multisrc jobconfig](bzfs_tests/bzfs_job_example_multisrc.py) script.

`stdin` must contain a list of zero or more CLI commands, where each command is a list of one or more strings.

This scheduler program submits each command as a job to `bzfs_jobrunner`, which in turn delegates most of the actual work
to the `bzfs` CLI. Uses an "Infrastructure as Code" approach.
""", formatter_class=argparse.RawTextHelpFormatter)

    workers_default = 100  # percent
    parser.add_argument(
        "--workers", min=1, default=(workers_default, True), action=bzfs.CheckPercentRange, metavar="INT[%]",
        help="The maximum number of jobs to run in parallel at any time; can be given as a positive integer, "
             f"optionally followed by the %% percent character (min: 1, default: {workers_default}%%). Percentages "
             "are relative to the number of CPU cores on the machine. Example: 200%% uses twice as many parallel jobs as "
             "there are cores on the machine; 75%% uses num_procs = num_cores * 0.75. Examples: 1, 4, 75%%, 150%%\n\n")
    parser.add_argument(
        "--work-period-seconds", type=float, min=0, default=0, action=bzfs.CheckRange, metavar="FLOAT",
        help="Reduces bandwidth spikes by evenly spreading the start of worker jobs over this much time; "
             "0 disables this feature (default: 0). Examples: 0, 60, 86400\n\n")
    parser.add_argument(
        "--worker-timeout-seconds", type=float, min=0, default=None, action=bzfs.CheckRange, metavar="FLOAT",
        help="If this much time has passed after a worker process has started executing, kill the straggling worker "
             "(optional). Other workers remain unaffected. Examples: 60, 3600\n\n")
    return parser
    # fmt: on


def load_module(progname: str) -> types.ModuleType:
    prog_path = shutil.which(progname)
    assert prog_path, f"{progname}: command not found on PATH"
    prog_path = os.path.realpath(prog_path)  # resolve symlink, if any
    loader = importlib.machinery.SourceFileLoader(progname, prog_path)
    spec = importlib.util.spec_from_loader(progname, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    if hasattr(module, "run_main"):
        return module
    else:  # It's a wrapper script as `bzfs` was installed as a package by 'pip install'; load that installed package
        return importlib.import_module(f"{progname}.{progname}")


# constants:
bzfs: types.ModuleType = load_module("bzfs")


def main():
    sys.exit(Job().run_main(sys.argv))


#############################################################################
class Job:
    def __init__(self, log: Optional[Logger] = None):
        self.log: Logger = log if log is not None else bzfs.get_simple_logger(prog_name)
        self.stats: Stats = Stats()

    def run_main(self, sys_argv: List[str]) -> int:
        self.log.info(
            "WARNING: For now, `bzfs_jobscheduler` is work-in-progress, and as such may still change in incompatible ways."
        )
        self.log.info("CLI arguments: %s", " ".join(sys_argv))
        args = argument_parser().parse_args(sys_argv[1:])
        commands = validate_commands(ast.literal_eval(sys.stdin.read()))  # passing via stdin enables large number of cmds
        workers, workers_is_percent = args.workers
        max_workers = max(1, round(os.cpu_count() * workers / 100.0) if workers_is_percent else round(workers))
        update_interval_nanos = round(1_000_000_000 * max(0, args.work_period_seconds) / max(1, len(commands)))
        self.stats = Stats()
        self.stats.jobs_all = len(commands)
        futures = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:  # waits for completion of all jobs on exit of "with"
            next_update_nanos = time.monotonic_ns()
            for cmd in commands:
                time.sleep(max(0, next_update_nanos - time.monotonic_ns()) / 1_000_000_000)
                next_update_nanos += update_interval_nanos
                # enqueue request to (eventually) execute run_worker_job_process()
                futures.append(executor.submit(self.run_worker_job_process, cmd, args.worker_timeout_seconds))
        fails = sum(1 for future in futures if future.result() != 0)
        return min(101, fails)  # exit code is number of failed workers, same as the default behavior of GNU `parallels` CLI

    def run_worker_job_process(self, cmd: List[str], timeout_secs: Optional[float]) -> Optional[int]:
        returncode = None
        start_time_nanos = time.monotonic_ns()
        stats = self.stats
        cmd_str = " ".join(cmd)
        try:
            self.log.debug("Starting worker job: %s", cmd_str)
            with stats.lock:
                stats.jobs_started += 1
                stats.jobs_running += 1
                msg = str(stats)
            self.log.info("Progress: %s", msg)
            start_time_nanos = time.monotonic_ns()

            proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, text=True)  # run job in a separate subprocess
            try:
                proc.communicate(timeout=timeout_secs)  # Wait for the subprocess to exit
            except subprocess.TimeoutExpired:
                self.log.error("%s", f"Terminating worker job as it failed to complete within {timeout_secs}s: {cmd_str}")
                proc.terminate()  # Sends SIGTERM signal to job subprocess
                timeout_secs = min(1, timeout_secs)
                try:
                    proc.communicate(timeout=timeout_secs)  # Wait for the subprocess to exit
                except subprocess.TimeoutExpired:
                    self.log.error("%s", f"Killing worker job as it failed to terminate within {timeout_secs}s: {cmd_str}")
                    proc.kill()  # Sends SIGKILL signal to job subprocess because SIGTERM wasn't enough
                    timeout_secs = min(0.025, timeout_secs)
                    with contextlib.suppress(subprocess.TimeoutExpired):
                        proc.communicate(timeout=timeout_secs)  # Wait for the subprocess to exit
            finally:
                returncode = proc.returncode
        except BaseException as e:
            self.log.error("Worker job failed with unexpected exception: %s for command: %s", e, cmd_str)
            raise e
        else:
            elapsed_nanos = time.monotonic_ns() - start_time_nanos
            elapsed_human = bzfs.human_readable_duration(elapsed_nanos, separator="")
            if returncode != 0:
                self.log.error("Worker job failed with exit code %s in %s: %s", returncode, elapsed_human, cmd_str)
            else:
                self.log.info("Worker job succeeded in %s: %s", elapsed_human, cmd_str)
        finally:
            elapsed_nanos = time.monotonic_ns() - start_time_nanos
            with stats.lock:
                stats.jobs_running -= 1
                stats.jobs_completed += 1
                stats.sum_elapsed_nanos += elapsed_nanos
                stats.jobs_failed += 1 if returncode != 0 else 0
                msg = str(stats)
            self.log.info("Progress: %s", msg)
        return returncode


#############################################################################
class Stats:
    def __init__(self):
        self.lock: threading.Lock = threading.Lock()
        self.jobs_all: int = 0
        self.jobs_started: int = 0
        self.jobs_running: int = 0
        self.jobs_completed: int = 0
        self.jobs_failed: int = 0
        self.sum_elapsed_nanos: int = 0

    def __repr__(self) -> str:
        def pct(number: int) -> str:
            return f"{number}={bzfs.human_readable_float(100 * number / self.jobs_all)}%"

        all, started, running, completed = self.jobs_all, self.jobs_started, self.jobs_running, self.jobs_completed
        failed = self.jobs_failed
        t = "avg_completion_time:" + bzfs.human_readable_duration(self.sum_elapsed_nanos / max(1, completed), separator="")
        return f"all:{all}, running:{running}, started:{pct(started)}, completed:{pct(completed)}, failed:{pct(failed)}, {t}"


#############################################################################
def validate_commands(commands: List[str]) -> List[str]:
    assert isinstance(commands, list)
    for cmd in commands:
        assert isinstance(cmd, list)
        assert all(isinstance(arg, str) for arg in cmd)
    return commands


#############################################################################
if __name__ == "__main__":
    main()
