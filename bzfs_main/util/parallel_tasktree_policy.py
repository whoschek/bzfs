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
#
"""Policy layer for the generic parallel task tree scheduling algorithm.

Purpose: Provide bzfs-specific behavior on top of the policy-free generic ``parallel_tasktree`` scheduling algorithm:
retries, skip-on-error modes (fail/dataset/tree), and logging.

Assumptions: Callers provide a thread-safe ``process_dataset(dataset, tid, Retry) -> bool`` callback. Dataset list is sorted
and contains no duplicate entries (enforced by tests).

Design rationale: Keep scheduling generic and reusable while concentrating error handling and side-effects here. This module
exposes a stable API for callers like ``bzfs`` and ``bzfs_jobrunner``.
"""

from __future__ import (
    annotations,
)
import logging
import os
import subprocess
import threading
import time
from concurrent.futures import (
    Future,
)
from typing import (
    Callable,
)

from bzfs_main.util.parallel_tasktree import (
    CompletionCallback,
    CompletionCallbackResult,
    ParallelTaskTree,
)
from bzfs_main.util.retry import (
    Retry,
    RetryPolicy,
    RetryTemplate,
)
from bzfs_main.util.utils import (
    dry,
    human_readable_duration,
)


def process_datasets_in_parallel_and_fault_tolerant(
    *,
    log: logging.Logger,
    datasets: list[str],  # (sorted) list of datasets to process
    process_dataset: Callable[
        [str, str, Retry], bool  # lambda: dataset, tid, Retry; return False to skip subtree; must be thread-safe
    ],
    skip_tree_on_error: Callable[[str], bool],  # lambda: dataset # called on error; return True to skip subtree on error
    skip_on_error: str = "fail",
    max_workers: int = os.cpu_count() or 1,
    interval_nanos: Callable[
        [int, str, int], int
    ] = lambda last_update_nanos, dataset, submit_count: 0,  # optionally spread tasks out over time; e.g. for jitter
    termination_event: threading.Event | None = None,  # optional event to request early async termination
    termination_handler: Callable[[], None] = lambda: None,
    task_name: str = "Task",
    enable_barriers: bool | None = None,  # for testing only; None means 'auto-detect'
    append_exception: Callable[[BaseException, str, str], None] = lambda ex, task, dataset: None,  # called on nonfatal error
    retry_template: RetryTemplate[bool] = RetryTemplate[bool]().copy(policy=RetryPolicy.no_retries()),  # noqa: B008
    dry_run: bool = False,
    is_test_mode: bool = False,
) -> bool:  # returns True if any dataset processing failed, False if all succeeded
    """Runs datasets in parallel with retries and skip policy.

    Purpose: Adapt the generic engine to bzfs needs by wrapping the worker function with retries and determining skip/fail
    behavior on completion.

    Assumptions: ``skip_on_error`` is one of {"fail","dataset","tree"}. ``skip_tree_on_error(dataset)`` returns True if
    subtree should be skipped.

    Design rationale: The completion callback runs in the main thread, enabling safe cancellation of in-flight futures for
    fail-fast while keeping worker threads free of policy decisions.
    """
    assert callable(process_dataset)
    assert callable(skip_tree_on_error)
    termination_event = threading.Event() if termination_event is None else termination_event
    assert "%" not in task_name
    assert callable(append_exception)
    len_datasets: int = len(datasets)
    is_debug: bool = log.isEnabledFor(logging.DEBUG)

    def _process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
        """Wrapper around process_dataset(); adds retries plus a callback determining if to fail or skip subtree on error."""
        tid: str = f"{submit_count}/{len_datasets}"
        start_time_nanos: int = time.monotonic_ns()
        exception = None
        no_skip: bool = False
        try:
            no_skip = retry_template.call_with_retries(
                fn=lambda retry: process_dataset(dataset, tid, retry),
                log=log,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, SystemExit, UnicodeDecodeError) as e:
            exception = e  # may be reraised later
        finally:
            if is_debug:
                elapsed_duration: str = human_readable_duration(time.monotonic_ns() - start_time_nanos)
                log.debug(dry(f"{tid} {task_name} done: %s took %s", dry_run), dataset, elapsed_duration)

        def _completion_callback(todo_futures: set[Future[CompletionCallback]]) -> CompletionCallbackResult:
            """CompletionCallback determining if to fail or skip subtree on error; Runs in the (single) main thread as part
            of the coordination loop."""
            nonlocal no_skip
            fail: bool = False
            if exception is not None:
                fail = True
                if skip_on_error == "fail" or termination_event.is_set():
                    for todo_future in todo_futures:
                        todo_future.cancel()
                    termination_handler()
                    raise exception
                no_skip = not (skip_on_error == "tree" or skip_tree_on_error(dataset))
                log.error("%s", exception)
                append_exception(exception, task_name, dataset)
            return CompletionCallbackResult(no_skip=no_skip, fail=fail)

        return _completion_callback

    tasktree: ParallelTaskTree = ParallelTaskTree(
        log=log,
        datasets=datasets,
        process_dataset=_process_dataset,
        max_workers=max_workers,
        interval_nanos=interval_nanos,
        termination_event=termination_event,
        enable_barriers=enable_barriers,
        is_test_mode=is_test_mode,
    )
    return tasktree.process_datasets_in_parallel()
