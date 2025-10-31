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
"""Helpers for running CLI commands in (sequential or parallel) batches, without exceeding operating system limits.

The batch size aka max_batch_items splits one CLI command into one or more CLI commands. The resulting commands are executed
sequentially (via functions *_batched()), or in parallel across max_workers threads (via functions *_parallel()).

The degree of parallelism (max_workers) is specified by the job (via --threads).
Batch size is a trade-off between resource consumption, latency, bandwidth and throughput.

Example:
--------

- max_batch_items=1 (seq or par):
```
zfs list -t snapshot d1
zfs list -t snapshot d2
zfs list -t snapshot d3
zfs list -t snapshot d4
```

- max_batch_items=2 (seq or par):
```
zfs list -t snapshot d1 d2
zfs list -t snapshot d3 d4

- max_batch_items=N (seq or par):
```
zfs list -t snapshot d1 d2 d3 d4
```
"""

from __future__ import (
    annotations,
)
import sys
from collections.abc import (
    Iterable,
    Iterator,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    TypeVar,
)

from bzfs_main.connection import (
    SHARED,
    ConnectionPool,
    try_ssh_command,
)
from bzfs_main.parallel_iterator import (
    batch_cmd_iterator,
    get_max_command_line_bytes,
    parallel_iterator,
)
from bzfs_main.utils import (
    LOG_TRACE,
    drain,
)

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from bzfs_main.bzfs import (
        Job,
    )
    from bzfs_main.configuration import (
        Remote,
    )

T = TypeVar("T")


def run_ssh_cmd_batched(
    job: Job,
    r: Remote,
    cmd: list[str],
    cmd_args: Iterable[str],
    fn: Callable[[list[str]], Any],
    max_batch_items: int = 2**29,
    sep: str = " ",
) -> None:
    """Runs ssh command for each sequential batch of args, without creating a cmdline that's too big for the OS to handle."""
    drain(itr_ssh_cmd_batched(job, r, cmd, cmd_args, fn, max_batch_items=max_batch_items, sep=sep))


def itr_ssh_cmd_batched(
    job: Job,
    r: Remote,
    cmd: list[str],
    cmd_args: Iterable[str],
    fn: Callable[[list[str]], T],
    max_batch_items: int = 2**29,
    sep: str = " ",
) -> Iterator[T]:
    """Runs fn(cmd_args) in sequential batches w/ cmd, without creating a cmdline that's too big for the OS to handle."""
    max_bytes: int = _max_batch_bytes(job, r, cmd, sep)
    return batch_cmd_iterator(cmd_args=cmd_args, fn=fn, max_batch_items=max_batch_items, max_batch_bytes=max_bytes, sep=sep)


def run_ssh_cmd_parallel(
    job: Job,
    r: Remote,
    cmd_args_list: Iterable[tuple[list[str], Iterable[str]]],
    fn: Callable[[list[str], list[str]], Any],
    max_batch_items: int = 2**29,
) -> None:
    """Runs multiple ssh commands in parallel, batching each set of args."""
    drain(itr_ssh_cmd_parallel(job, r, cmd_args_list, fn=fn, max_batch_items=max_batch_items, ordered=False))


def itr_ssh_cmd_parallel(
    job: Job,
    r: Remote,
    cmd_args_list: Iterable[tuple[list[str], Iterable[str]]],
    fn: Callable[[list[str], list[str]], T],
    max_batch_items: int = 2**29,
    ordered: bool = True,
) -> Iterator[T]:
    """Streams results from multiple parallel (batched) SSH commands.

    When ordered=True, preserves the order of the batches as provided by cmd_args_list (i.e. yields results in the same order
    as the input), not in completion order. When ordered=False, yields results as they complete for minimum latency.
    """
    return parallel_iterator(
        iterator_builder=lambda executr: (
            itr_ssh_cmd_batched(
                job, r, cmd, cmd_args, lambda batch, cmd=cmd: executr.submit(fn, cmd, batch), max_batch_items=max_batch_items  # type: ignore[misc]
            )
            for cmd, cmd_args in cmd_args_list
        ),
        max_workers=job.max_workers[r.location],
        ordered=ordered,
        termination_event=job.termination_event,
    )


def zfs_list_snapshots_in_parallel(
    job: Job, r: Remote, cmd: list[str], datasets: list[str], ordered: bool = True
) -> Iterator[list[str]]:
    """Runs 'zfs list -t snapshot' on multiple datasets at the same time.

    Implemented with a time and space efficient streaming algorithm; easily scales to millions of datasets and any number of
    snapshots. For local execution (no SSH leg), the minibatch size is divided by a factor of 8 relative to the number of
    workers to reflect reduced communication latency, which improves throughput. Attempts to use at least 8 datasets per
    remote cmd in order to amortize SSH latency.
    """
    max_workers: int = job.max_workers[r.location]
    max_batch_items: int = min(
        job.max_datasets_per_minibatch_on_list_snaps[r.location],
        max(
            len(datasets) // (max_workers if r.ssh_user_host else max_workers * 8),
            8 if r.ssh_user_host else 1,
        ),
    )
    return itr_ssh_cmd_parallel(
        job,
        r,
        [(cmd, datasets)],
        fn=lambda cmd, batch: (try_ssh_command(job, r, LOG_TRACE, cmd=cmd + batch) or "").splitlines(),
        max_batch_items=max_batch_items,
        ordered=ordered,
    )


def _max_batch_bytes(job: Job, r: Remote, cmd: list[str], sep: str) -> int:
    """Avoids creating a cmdline that's too big for the OS to handle.

    The calculation subtracts 'header_bytes', which accounts for the full SSH invocation (including control socket/options)
    plus the fixed subcommand prefix, so that the remaining budget is reserved exclusively for the batched arguments.
    """
    assert isinstance(sep, str)
    max_bytes: int = min(_get_max_command_line_bytes(job, "local"), _get_max_command_line_bytes(job, r.location))
    # Max size of a single argument is 128KB on Linux - https://lists.gnu.org/archive/html/bug-bash/2020-09/msg00095.html
    max_bytes = max_bytes if sep == " " else min(max_bytes, 128 * 1024 - 1)  # e.g. 'zfs destroy foo@s1,s2,...,sN'
    conn_pool: ConnectionPool = job.params.connection_pools[r.location].pool(SHARED)
    with conn_pool.connection() as conn:
        cmd = conn.ssh_cmd + cmd
    header_bytes: int = len(" ".join(cmd).encode(sys.getfilesystemencoding()))
    return max_bytes - header_bytes


def _get_max_command_line_bytes(job: Job, location: str, os_name: str | None = None) -> int:
    """Remote flavor of os.sysconf("SC_ARG_MAX") - size(os.environb) - safety margin"""
    os_name = os_name if os_name else job.params.available_programs[location].get("os")
    os_name = os_name if os_name else "n/a"
    max_bytes = get_max_command_line_bytes(os_name)
    if job.max_command_line_bytes is not None:
        return job.max_command_line_bytes  # for testing only
    else:
        return max_bytes
