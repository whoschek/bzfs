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
"""Unit tests for replication.py utilities without requiring the zfs CLI."""

from __future__ import (
    annotations,
)
import shlex
import subprocess
import threading
import unittest
from collections import (
    defaultdict,
)
from typing import (
    TYPE_CHECKING,
    Callable,
)
from unittest.mock import (
    MagicMock,
    call,
    patch,
)

from bzfs_main.argparse_actions import (
    SnapshotFilter,
)
from bzfs_main.filter import (
    SNAPSHOT_REGEX_FILTER_NAME,
)
from bzfs_main.replication import (
    _check_zfs_dataset_busy,
    _clear_resumable_recv_state_if_necessary,
    _compress_cmd,
    _create_zfs_filesystem,
    _decompress_cmd,
    _delete_snapshot_cmd,
    _estimate_send_size,
    _estimate_send_sizes_in_parallel,
    _format_size,
    _incremental_send_steps_wrapper,
    _is_zfs_dataset_busy,
    _mbuffer_cmd,
    _prepare_zfs_send_receive,
    _pv_cmd,
    _recv_resume_token,
    _zfs_get,
    _zfs_set,
    replicate_dataset,
)
from bzfs_main.util.retry import (
    Retry,
    RetryableError,
    RetryConfig,
    RetryPolicy,
)
from bzfs_main.util.utils import (
    LOG_DEBUG,
    SynchronizedBool,
    compile_regexes,
)
from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)

if TYPE_CHECKING:  # type-only imports for annotations
    from bzfs_main.bzfs import (
        Job,
    )
    from bzfs_main.configuration import (
        Params,
        Remote,
    )


###############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestQuoting,
        TestResumeErrorParsing,
        TestReplication,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


###############################################################################
def _make_params(**kwargs: object) -> MagicMock:
    """Creates a Params mock with split_args helper."""
    params = MagicMock()
    params.split_args.side_effect = lambda *parts: shlex.split(" ".join(str(p) for p in parts if p))
    for key, value in kwargs.items():
        setattr(params, key, value)
    return params


def _make_job(**params_kwargs: object) -> MagicMock:
    """Returns a Job mock with its Params configured for testing."""
    job = MagicMock()
    job.params = _make_params(**params_kwargs)
    job.max_workers = {"src": 1, "dst": 1, "local": 1}
    job.termination_event = threading.Event()
    return job


def _prepare_job(
    src_host: str = "",
    dst_host: str = "",
    is_program_available: Callable[[str, str], bool] | None = None,
) -> MagicMock:
    """Creates a Job mock with hosts and program availability for pipeline tests."""
    if is_program_available is None:

        def is_program_available(_p: str, _l: str) -> bool:
            return True

    job = _make_job(
        src=MagicMock(ssh_user_host=src_host),
        dst=MagicMock(ssh_user_host=dst_host),
        shell_program="sh",
        is_program_available=MagicMock(side_effect=is_program_available),
    )
    job.src_properties = {"pool/ds": MagicMock(recordsize=1)}
    job.inject_params = {}
    return job


###############################################################################
class TestQuoting(AbstractTestCase):
    """Covers command builders and safety helpers in replication."""

    def test_prepare_zfs_send_never_passes_trailing_backslash_to_dquote(self) -> None:
        """Purpose: For security, verify `_prepare_zfs_send_receive` never passes a string ending with a bare backslash to
        `dquote`.

        Assumptions: Pipelines are built via shlex.join and shlex.quote. Program opts and the pv log path may include
        backslashes.

        Design Rationale: Patch stage builders to deterministic minimal commands and patch `_dquote` to capture inputs;
        use a nested helper (with `+=`) instead of a lambda for mypy-friendly mutation and clarity.
        """
        # both legs remote so _dquote is exercised for src and dst
        job = _prepare_job(src_host="src", dst_host="dst", is_program_available=lambda _p, _l: True)
        # make sure options end with a backslash
        job.params.pv_program = "pv"
        job.params.pv_program_opts = ["Y\\"]
        job.params.mbuffer_program = "mbuffer"
        job.params.mbuffer_program_opts = ["X\\"]
        job.params.compression_program = "zstd"
        job.params.compression_program_opts = ["Z\\"]
        job.params.log_params = MagicMock(pv_log_file="/tmp/pv.log\\")  # pv log path ending in backslash

        seen: list[str] = []
        with (
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch(  # force mbuffer to be present on all legs and include our trailing-backslash opt
                "bzfs_main.replication._mbuffer_cmd",
                side_effect=lambda p, loc, _sz, _rec: shlex.join([p.mbuffer_program, "-s", "1"] + p.mbuffer_program_opts),
            ),
            patch(  # force pv to be present and include our backslashy opts + backslashy log path
                "bzfs_main.replication._pv_cmd",
                side_effect=lambda j, loc, _sz, _human, disable_progress_bar=False: f"LC_ALL=C {shlex.join([j.params.pv_program] + j.params.pv_program_opts)} 2>> {shlex.quote(j.params.log_params.pv_log_file)}",
            ),
            patch(  # keep squote a no-op so we see the raw strings
                "bzfs_main.replication.squote", side_effect=lambda _r, s: s
            ),
            patch(  # capture the exact strings sent to dquote
                "bzfs_main.replication.dquote", side_effect=lambda s: (seen.__iadd__([s]), s)[1]
            ),
        ):
            _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")

        self.assertTrue(seen)  # we should have seen src and dst
        self.assertTrue(all(not s.endswith("\\") for s in seen), f"_dquote saw trailing backslash: {seen!r}")


###############################################################################
class TestResumeErrorParsing(AbstractTestCase):
    """Covers parsing of multi-line stderr for resumable receive recovery."""

    def test_clear_resumable_on_incremental_source_missing_with_prefix(self) -> None:
        """Ensures multi-line stderr still triggers clearing of resumable recv state.

        In the future, some platforms may prepend extra lines before the actual error. The parser must not rely on the
        message being on the first line and should match anywhere in stderr.
        """
        job = _make_job(
            zfs_program="zfs",
            dry_run=False,
            dst=MagicMock(sudo="sudo"),
            log=MagicMock(),
        )
        stderr = (
            "random prefix line A\n"
            "random prefix line B\n"
            "cannot resume send: incremental source 0xa0b1c2d3 no longer exists\n"
            "random suffix line\n"
        )
        with patch.object(job, "try_ssh_command") as mock_try_ssh_command:
            cleared: bool = _clear_resumable_recv_state_if_necessary(job, "pool/ds", stderr)
        self.assertTrue(cleared)
        self.assertTrue(mock_try_ssh_command.called)


###############################################################################
class TestReplication(AbstractTestCase):
    """Covers command builders and safety helpers in replication."""

    def test_format_size(self) -> None:
        self.assertEqual("0B".rjust(7), _format_size(0))
        self.assertEqual("1MiB".rjust(7), _format_size(1048576))

    def test_mbuffer_cmd_no_mbuffer(self) -> None:
        p = _make_params(
            min_pipe_transfer_size=1,
            src=MagicMock(is_nonlocal=True),
            dst=MagicMock(is_nonlocal=False),
            is_program_available=MagicMock(return_value=False),
            mbuffer_program="mbuffer",
            mbuffer_program_opts=["-O", "localhost:0"],
            no_estimate_send_size=False,
        )
        self.assertEqual("cat", _mbuffer_cmd(p, "src", 2, 1024))

    def test_mbuffer_cmd_uses_mbuffer(self) -> None:
        p = _make_params(
            min_pipe_transfer_size=1,
            src=MagicMock(is_nonlocal=True),
            dst=MagicMock(is_nonlocal=False),
            is_program_available=MagicMock(return_value=True),
            mbuffer_program="mbuffer",
            mbuffer_program_opts=["-O", "localhost:0"],
            no_estimate_send_size=False,
        )
        self.assertEqual("mbuffer -s 2097152 -O localhost:0", _mbuffer_cmd(p, "src", 2, 4096))

    def test_compress_cmd_returns_cat(self) -> None:
        p = _make_params(
            min_pipe_transfer_size=10,
            src=MagicMock(is_nonlocal=False),
            dst=MagicMock(is_nonlocal=False),
            is_program_available=MagicMock(return_value=False),
            compression_program="zstd",
            compression_program_opts=["-3"],
            no_estimate_send_size=False,
        )
        self.assertEqual("cat", _compress_cmd(p, "src", 20))

    def test_compress_cmd_uses_zstd(self) -> None:
        p = _make_params(
            min_pipe_transfer_size=1,
            src=MagicMock(is_nonlocal=True),
            dst=MagicMock(is_nonlocal=False),
            is_program_available=MagicMock(return_value=True),
            compression_program="zstd",
            compression_program_opts=["-3"],
            no_estimate_send_size=False,
        )
        self.assertEqual("zstd -c -3", _compress_cmd(p, "src", 20))

    def test_decompress_cmd_returns_cat(self) -> None:
        p = _make_params(
            min_pipe_transfer_size=10,
            src=MagicMock(is_nonlocal=False),
            dst=MagicMock(is_nonlocal=False),
            is_program_available=MagicMock(return_value=False),
            compression_program="zstd",
            no_estimate_send_size=False,
        )
        self.assertEqual("cat", _decompress_cmd(p, "src", 20))

    def test_decompress_cmd_uses_zstd(self) -> None:
        p = _make_params(
            min_pipe_transfer_size=1,
            src=MagicMock(is_nonlocal=True),
            dst=MagicMock(is_nonlocal=False),
            is_program_available=MagicMock(return_value=True),
            compression_program="zstd",
            no_estimate_send_size=False,
        )
        self.assertEqual("zstd -dc", _decompress_cmd(p, "src", 20))

    def test_pv_cmd_builds_command(self) -> None:
        log_params = MagicMock(pv_log_file="/tmp/pv.log")
        p = _make_params(
            is_program_available=MagicMock(return_value=True),
            pv_program="pv",
            pv_program_opts=["-L1"],
            log_params=log_params,
            quiet=True,
            no_estimate_send_size=False,
        )
        job = MagicMock(
            params=p,
            is_first_replication_task=SynchronizedBool(True),
            isatty=False,
            progress_reporter=MagicMock(),
            progress_update_intervals=None,
        )
        result = _pv_cmd(job, "src", 1048576, "1MB")
        self.assertEqual("LC_ALL=C pv -L1 --force --name=1MB --size=1048576 2>> /tmp/pv.log", result)

    def test_pv_cmd_returns_cat_when_missing(self) -> None:
        p = _make_params(is_program_available=MagicMock(return_value=False), no_estimate_send_size=False)
        job = MagicMock(params=p)
        self.assertEqual("cat", _pv_cmd(job, "src", 1, "1B"))

    def test_delete_snapshot_cmd(self) -> None:
        p = _make_params(
            zfs_program="zfs",
            force_hard="-f",
            verbose_destroy="-v",
            dry_run_destroy="-n",
        )
        remote = MagicMock(sudo="sudo")
        cmd = _delete_snapshot_cmd(p, remote, "pool/ds@s1,pool/ds@s2")
        self.assertListEqual(
            ["sudo", "zfs", "destroy", "-f", "-v", "-n", "pool/ds@s1,pool/ds@s2"],
            cmd,
        )

    def test_is_zfs_dataset_busy(self) -> None:
        procs = [
            "sudo -n zfs send pool/ds@s1",
            "sudo -n zfs receive pool/ds",
        ]
        self.assertTrue(_is_zfs_dataset_busy(procs, "pool/ds", True))
        self.assertTrue(_is_zfs_dataset_busy(procs, "pool/ds", False))
        self.assertFalse(_is_zfs_dataset_busy(["other"], "pool/ds", True))

    def test_check_zfs_dataset_busy(self) -> None:
        log = MagicMock()
        p = _make_params(
            is_program_available=MagicMock(return_value=True),
            ps_program="ps",
            log=log,
        )
        job = MagicMock(
            params=p,
            inject_params={},
        )
        with patch.object(job, "try_ssh_command", new=MagicMock(return_value="")):
            remote = MagicMock(location="dst")
            self.assertTrue(_check_zfs_dataset_busy(job, remote, "pool/ds"))
            job.inject_params["is_zfs_dataset_busy"] = True
            with self.assertRaises(RetryableError):
                _check_zfs_dataset_busy(job, remote, "pool/ds")

    def test_recv_resume_token_disabled(self) -> None:
        job = _make_job(resume_recv=False, log=MagicMock())
        job.dst_dataset_exists = {"pool/ds": True}
        token, send_opts, recv_opts = _recv_resume_token(job, "pool/ds", 0)
        self.assertIsNone(token)
        self.assertEqual([], send_opts)
        self.assertEqual([], recv_opts)

    @patch("bzfs_main.replication.is_zpool_feature_enabled_or_active", return_value=False)
    def test_recv_resume_token_feature_disabled(self, _feat: MagicMock) -> None:
        log = MagicMock()
        job = _make_job(resume_recv=True, log=log)
        job.dst_dataset_exists = {"pool/ds": True}
        token, send_opts, recv_opts = _recv_resume_token(job, "pool/ds", 0)
        self.assertIsNone(token)
        self.assertEqual([], send_opts)
        self.assertEqual([], recv_opts)

    @patch("bzfs_main.replication.is_zpool_feature_enabled_or_active", return_value=True)
    def test_recv_resume_token_reads_value(self, _feat: MagicMock) -> None:
        job = _make_job(
            resume_recv=True,
            zfs_program="zfs",
            dry_run=False,
            verbose_zfs=False,
            dst=MagicMock(),
            log=MagicMock(),
            is_program_available=MagicMock(return_value=True),
        )
        job.dst_dataset_exists = {"pool/ds": True}
        with patch.object(job, "run_ssh_command", return_value="tok\n") as mock_run_ssh_command:
            token, send_opts, recv_opts = _recv_resume_token(job, "pool/ds", 0)
        self.assertEqual("tok", token)
        self.assertEqual(["-t", "tok"], send_opts)
        self.assertEqual(["-s"], recv_opts)
        mock_run_ssh_command.assert_called_once()

    def test_create_zfs_filesystem_creates_missing(self) -> None:
        job = _make_job(
            zfs_program="zfs",
            dst=MagicMock(sudo="sudo"),
            dry_run=False,
            is_program_available=MagicMock(return_value=True),
        )
        job.dst_dataset_exists = defaultdict(lambda: False, {"a": True})
        with patch.object(job, "run_ssh_command") as mock_run_ssh_command:
            _create_zfs_filesystem(job, "a/b/c")
        expected = [
            ["sudo", "zfs", "create", "-p", "-u", "a/b"],
            ["sudo", "zfs", "create", "-p", "-u", "a/b/c"],
        ]
        mock_run_ssh_command.assert_has_calls(
            [
                call(job.params.dst, LOG_DEBUG, is_dry=False, print_stdout=True, cmd=expected[0]),
                call(job.params.dst, LOG_DEBUG, is_dry=False, print_stdout=True, cmd=expected[1]),
            ]
        )

    def test_estimate_send_size_parses_output(self) -> None:
        job = _make_job(
            no_estimate_send_size=False,
            zfs_program="zfs",
            dry_run=False,
            curr_zfs_send_program_opts=["-P"],
        )
        remote = MagicMock(sudo="sudo")
        with patch.object(job, "try_ssh_command", return_value="size\t123\n") as mock_try_ssh_command:
            size = _estimate_send_size(job, remote, "pool/ds", None, "src@snap")
        self.assertEqual(123, size)
        mock_try_ssh_command.assert_called_once()

    @patch("bzfs_main.replication._clear_resumable_recv_state_if_necessary", return_value=True)
    def test_estimate_send_size_retryable_error(self, clear: MagicMock) -> None:
        cp_error = subprocess.CalledProcessError(1, "cmd", stderr="cannot resume send: fail")
        job = _make_job(
            no_estimate_send_size=False,
            zfs_program="zfs",
            dry_run=False,
            curr_zfs_send_program_opts=[],
            log=MagicMock(),
        )
        remote = MagicMock(sudo="sudo")

        def raise_retryable(*_args: object, **_kwargs: object) -> str:
            raise RetryableError("boom") from cp_error

        with patch.object(job, "try_ssh_command", side_effect=raise_retryable):
            with self.assertRaises(RetryableError) as ctx:
                _estimate_send_size(job, remote, "pool/ds", "token", "src@snap")
        self.assertTrue(ctx.exception.retry_immediately_once)
        clear.assert_called_once_with(job, "pool/ds", cp_error.stderr)

    @patch("bzfs_main.replication._estimate_send_size")
    def test_estimate_send_sizes_parallel_respects_no_estimate(self, est: MagicMock) -> None:
        """When p.no_estimate_send_size is True, sizes should be zeros and no calls made."""
        job = _make_job(no_estimate_send_size=True)
        remote = MagicMock(location="src")
        steps = [
            ("-i", "pool/src@s1", "pool/src@s2", ["pool/src@s2"]),
            ("-i", "pool/src@s2", "pool/src@s3", ["pool/src@s3"]),
        ]
        sizes = _estimate_send_sizes_in_parallel(job, remote, "pool/ds", None, steps)
        self.assertEqual([0, 0], sizes)
        est.assert_not_called()

    @patch("bzfs_main.replication.run_ssh_cmd_batched")
    def test_zfs_set_batches_properties(self, batched: MagicMock) -> None:
        remote = MagicMock(sudo="sudo")
        job = _make_job(zfs_program="zfs", dry_run=False)
        _zfs_set(job, ["foo=bar", "baz=qux"], remote, "pool/ds")
        batched.assert_called_once()
        self.assertEqual(["foo=bar", "baz=qux"], batched.call_args[0][3])

    def test_zfs_get_uses_cache(self) -> None:
        job = _make_job(zfs_program="zfs", log=MagicMock())
        remote = MagicMock()
        cache: dict = {}
        with patch.object(job, "run_ssh_command", return_value="prop\tval\n") as mock_run_ssh_command:
            result1 = _zfs_get(job, remote, "pool/ds", "none", "property,value", "prop", True, cache)
            result2 = _zfs_get(job, remote, "pool/ds", "none", "property,value", "prop", True, cache)
        self.assertEqual({"prop": "val"}, result1)
        self.assertEqual(result1, result2)
        mock_run_ssh_command.assert_called_once()

    def test_prepare_src_local_pv(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog == "sh"

        job = _prepare_job(dst_host="host", is_program_available=avail)
        with (
            patch("bzfs_main.replication._pv_cmd") as pv,
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            pv.return_value = "pv_src"
            src, loc, dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertTrue(src.startswith("zfs send"))
        self.assertIn("pv_src", src)
        self.assertEqual("", loc)
        self.assertEqual("zfs recv", dst)
        self.assertFalse(pv.call_args.kwargs.get("disable_progress_bar", False))

    def test_prepare_dst_local_pv(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog == "sh"

        job = _prepare_job(src_host="host", is_program_available=avail)
        with (
            patch("bzfs_main.replication._pv_cmd") as pv,
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            pv.return_value = "pv_dst"
            src, loc, dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertEqual("zfs send", src)
        self.assertEqual("", loc)
        self.assertEqual("pv_dst | zfs recv", dst.strip())

    def test_prepare_local_pv_with_compress_disabled_progress(self) -> None:
        job = _prepare_job(src_host="src", dst_host="dst", is_program_available=lambda _p, _l: True)
        with (
            patch("bzfs_main.replication._pv_cmd") as pv,
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="comp"),
            patch("bzfs_main.replication._decompress_cmd", return_value="decomp"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            pv.return_value = "pv_loc"
            src, loc, dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertTrue(src.startswith("sh -c"))
        self.assertIn("comp", src)
        self.assertIn("pv_loc", loc)
        self.assertTrue(dst.startswith("sh -c"))
        self.assertIn("decomp", dst)
        self.assertTrue(dst.endswith("zfs recv"))
        self.assertTrue(pv.call_args.kwargs["disable_progress_bar"])

    def test_prepare_local_pv_without_compress_no_disable(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog == "sh"

        job = _prepare_job(src_host="src", dst_host="dst", is_program_available=avail)
        with (
            patch("bzfs_main.replication._pv_cmd") as pv,
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            pv.return_value = "pv_loc"
            src, loc, dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertEqual("zfs send", src)
        self.assertIn("pv_loc", loc)
        self.assertEqual("zfs recv", dst)
        self.assertFalse(pv.call_args.kwargs.get("disable_progress_bar", False))

    def test_prepare_local_buffer_constructs_pipe(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog == "sh"

        job = _prepare_job(src_host="src", dst_host="dst", is_program_available=avail)

        def mbuf(_p: MagicMock, loc: str, *_a: object) -> str:
            return "LBUF" if loc == "local" else "cat"

        with (
            patch("bzfs_main.replication._pv_cmd", return_value="PV"),
            patch("bzfs_main.replication._mbuffer_cmd", side_effect=mbuf),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            _src, loc, _dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertEqual("| LBUF | PV | LBUF", loc)

    def test_prepare_local_buffer_cat_omits_pipe(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog == "sh"

        job = _prepare_job(src_host="src", dst_host="dst", is_program_available=avail)
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            src, loc, dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertEqual("zfs send", src)
        self.assertEqual("", loc)
        self.assertEqual("zfs recv", dst)

    def test_prepare_local_buffer_without_pv_uses_single_buffer(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog == "sh"

        job = _prepare_job(src_host="src", dst_host="dst", is_program_available=avail)

        def mbuf(_p: MagicMock, loc: str, *_a: object) -> str:
            return "LBUF" if loc == "local" else "cat"

        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", side_effect=mbuf),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            _src, loc, _dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertEqual("| LBUF", loc)

    def test_prepare_src_pipe_inject_fail(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog == "sh"

        job = _prepare_job(is_program_available=avail)
        job.inject_params["inject_src_pipe_fail"] = True
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            src, _loc, _dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertIn("dd bs=64", src)

    def test_prepare_src_pipe_inject_garble(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog == "sh"

        job = _prepare_job(is_program_available=avail)
        job.inject_params["inject_src_pipe_garble"] = True
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            src, _loc, _dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertIn("base64", src)

    def test_prepare_src_send_error_injected(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog == "sh"

        job = _prepare_job(is_program_available=avail)
        job.inject_params["inject_src_send_error"] = True
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            src, _loc, _dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertTrue(src.startswith("zfs send --injectedGarbageParameter"))

    def test_prepare_dst_pipe_inject_fail(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog == "sh"

        job = _prepare_job(is_program_available=avail)
        job.inject_params["inject_dst_pipe_fail"] = True
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            _src, _loc, dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertIn("dd bs=1024", dst)

    def test_prepare_dst_pipe_inject_garble(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog == "sh"

        job = _prepare_job(is_program_available=avail)
        job.inject_params["inject_dst_pipe_garble"] = True
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            _src, _loc, dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertIn("base64", dst)

    def test_prepare_dst_receive_error_injected(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog == "sh"

        job = _prepare_job(is_program_available=avail)
        job.inject_params["inject_dst_receive_error"] = True
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            _src, _loc, dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertTrue(dst.endswith("--injectedGarbageParameter"))

    def test_prepare_src_buffer_added(self) -> None:
        def mbuf(_p: MagicMock, loc: str, *_a: object) -> str:
            return "SRCBUF" if loc == "src" else "cat"

        job = _prepare_job(is_program_available=lambda prog, loc: prog == "sh")
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", side_effect=mbuf),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            src, _loc, _dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertTrue(src.startswith("zfs send"))
        self.assertIn("SRCBUF", src)

    def test_prepare_dst_buffer_added(self) -> None:
        def mbuf(_p: MagicMock, loc: str, *_a: object) -> str:
            return "DSTBUF" if loc == "dst" else "cat"

        job = _prepare_job(is_program_available=lambda prog, loc: prog == "sh")
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", side_effect=mbuf),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            _src, _loc, dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertIn("DSTBUF", dst)
        self.assertTrue(dst.endswith("zfs recv"))

    def test_prepare_compression_commands_included(self) -> None:
        job = _prepare_job(is_program_available=lambda prog, loc: True)
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="COMP") as comp,
            patch("bzfs_main.replication._decompress_cmd", return_value="DECOMP") as decomp,
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            src, _loc, dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertTrue(src.startswith("zfs send"))
        self.assertIn("COMP", src)
        self.assertIn("DECOMP", dst)
        self.assertTrue(dst.endswith("zfs recv"))
        comp.assert_called_once()
        decomp.assert_called_once()

    def test_prepare_no_compression_skips_commands(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog == "sh"

        job = _prepare_job(is_program_available=avail)
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd") as comp,
            patch("bzfs_main.replication._decompress_cmd") as decomp,
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            _src, _loc, _dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        comp.assert_not_called()
        decomp.assert_not_called()

    def test_prepare_enables_compression_only_if_both_hosts_have_shell(self) -> None:
        """Scenario: src host has sh, but the dst host has no sh on the PATH. This should disable zstd."""

        def avail(prog: str, loc: str) -> bool:
            if prog == "sh" and loc == "dst":
                return False
            return prog in {"sh", "zstd"}

        job = _prepare_job(src_host="src", dst_host="dst", is_program_available=avail)

        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="COMP"),
            patch("bzfs_main.replication._decompress_cmd", return_value="DECOMP"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            src, _loc, dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertEqual("zfs send", src)
        self.assertEqual("zfs recv", dst)

    def test_prepare_no_shell_src(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog != "sh" or loc != "src"

        job = _prepare_job(is_program_available=avail)
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="pv"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="BUF"),
            patch("bzfs_main.replication._compress_cmd", return_value="comp"),
            patch("bzfs_main.replication._decompress_cmd", return_value="decomp"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            src, _loc, _dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertEqual("zfs send", src)

    def test_prepare_no_shell_dst(self) -> None:
        def avail(prog: str, loc: str) -> bool:
            return prog != "sh" or loc != "dst"

        job = _prepare_job(is_program_available=avail)
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="pv"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="BUF"),
            patch("bzfs_main.replication._compress_cmd", return_value="comp"),
            patch("bzfs_main.replication._decompress_cmd", return_value="decomp"),
            patch("bzfs_main.replication.squote", side_effect=lambda _r, s: s),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            _src, _loc, dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertEqual("zfs recv", dst)

    def test_prepare_src_remote_quoted(self) -> None:
        job = _prepare_job(src_host="host", is_program_available=lambda prog, loc: prog == "sh")
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            src, _loc, _dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertEqual("'zfs send'", src)

    def test_prepare_dst_remote_quoted(self) -> None:
        job = _prepare_job(dst_host="host", is_program_available=lambda prog, loc: prog == "sh")
        with (
            patch("bzfs_main.replication._pv_cmd", return_value="cat"),
            patch("bzfs_main.replication._mbuffer_cmd", return_value="cat"),
            patch("bzfs_main.replication._compress_cmd", return_value="cat"),
            patch("bzfs_main.replication._decompress_cmd", return_value="cat"),
            patch("bzfs_main.replication.dquote", side_effect=lambda s: s),
        ):
            _src, _loc, dst = _prepare_zfs_send_receive(job, "pool/ds", ["zfs", "send"], ["zfs", "recv"], 1, "1B")
        self.assertEqual("'zfs recv'", dst)

    def test_replicate_dataset_e2e_skips_hourly_in_steps(self) -> None:
        """End-to-end verification that incremental replication planning never includes snapshots excluded by policy,
        specifically hourlies, even when bookmarks are present and bookmark-aware planning is enabled.

        Setup: The source dataset has a daily d1, an hourly h1 (both as a bookmark and a true snapshot), and a
        newer daily d2. The destination already contains d1. We configure snapshot selection with an include-all
        rule and an exclude-"h.*" rule, thereby allowing only dailies to be replicated. We build a real Params via
        the parser with `--dryrun=send` so side effects are suppressed while exercising production splitting and
        validation logic.

        Isolation: We patch replication helpers to avoid unrelated behavior (I/O, threads, property propagation),
        return canned src/dst snapshot lists, and force `are_bookmarks_enabled=True` to take the bookmark-aware
        code path. We also stub `_create_zfs_bookmarks` and `_run_zfs_send_receive` because the test is concerned
        only with planning, not execution.

        Assertion: We spy on `_incremental_send_steps_wrapper` to capture the sequence of computed `-i/-I` steps
        and flatten each step's `to_snapshots` payload. The final check asserts that `@h1` is absent from the union
        of all `to_snapshots`, proving that snapshot filtering fully propagates into planning regardless of the
        presence of a same-GUID bookmark. This validates correct interplay between filtering, GUID alignment, and
        step construction, including the latest-common-snapshot logic.
        """
        src_dataset = "pool/src"
        dst_dataset = "pool/dst"
        p = self.make_params(args=self.argparser_parse_args([src_dataset, dst_dataset]))

        exclude_regexes = compile_regexes(["h.*"])  # exclude hourlies
        include_regexes = compile_regexes([".*"])  # include everything else
        p.snapshot_filters = [[SnapshotFilter(SNAPSHOT_REGEX_FILTER_NAME, None, (exclude_regexes, include_regexes))]]
        job = _make_job()
        job.params = p

        src_list = "\n".join(
            [
                f"1\t{src_dataset}@d1",
                f"2\t{src_dataset}#h1",  # bookmark for hourly
                f"2\t{src_dataset}@h1",
                f"3\t{src_dataset}@d2",
            ]
        )
        dst_list = f"1\t{dst_dataset}@d1\n"

        def fake_try_ssh_command(_job: Job, remote: Remote, _lvl: int, **kwargs: dict[str, list[str]]) -> str:
            cmd_opt = kwargs.get("cmd")
            cmd: list[str] = cmd_opt if isinstance(cmd_opt, list) else []
            text = " ".join(cmd)
            if remote is p.src and " list -t " in text and "-o guid,name" in text:
                return src_list
            if remote is p.dst and " list -t snapshot" in text and "-o guid,name" in text:
                return dst_list
            return ""

        captured_steps: list[list[tuple[str, str, str, list[str]]]] = []

        def observing_incremental_send_steps_wrapper(
            pp: Params, src_snaps: list[str], src_guids: list[str], included: set[str], is_resume: bool
        ) -> list[tuple]:
            steps = _incremental_send_steps_wrapper(pp, src_snaps, src_guids, included, is_resume)
            captured_steps.append(steps)
            return steps

        with (
            patch("bzfs_main.replication.are_bookmarks_enabled", return_value=True),
            patch("bzfs_main.replication._recv_resume_token", return_value=(None, [], [])),
            patch("bzfs_main.replication._estimate_send_size", return_value=0),
            patch(
                "bzfs_main.replication._add_recv_property_options",
                side_effect=lambda j, _full, recv_opts, _ds, _c: (recv_opts, []),
            ),
            patch("bzfs_main.replication._check_zfs_dataset_busy", return_value=True),
            patch("bzfs_main.replication._create_zfs_bookmarks", side_effect=lambda *_a, **_kw: None),
            patch(
                "bzfs_main.replication._incremental_send_steps_wrapper", side_effect=observing_incremental_send_steps_wrapper
            ),
            patch("bzfs_main.replication._run_zfs_send_receive", side_effect=lambda *_args, **_kw: None),
        ):
            with patch.object(
                job, "try_ssh_command", side_effect=lambda *args, **kwargs: fake_try_ssh_command(job, *args, **kwargs)
            ):
                replicate_dataset(
                    job,
                    src_dataset,
                    tid="1/1",
                    retry=Retry(
                        count=0,
                        start_time_nanos=0,
                        policy=RetryPolicy(),
                        config=RetryConfig(),
                        previous_outcomes=(),
                    ),
                )

        self.assertTrue(captured_steps, "No steps captured")
        to_snaps_all = [snap for step in captured_steps[0] for snap in step[3]]
        self.assertNotIn(f"{src_dataset}@h1", to_snaps_all)
