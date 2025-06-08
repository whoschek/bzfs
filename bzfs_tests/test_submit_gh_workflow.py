from __future__ import annotations

import contextlib
import io
import json
import pathlib
import tempfile
import subprocess
from typing import Any, Callable, Iterator, List, Tuple
import zipfile

import unittest
from unittest import mock

import bzfs_gh.submit_gh_workflow as sw  # noqa: E402


def _mock_subprocess_run(responses: List[Tuple[str, int, str]]) -> Callable[..., Any]:
    """Return a fake subprocess.run that pops responses."""

    def _run(*_args: Any, **_kw: Any) -> mock.Mock:
        if not responses:
            raise RuntimeError("No more mock responses for subprocess.run")
        stdout, returncode, stderr = responses.pop(0)
        if stdout == "TIMEOUT":
            raise subprocess.TimeoutExpired(cmd=_args[0], timeout=_kw.get("timeout", 0))
        proc = mock.Mock()
        proc.stdout = stdout
        proc.returncode = returncode
        proc.stderr = stderr
        return proc

    return _run


SAMPLE_YAML: str = """name: CI
on: workflow_dispatch
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
workflow_dispatch:
  inputs:
    run_all_jobs:
      description: Run all jobs
      type: boolean
      default: false
    job_name:
      description: Job name
      type: choice
      options: [fast, full]
      default: fast
    num_runs:
      description: Number runs
      type: number
      default: 2
    env_name:
      description: Env
      type: environment
      default: prod
    extra:
      description: Extra
      type: string
      default: xyz
"""


class TestHelpers(unittest.TestCase):
    """Unit tests for helper functions."""

    def test_canonicalise_default(self) -> None:
        self.assertTrue(sw.canonicalise_default("true", "boolean"))
        self.assertFalse(sw.canonicalise_default(None, "boolean"))
        self.assertEqual(sw.canonicalise_default("3", "number"), 3)
        self.assertEqual(sw.canonicalise_default("3.5", "number"), 3.5)
        self.assertEqual(sw.canonicalise_default(None, "string"), "")

    def test_bool_flag(self) -> None:
        self.assertEqual(sw._bool_flag("flag", False)["flags"], ["--flag"])
        self.assertEqual(sw._bool_flag("flag", True)["flags"], ["--no-flag"])

    def test_run_retries(self) -> None:
        responses = [("", 1, "err"), ("ok", 0, "")]
        with mock.patch("bzfs_gh.submit_gh_workflow.subprocess.run", _mock_subprocess_run(responses)), mock.patch(
            "time.sleep", lambda _x: None
        ):
            out = sw.run(["gh"])
        self.assertEqual(out, "ok")

    def test_run_failure(self) -> None:
        responses = [("", 1, "boom1"), ("", 1, "boom2"), ("", 1, "boom3")]
        with mock.patch(
            "bzfs_gh.submit_gh_workflow.subprocess.run",
            _mock_subprocess_run(responses),
        ), mock.patch("time.sleep", lambda _x: None):
            buf = io.StringIO()
            with contextlib.redirect_stderr(buf), self.assertRaises(SystemExit):
                sw.run(["gh"])
            self.assertIn("boom3", buf.getvalue())

    def test_load_inputs_and_parser(self) -> None:
        with tempfile.NamedTemporaryFile("w+", delete=False) as tf:
            tf.write(SAMPLE_YAML)
            tf.flush()
            inputs = sw.load_inputs(tf.name)
            self.assertIn("job_name", inputs)
            parser = sw.make_parser(inputs)
            parsed = parser.parse_args(
                [
                    "repo",
                    "main",
                    tf.name,
                    "--job-name",
                    "full",
                ]
            )
            self.assertEqual(parsed.job_name, "full")


class TestMainFlow(unittest.TestCase):
    """End-to-end tests (with mocking) for main()"""

    @staticmethod
    def _make_yaml() -> str:
        tf = tempfile.NamedTemporaryFile("w+", delete=False)
        tf.write(SAMPLE_YAML)
        tf.flush()
        return tf.name

    def test_network_unavailable(self) -> None:
        with self.assertRaises(SystemExit):
            with mock.patch("bzfs_gh.submit_gh_workflow.network_available", return_value=False):
                sw.main(["repo", "main", "wf"])

    @mock.patch("time.sleep", lambda _x: None)
    def test_main_success(self) -> None:
        yaml_path = self._make_yaml()
        responses: List[Tuple[str, int, str]] = [
            ("", 0, ""),
            ('[{"databaseId":1,"status":"queued","conclusion":null,"htmlURL":"url"}]', 0, ""),
            ("", 0, ""),  # gh run watch --exit-status (success)
            ('{"status":"completed","conclusion":"success","htmlURL":"url","logUrl":"log"}', 0, ""),
        ]
        with mock.patch("bzfs_gh.submit_gh_workflow.subprocess.run", _mock_subprocess_run(responses)), mock.patch(
            "bzfs_gh.submit_gh_workflow.network_available", return_value=True
        ):
            stdout_buf = io.StringIO()
            stderr_buf = io.StringIO()
            with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
                sw.main(["repo", "main", yaml_path])
        result = json.loads(stdout_buf.getvalue().strip())
        self.assertEqual(result["conclusion"], "success")
        self.assertEqual(result["exit_code"], 0)

    @mock.patch("time.sleep", lambda _x: None)
    def test_main_failure_with_log(self) -> None:
        yaml_path = self._make_yaml()
        tmp_download = tempfile.mkdtemp()
        tmp_extract = tempfile.mkdtemp()
        zip_path = pathlib.Path(tmp_download) / "logs.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("dummy.txt", "hi")
        responses: List[Tuple[str, int, str]] = [
            ("", 0, ""),
            ('[{"databaseId":2,"status":"queued","conclusion":null,"htmlURL":"url"}]', 0, ""),
            ("", 1, ""),
            ('{"status":"completed","conclusion":"failure","htmlURL":"url","logUrl":"log"}', 0, ""),
            ("", 0, ""),
        ]

        def glob_override(self: pathlib.Path, pattern: str = "*") -> Iterator[pathlib.Path]:
            yield zip_path

        mkdtemp_mock = mock.Mock(side_effect=[tmp_download, tmp_extract])
        with mock.patch("bzfs_gh.submit_gh_workflow.subprocess.run", _mock_subprocess_run(responses)), mock.patch(
            "bzfs_gh.submit_gh_workflow.pathlib.Path.glob", glob_override
        ), mock.patch("bzfs_gh.submit_gh_workflow.tempfile.mkdtemp", mkdtemp_mock), mock.patch(
            "bzfs_gh.submit_gh_workflow.network_available", return_value=True
        ):
            stdout_buf = io.StringIO()
            with contextlib.redirect_stdout(stdout_buf):
                sw.main(["repo", "main", yaml_path])
        result = json.loads(stdout_buf.getvalue().strip())
        self.assertEqual(result["conclusion"], "failure")
        self.assertEqual(result["log_archive"], str(zip_path.resolve()))
        self.assertEqual(result["archive_log_dir"], str(pathlib.Path(tmp_extract).resolve()))
        self.assertEqual(result["exit_code"], 1)

    @mock.patch("time.sleep", lambda _x: None)
    def test_main_timeout(self) -> None:
        yaml_path = self._make_yaml()
        responses: List[Tuple[str, int, str]] = [
            ("", 0, ""),
            ('[{"databaseId":3,"status":"queued","conclusion":null,"htmlURL":"url"}]', 0, ""),
            ("TIMEOUT", 1, ""),
            ("", 0, ""),
        ]
        tmp_download = tempfile.mkdtemp()
        tmp_extract = tempfile.mkdtemp()
        zip_path = pathlib.Path(tmp_download) / "logs.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("dummy.txt", "hi")

        times = iter([0.0, 1.0])

        mkdtemp_mock = mock.Mock(side_effect=[tmp_download, tmp_extract])
        with mock.patch("bzfs_gh.submit_gh_workflow.subprocess.run", _mock_subprocess_run(responses)), mock.patch(
            "bzfs_gh.submit_gh_workflow.time.monotonic", lambda: next(times)
        ), mock.patch(
            "bzfs_gh.submit_gh_workflow.tempfile.mkdtemp",
            mkdtemp_mock,
        ), mock.patch(
            "bzfs_gh.submit_gh_workflow.pathlib.Path.glob",
            lambda self, pattern="*": iter([zip_path]),
        ), mock.patch(
            "bzfs_gh.submit_gh_workflow.network_available",
            return_value=True,
        ):
            stdout_buf = io.StringIO()
            with contextlib.redirect_stdout(stdout_buf):
                sw.main(["repo", "main", yaml_path, "--timeout-secs", "0"])
        result = json.loads(stdout_buf.getvalue().strip())
        self.assertEqual(result["conclusion"], "timed_out")
        self.assertEqual(result["archive_log_dir"], str(pathlib.Path(tmp_extract).resolve()))
        self.assertEqual(result["exit_code"], 1)

    def test_log_download_retry(self) -> None:
        yaml_path = self._make_yaml()
        tmp_download = tempfile.mkdtemp()
        tmp_extract = tempfile.mkdtemp()
        zip_path = pathlib.Path(tmp_download) / "logs.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("dummy.txt", "hi")

        responses: List[Tuple[str, int, str]] = [
            ("", 0, ""),
            ('[{"databaseId":4,"status":"queued","conclusion":null,"htmlURL":"url"}]', 0, ""),
            ("", 1, ""),
            ('{"status":"completed","conclusion":"failure","htmlURL":"url","logUrl":"log"}', 0, ""),
            ("", 0, ""),
            ("", 0, ""),
        ]

        glob_calls = []

        def glob_override(self: pathlib.Path, pattern: str = "*") -> Iterator[pathlib.Path]:
            glob_calls.append(None)
            if len(glob_calls) == 2:
                yield zip_path

        mkdtemp_mock = mock.Mock(side_effect=[tmp_download, tmp_extract])
        with mock.patch("bzfs_gh.submit_gh_workflow.subprocess.run", _mock_subprocess_run(responses)), mock.patch(
            "bzfs_gh.submit_gh_workflow.pathlib.Path.glob",
            glob_override,
        ), mock.patch("bzfs_gh.submit_gh_workflow.tempfile.mkdtemp", mkdtemp_mock), mock.patch(
            "bzfs_gh.submit_gh_workflow.network_available",
            return_value=True,
        ):
            stdout_buf = io.StringIO()
            with contextlib.redirect_stdout(stdout_buf):
                sw.main(["repo", "main", yaml_path])

        result = json.loads(stdout_buf.getvalue().strip())
        self.assertEqual(result["log_archive"], str(zip_path.resolve()))
        self.assertEqual(result["archive_log_dir"], str(pathlib.Path(tmp_extract).resolve()))
        self.assertEqual(len(glob_calls), 2)
        self.assertEqual(result["exit_code"], 1)


def suite() -> unittest.TestSuite:
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestHelpers))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestMainFlow))
    return suite
