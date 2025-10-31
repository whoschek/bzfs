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
"""Unit tests for logging configuration utilities."""

from __future__ import (
    annotations,
)
import json
import logging
import os
import socket
import sys
import tempfile
import unittest
from datetime import (
    datetime,
)
from logging import (
    Logger,
)
from typing import (
    Any,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from bzfs_main import (
    argparse_cli,
    bzfs,
)
from bzfs_main.configuration import (
    LogParams,
)
from bzfs_main.loggers import (
    LOG_LEVEL_PREFIXES,
    Tee,
    _get_default_logger,
    _get_dict_config_logger,
    _get_logger_subname,
    _get_syslog_address,
    _remove_json_comments,
    _validate_log_config_dict,
    get_default_log_formatter,
    get_logger,
    get_simple_logger,
    reset_logger,
    validate_log_config_variable,
)
from bzfs_main.utils import (
    DIE_STATUS,
    LOG_STDERR,
    LOG_STDOUT,
    LOG_TRACE,
)
from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestHelperFunctions,
        TestLogging,
        TestTee,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestHelperFunctions(AbstractTestCase):

    def test_logdir_basename_prefix(self) -> None:
        """Basename of --log-dir must start with prefix 'bzfs-logs'."""
        with tempfile.TemporaryDirectory(prefix="logdir_symlink_test") as tmp_rootdir:
            logdir = os.path.join(tmp_rootdir, argparse_cli.LOG_DIR_DEFAULT + "-tmp")
            LogParams(bzfs.argument_parser().parse_args(args=["src", "dst", "--log-dir=" + logdir]))
            self.assertTrue(os.path.exists(logdir))
        with tempfile.TemporaryDirectory(prefix="logdir_symlink_test") as tmp_rootdir2:
            logdir = os.path.join(tmp_rootdir2, "bzfs-tmp")
            args = bzfs.argument_parser().parse_args(args=["src", "dst", "--log-dir=" + logdir])
            with self.assertRaises(SystemExit):
                LogParams(args)
            self.assertFalse(os.path.exists(logdir))

    def test_logdir_must_not_be_symlink(self) -> None:
        with tempfile.TemporaryDirectory(prefix="logdir_symlink_test") as tmpdir:
            target = os.path.join(tmpdir, "target")
            os.mkdir(target)
            link_path = os.path.join(tmpdir, argparse_cli.LOG_DIR_DEFAULT + "-link")
            os.symlink(target, link_path)
            args = bzfs.argument_parser().parse_args(args=["src", "dst", "--log-dir=" + link_path])
            with self.assertRaises(SystemExit) as cm:
                LogParams(args)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn("--log-dir must not be a symlink", str(cm.exception))

    def test_get_logger_with_cleanup(self) -> None:
        """Verify logger handlers clean up and logged output stays quiet."""

        def check(log: Logger, files: set[str]) -> None:
            files_todo = files.copy()
            for handler in log.handlers:
                if isinstance(handler, logging.FileHandler):
                    self.assertIn(handler.baseFilename, files_todo)
                    files_todo.remove(handler.baseFilename)
            self.assertEqual(0, len(files_todo))

        reset_logger()
        prefix = "test_get_logger:"
        args = self.argparser_parse_args(args=["src", "dst"])
        root_logger = logging.getLogger()
        log_params = LogParams(args)
        log = get_logger(log_params, args, root_logger, logger_name_suffix="")
        self.assertTrue(log is root_logger)
        log.info(f"{prefix}aaa1")

        args = self.argparser_parse_args(args=["src", "dst"])
        log_params = LogParams(args)
        log = get_logger(log_params, args)
        # log.log(LOG_STDERR, "%s", prefix + "bbbe1")
        log.log(LOG_STDOUT, "%s", " ")
        # log.info("%s", prefix + "bbb3")
        log.setLevel(logging.WARNING)
        log.log(LOG_STDERR, "%s", prefix + "bbbe2")
        log.log(LOG_STDOUT, "%s", prefix + "bbbo2")
        log.info("%s", prefix + "bbb4")
        log.log(LOG_TRACE, "%s", prefix + "bbb5")
        log.setLevel(LOG_TRACE)
        log.log(LOG_TRACE, "%s", prefix + "bbb6")
        files = {os.path.abspath(log_params.log_file)}
        check(log, files)

        args = self.argparser_parse_args(args=["src", "dst", "-v"])
        log_params = LogParams(args)
        log = get_logger(log_params, args)
        self.assertIsNotNone(log)
        files.add(os.path.abspath(log_params.log_file))
        check(log, files)

        log.addFilter(lambda record: True)  # dummy
        reset_logger()
        files.clear()
        check(log, files)

        args = self.argparser_parse_args(args=["src", "dst", "-v", "-v"])
        log_params = LogParams(args)
        log = get_logger(log_params, args)
        self.assertIsNotNone(log)
        files.add(os.path.abspath(log_params.log_file))
        check(log, files)

        args = self.argparser_parse_args(args=["src", "dst", "--quiet"])
        log_params = LogParams(args)
        log = get_logger(log_params, args)
        self.assertIsNotNone(log)
        files.add(os.path.abspath(log_params.log_file))
        check(log, files)

        reset_logger()

    def test_get_syslog_address(self) -> None:
        udp = socket.SOCK_DGRAM
        tcp = socket.SOCK_STREAM
        self.assertEqual((("localhost", 514), udp), _get_syslog_address("localhost:514", "UDP"))
        self.assertEqual((("localhost", 514), tcp), _get_syslog_address("localhost:514", "TCP"))
        self.assertEqual(("/dev/log", None), _get_syslog_address("/dev/log", "UDP"))
        self.assertEqual(("/dev/log", None), _get_syslog_address("/dev/log", "TCP"))

    def test_validate_log_config_variable(self) -> None:
        self.assertIsNone(validate_log_config_variable("name:value"))
        for var in ["noColon", ":noName", "$n:v", "{:v", "}:v", "", "  ", "\t", "a\tb:v", "spa ce:v", '"k":v', "'k':v"]:
            self.assertIsNotNone(validate_log_config_variable(var))

    def test_log_config_file_validation(self) -> None:
        args = self.argparser_parse_args(["src", "dst", "--log-config-file", "+bad_file_name.txt"])
        log_params = LogParams(args)
        with self.assertRaises(SystemExit):
            _get_dict_config_logger(log_params, args)

    def test_no_argument_file_blocks_plus_file_log_config(self) -> None:
        """+file expansion for --log-config-file must be rejected under --no-argument-file."""
        args = self.argparser_parse_args(["src", "dst", "--no-argument-file", "--log-config-file", "+some_file.json"])
        with self.assertRaises(SystemExit):
            LogParams(args)


#############################################################################
class TestLogging(AbstractTestCase):
    """Tests logging helpers including default and simple formatter behavior."""

    def test_get_default_logger(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        lp = LogParams(args)
        logging.getLogger(_get_logger_subname()).handlers.clear()
        logging.getLogger(bzfs.__name__).handlers.clear()
        log = _get_default_logger(lp, args)
        self.assertTrue(any(isinstance(h, logging.StreamHandler) for h in log.handlers))
        self.assertTrue(any(isinstance(h, logging.FileHandler) for h in log.handlers))

    def test_get_default_logger_considers_existing_sublog_handlers(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        lp = LogParams(args)
        logger = logging.getLogger(f"{bzfs.__name__}.{lp.logger_name_suffix}")
        logger.handlers.clear()
        stream_h: logging.StreamHandler | None = None
        file_h: logging.FileHandler | None = None
        try:
            stream_h = logging.StreamHandler(stream=sys.stdout)
            file_h = logging.FileHandler(lp.log_file, encoding="utf-8")
            logger.addHandler(stream_h)
            logger.addHandler(file_h)
            log_result = _get_default_logger(lp, args, logger_name_suffix=lp.logger_name_suffix)
            self.assertEqual([stream_h, file_h], log_result.handlers)
        finally:
            if stream_h is not None:
                stream_h.close()
            if file_h is not None:
                file_h.close()
            logger.handlers.clear()
            reset_logger(logger_name_suffix=lp.logger_name_suffix)

    @patch("logging.handlers.SysLogHandler")
    def test_get_default_logger_syslog_warning(self, mock_syslog: MagicMock) -> None:
        args = self.argparser_parse_args(
            [
                "src",
                "dst",
                "--log-syslog-address=127.0.0.1:514",
                "--log-syslog-socktype=UDP",
                "--log-syslog-facility=1",
                "--log-syslog-level=DEBUG",
            ]
        )
        lp = LogParams(args)
        self.assertEqual("127.0.0.1:514", args.log_syslog_address)
        self.assertEqual("UDP", args.log_syslog_socktype)
        self.assertEqual(1, args.log_syslog_facility)
        self.assertEqual("DEBUG", args.log_syslog_level)
        logging.getLogger(_get_logger_subname()).handlers.clear()
        logger = logging.getLogger(bzfs.__name__)
        logger.handlers.clear()
        handler = logging.Handler()
        mock_syslog.return_value = handler
        try:
            with patch.object(logger, "warning") as mock_warning:
                log = _get_default_logger(lp, args)
                mock_syslog.assert_called_once()
                self.assertIn(handler, log.handlers)
                mock_warning.assert_called_once()
        finally:
            reset_logger()

    def test_default_log_formatter_with_exc_info(self) -> None:
        """Ensures DefaultLogFormatter outputs traceback when exc_info is set."""
        formatter = get_default_log_formatter()
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            record = logging.LogRecord(
                name="test",
                level=logging.ERROR,
                pathname=__file__,
                lineno=1,
                msg="problem",
                args=(),
                exc_info=sys.exc_info(),
            )
        formatted = formatter.format(record)
        self.assertIn("RuntimeError: boom", formatted)
        self.assertIn("problem", formatted)

    def test_default_log_formatter_pads_placeholder(self) -> None:
        """Pads message left of '%s' to 54 characters before substitution."""
        formatter = get_default_log_formatter()
        with patch("bzfs_main.loggers.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2000, 1, 1, 0, 0, 0)
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname=__file__,
                lineno=1,
                msg="before %s after",
                args=("X",),
                exc_info=None,
            )
            formatted = formatter.format(record)
        self.assertEqual(54, formatted.index("X"))
        self.assertTrue(formatted.endswith("after"))

    def test_default_log_formatter_formats_args(self) -> None:
        """Substitutes arguments into message when no exception is present."""
        formatter = get_default_log_formatter()
        with patch("bzfs_main.loggers.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2000, 1, 1, 0, 0, 0)
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname=__file__,
                lineno=1,
                msg="%s start",
                args=("Go",),
                exc_info=None,
            )
            formatted = formatter.format(record)
        self.assertIn("Go start", formatted)
        self.assertNotIn("%s", formatted)

    def test_scoped_loggers_teardown_is_isolated(self) -> None:
        """reset_logger(logger_name_suffix=...) only removes handlers for the targeted suffix-specific logger."""
        args1 = self.argparser_parse_args(["src", "dst"])
        lp1 = LogParams(args1)
        log1 = get_logger(lp1, args1, logger_name_suffix=lp1.logger_name_suffix)

        args2 = self.argparser_parse_args(["src", "dst", "--quiet"])
        lp2 = LogParams(args2)
        log2 = get_logger(lp2, args2, logger_name_suffix=lp2.logger_name_suffix)

        def file_paths(log: Logger) -> set[str]:
            return {
                os.path.abspath(handler.baseFilename) for handler in log.handlers if isinstance(handler, logging.FileHandler)
            }

        self.assertNotEqual(log1.name, log2.name)
        self.assertEqual({os.path.abspath(lp1.log_file)}, file_paths(log1))
        self.assertEqual({os.path.abspath(lp2.log_file)}, file_paths(log2))

        reset_logger(logger_name_suffix=lp1.logger_name_suffix)
        self.assertEqual([], logging.getLogger(log1.name).handlers)
        remaining = logging.getLogger(log2.name)
        self.assertNotEqual([], remaining.handlers)
        self.assertEqual({os.path.abspath(lp2.log_file)}, file_paths(remaining))

        reset_logger(logger_name_suffix=lp2.logger_name_suffix)
        self.assertEqual([], logging.getLogger(log2.name).handlers)

    def test_level_formatter_injects_fields(self) -> None:
        """LevelFormatter attaches level prefix and program name to records."""
        logger = get_simple_logger("demo")
        handler = logger.handlers[0]
        formatter = handler.formatter
        assert formatter is not None
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname=__file__,
            lineno=1,
            msg="hi",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        rec_any: Any = record
        self.assertEqual(LOG_LEVEL_PREFIXES[logging.WARNING], rec_any.level_prefix)
        self.assertEqual("demo", rec_any.program)
        self.assertIn("[demo]", formatted)

    def test_remove_json_comments(self) -> None:
        config_str = (
            "#c1\n" + "line_without_comment\n" + "line_with_trailing_hash_only #\n" + "line_with_trailing_comment##tail#\n"
        )
        expected = "\nline_without_comment\nline_with_trailing_hash_only #\n" + "line_with_trailing_comment#"
        self.assertEqual(expected, _remove_json_comments(config_str))

    def test_get_dict_config_logger(self) -> None:
        with tempfile.NamedTemporaryFile("w", prefix="bzfs_log_config", suffix=".json", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "version": 1,
                    "handlers": {"h": {"class": "logging.StreamHandler"}},
                    "root": {"level": "${bzfs.log_level}", "handlers": ["h"]},
                },
                f,
            )
            path = f.name
        try:
            args = self.argparser_parse_args(["src", "dst", f"--log-config-file=+{path}"])
            lp = LogParams(args)
            with patch("logging.config.dictConfig") as m:
                _get_dict_config_logger(lp, args)
                self.assertEqual(lp.log_level, m.call_args[0][0]["root"]["level"])
        finally:
            os.remove(path)

    def test_get_dict_config_logger_inline_string_with_defaults(self) -> None:
        config = (
            '{"version": 1, "handlers": {"h": {"class": "logging.StreamHandler"}}, '
            '"root": {"level": "${missing:DEBUG}", "handlers": ["h"]}}'
        )
        args = self.argparser_parse_args(["src", "dst", "--log-config-file", config])
        lp = LogParams(args)
        with patch("logging.config.dictConfig") as m:
            _get_dict_config_logger(lp, args)
            self.assertEqual("DEBUG", m.call_args[0][0]["root"]["level"])

    def test_get_dict_config_logger_missing_default_raises(self) -> None:
        config = '{"version": 1, "root": {"level": "${missing}"}}'
        args = self.argparser_parse_args(["src", "dst", "--log-config-file", config])
        lp = LogParams(args)
        with self.assertRaises(ValueError):
            _get_dict_config_logger(lp, args)

    def test_get_dict_config_logger_invalid_name_raises(self) -> None:
        config = '{"version": 1, "root": {"level": "${bad name:INFO}"}}'
        args = self.argparser_parse_args(["src", "dst", "--log-config-file", config])
        lp = LogParams(args)
        with self.assertRaises(ValueError):
            _get_dict_config_logger(lp, args)

    def test_get_dict_config_logger_invalid_path(self) -> None:
        with tempfile.NamedTemporaryFile("w", prefix="badfilename", suffix=".json", delete=False, encoding="utf-8") as f:
            f.write("{}")
            path = f.name
        try:
            args = self.argparser_parse_args(["src", "dst", f"--log-config-file=+{path}"])
            lp = LogParams(args)
            with self.assertRaises(SystemExit):
                _get_dict_config_logger(lp, args)
        finally:
            os.remove(path)

    def test_log_config_file_with_validation(self) -> None:
        """Tests that validate_log_config_dict() would prevent an arbitrary code execution vulnerability."""
        malicious_config = {
            "version": 1,
            "handlers": {"safe_handler": {"class": "logging.StreamHandler", "stream": "ext://sys.stdout"}},
            "root": {"handlers": ["safe_handler", {"()": "os.system", "command": "echo pwned > /dev/null"}]},
        }
        malicious_config_str = json.dumps(malicious_config)
        args = self.argparser_parse_args(["src", "dst", "--skip-replication", "--log-config-file", malicious_config_str])
        lp = LogParams(args)
        with patch("os.system") as mock_system:
            # The fixed code should detect the disallowed callable and exit.
            with self.assertRaises(SystemExit) as cm:  # get_dict_config_logger() calls die(), which raises SystemExit.
                _get_dict_config_logger(lp, args)
            self.assertEqual(cm.exception.code, DIE_STATUS)
            self.assertIn("Disallowed callable 'os.system'", str(cm.exception))
            mock_system.assert_not_called()

        _validate_log_config_dict(None)  # type: ignore[arg-type]

    def test_log_config_file_disallows_untrusted_class_imports(self) -> None:
        """Ensure we also restrict 'class' entries to a safe whitelist to prevent import-time code execution."""
        malicious_config = {
            "version": 1,
            # Attempt to instantiate an arbitrary class via 'class' field (should be blocked)
            "handlers": {"evil": {"class": "http.server.SimpleHTTPRequestHandler"}},
            "root": {"level": "INFO", "handlers": ["evil"]},
        }
        malicious_config_str = json.dumps(malicious_config)
        args = self.argparser_parse_args(["src", "dst", "--log-config-file", malicious_config_str])
        lp = LogParams(args)
        with self.assertRaises(SystemExit) as cm:
            _get_dict_config_logger(lp, args)
        self.assertEqual(cm.exception.code, DIE_STATUS)
        self.assertIn("Disallowed class 'http.server.SimpleHTTPRequestHandler'", str(cm.exception))


#############################################################################
class TestTee(AbstractTestCase):
    """Tests the Tee class that duplicates writes to multiple streams."""

    def test_write_flushes_all_streams(self) -> None:
        """Write() forwards text and flush() to each target file."""
        file1 = MagicMock()
        file2 = MagicMock()
        tee = Tee(file1, file2)
        tee.write("abc")
        for f in (file1, file2):
            f.write.assert_called_once_with("abc")
            f.flush.assert_called_once()

    def test_flush_only(self) -> None:
        """Flush() independently flushes all target streams."""
        file1 = MagicMock()
        file2 = MagicMock()
        tee = Tee(file1, file2)
        tee.flush()
        file1.flush.assert_called_once()
        file2.flush.assert_called_once()

    def test_fileno_returns_first_descriptor(self) -> None:
        """Fileno() exposes the descriptor of the first underlying stream."""
        with tempfile.TemporaryFile() as f1, tempfile.TemporaryFile() as f2:
            tee = Tee(f1, f2)
            self.assertEqual(f1.fileno(), tee.fileno())
