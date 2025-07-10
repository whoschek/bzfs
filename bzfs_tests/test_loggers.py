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

from __future__ import annotations
import json
import logging
import os
import shutil
import socket
import sys
import tempfile
import unittest
from logging import Logger
from unittest.mock import (
    MagicMock,
    patch,
)

from bzfs_main import bzfs
from bzfs_main.bzfs import (
    LogParams,
)
from bzfs_main.loggers import (
    get_default_logger,
    get_dict_config_logger,
    get_logger,
    get_logger_subname,
    get_syslog_address,
    remove_json_comments,
    reset_logger,
    validate_log_config_dict,
    validate_log_config_variable,
)
from bzfs_main.utils import (
    die_status,
    get_home_directory,
    log_stderr,
    log_stdout,
    log_trace,
)
from bzfs_tests.abstract_testcase import AbstractTestCase


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestHelperFunctions,
        TestLogging,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestHelperFunctions(AbstractTestCase):

    def test_logdir_basename_prefix(self) -> None:
        """Basename of --log-dir must start with prefix 'bzfs-logs'."""
        logdir = os.path.join(get_home_directory(), bzfs.log_dir_default + "-tmp")
        try:
            LogParams(bzfs.argument_parser().parse_args(args=["src", "dst", "--log-dir=" + logdir]))
            self.assertTrue(os.path.exists(logdir))
        finally:
            shutil.rmtree(logdir, ignore_errors=True)

        logdir = os.path.join(get_home_directory(), "bzfs-tmp")
        with self.assertRaises(SystemExit):
            LogParams(bzfs.argument_parser().parse_args(args=["src", "dst", "--log-dir=" + logdir]))
        self.assertFalse(os.path.exists(logdir))

    def test_logdir_must_not_be_symlink(self) -> None:
        with tempfile.TemporaryDirectory(prefix="logdir_symlink_test") as tmpdir:
            target = os.path.join(tmpdir, "target")
            os.mkdir(target)
            link_path = os.path.join(tmpdir, bzfs.log_dir_default + "-link")
            os.symlink(target, link_path)
            with self.assertRaises(SystemExit) as cm:
                LogParams(bzfs.argument_parser().parse_args(args=["src", "dst", "--log-dir=" + link_path]))
            self.assertEqual(die_status, cm.exception.code)
            self.assertIn("--log-dir must not be a symlink", str(cm.exception))

    def test_get_logger_with_cleanup(self) -> None:
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
        log = get_logger(log_params, args, root_logger)
        self.assertTrue(log is root_logger)
        log.info(f"{prefix}aaa1")

        args = self.argparser_parse_args(args=["src", "dst"])
        log_params = LogParams(args)
        log = get_logger(log_params, args)
        log.log(log_stderr, "%s", prefix + "bbbe1")
        log.log(log_stdout, "%s", prefix + "bbbo1")
        log.info("%s", prefix + "bbb3")
        log.setLevel(logging.WARNING)
        log.log(log_stderr, "%s", prefix + "bbbe2")
        log.log(log_stdout, "%s", prefix + "bbbo2")
        log.info("%s", prefix + "bbb4")
        log.log(log_trace, "%s", prefix + "bbb5")
        log.setLevel(log_trace)
        log.log(log_trace, "%s", prefix + "bbb6")
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
        self.assertEqual((("localhost", 514), udp), get_syslog_address("localhost:514", "UDP"))
        self.assertEqual((("localhost", 514), tcp), get_syslog_address("localhost:514", "TCP"))
        self.assertEqual(("/dev/log", None), get_syslog_address("/dev/log", "UDP"))
        self.assertEqual(("/dev/log", None), get_syslog_address("/dev/log", "TCP"))

    def test_validate_log_config_variable(self) -> None:
        self.assertIsNone(validate_log_config_variable("name:value"))
        for var in ["noColon", ":noName", "$n:v", "{:v", "}:v", "", "  ", "\t", "a\tb:v", "spa ce:v", '"k":v', "'k':v"]:
            self.assertIsNotNone(validate_log_config_variable(var))

    def test_log_config_file_validation(self) -> None:
        args = self.argparser_parse_args(["src", "dst", "--log-config-file", "+bad_file_name.txt"])
        log_params = LogParams(args)
        with self.assertRaises(SystemExit):
            get_dict_config_logger(log_params, args)


#############################################################################
class TestLogging(AbstractTestCase):

    def test_get_default_logger(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        lp = LogParams(args)
        logging.getLogger(get_logger_subname()).handlers.clear()
        logging.getLogger(bzfs.__name__).handlers.clear()
        log = get_default_logger(lp, args)
        self.assertTrue(any(isinstance(h, logging.StreamHandler) for h in log.handlers))
        self.assertTrue(any(isinstance(h, logging.FileHandler) for h in log.handlers))

    def test_get_default_logger_considers_existing_sublog_handlers(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        lp = LogParams(args)
        sublog = logging.getLogger(get_logger_subname())
        sublog.handlers.clear()
        log = logging.getLogger(bzfs.__name__)
        log.handlers.clear()
        stream_h: logging.StreamHandler | None = None
        file_h: logging.FileHandler | None = None
        try:
            stream_h = logging.StreamHandler(stream=sys.stdout)
            file_h = logging.FileHandler(lp.log_file, encoding="utf-8")
            sublog.addHandler(stream_h)
            sublog.addHandler(file_h)
            log_result = get_default_logger(lp, args)
            self.assertEqual([], log_result.handlers)
        finally:
            if stream_h is not None:
                stream_h.close()
            if file_h is not None:
                file_h.close()
            sublog.handlers.clear()
            reset_logger()

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
        logging.getLogger(get_logger_subname()).handlers.clear()
        logger = logging.getLogger(bzfs.__name__)
        logger.handlers.clear()
        handler = logging.Handler()
        mock_syslog.return_value = handler
        try:
            with patch.object(logger, "warning") as mock_warning:
                log = get_default_logger(lp, args)
                mock_syslog.assert_called_once()
                self.assertIn(handler, log.handlers)
                mock_warning.assert_called_once()
        finally:
            reset_logger()

    def test_remove_json_comments(self) -> None:
        config_str = (
            "#c1\n" "line_without_comment\n" "line_with_trailing_hash_only #\n" "line_with_trailing_comment##tail#\n"
        )
        expected = "\nline_without_comment\nline_with_trailing_hash_only #\n" "line_with_trailing_comment#"
        self.assertEqual(expected, remove_json_comments(config_str))

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
                get_dict_config_logger(lp, args)
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
            get_dict_config_logger(lp, args)
            self.assertEqual("DEBUG", m.call_args[0][0]["root"]["level"])

    def test_get_dict_config_logger_missing_default_raises(self) -> None:
        config = '{"version": 1, "root": {"level": "${missing}"}}'
        args = self.argparser_parse_args(["src", "dst", "--log-config-file", config])
        lp = LogParams(args)
        with self.assertRaises(ValueError):
            get_dict_config_logger(lp, args)

    def test_get_dict_config_logger_invalid_name_raises(self) -> None:
        config = '{"version": 1, "root": {"level": "${bad name:INFO}"}}'
        args = self.argparser_parse_args(["src", "dst", "--log-config-file", config])
        lp = LogParams(args)
        with self.assertRaises(ValueError):
            get_dict_config_logger(lp, args)

    def test_get_dict_config_logger_invalid_path(self) -> None:
        with tempfile.NamedTemporaryFile("w", prefix="badfilename", suffix=".json", delete=False, encoding="utf-8") as f:
            f.write("{}")
            path = f.name
        try:
            args = self.argparser_parse_args(["src", "dst", f"--log-config-file=+{path}"])
            lp = LogParams(args)
            with self.assertRaises(SystemExit):
                get_dict_config_logger(lp, args)
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
                get_dict_config_logger(lp, args)
            self.assertEqual(cm.exception.code, die_status)
            self.assertIn("Disallowed callable 'os.system'", str(cm.exception))
            mock_system.assert_not_called()

        validate_log_config_dict(None)  # type: ignore[arg-type]
