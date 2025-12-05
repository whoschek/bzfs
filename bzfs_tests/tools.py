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
"""Various small tools for use in tests; Everything in this module relies only on the Python standard library so other
modules remain dependency free."""

from __future__ import (
    annotations,
)
import contextlib
import gc
import inspect
import io
import logging
import types
import unittest
from collections.abc import (
    Iterator,
)
from typing import (
    Any,
    Callable,
)


@contextlib.contextmanager
def stop_on_failure_subtest(**params: Any) -> Iterator[None]:
    """Context manager to mimic UnitTest.subTest() but stop on first failure."""
    try:
        yield
    except AssertionError as e:
        raise AssertionError(f"SubTest failed with parameters: {params}") from e


@contextlib.contextmanager
def suppress_output() -> Iterator[None]:
    """Silence stdout/stderr and temporarily disable logging to keep test output clean."""
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        old_disable = logging.root.manager.disable
        try:
            logging.disable(logging.CRITICAL)
            yield
        finally:
            logging.disable(old_disable)


@contextlib.contextmanager
def suppress_logger(log: logging.Logger) -> Iterator[None]:
    """Temporarily remove handlers from the given logger to prevent console output to keep test output clean."""
    previous_handlers: list[logging.Handler] = log.handlers.copy()
    previous_level: int = log.level
    previous_propagate: bool = log.propagate
    try:
        for h in previous_handlers:
            log.removeHandler(h)
        yield
    finally:
        for h in previous_handlers:
            log.addHandler(h)
        log.setLevel(previous_level)
        log.propagate = previous_propagate


@contextlib.contextmanager
def capture_stderr() -> Iterator[io.StringIO]:
    """Capture stderr output for later inspection within a test."""
    buf = io.StringIO()
    with contextlib.redirect_stderr(buf):
        yield buf


@contextlib.contextmanager
def capture_stdout() -> Iterator[io.StringIO]:
    """Capture stdout output for later inspection within a test."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        yield buf


@contextlib.contextmanager
def gc_disabled(run_gc_first: bool = False) -> Iterator[None]:
    """Temporarily disables Python's automatic cyclic GC within a with-block; for more accurate benchmarking."""
    if run_gc_first:
        gc.collect()
    was_enabled: bool = gc.isenabled()
    try:
        if was_enabled:
            gc.disable()
        yield
    finally:
        if was_enabled:
            gc.enable()


#############################################################################
class TestSuiteCompleteness(unittest.TestCase):
    """Verifies each test module's suite() includes all locally defined test classes to avoid accidentally orphaned tests."""

    def __init__(
        self,
        method_name: str = "runTest",
        modules: list[types.ModuleType] | None = None,
        class_predicate: Callable[[type[unittest.TestCase]], bool] | None = None,
    ) -> None:
        """Assumes each module in ``modules`` expose a ``suite()`` function and ``class_predicate`` returns True for classes
        that must appear in that suite."""
        super().__init__(method_name)
        self.modules = modules or []
        self.class_predicate = class_predicate or (lambda _cls: False)

    def test_all_modules_have_a_complete_suite(self) -> None:
        failures: list[str] = []
        for module in self.modules:
            # Discover locally defined TestCase classes starting with 'Test'
            local_classes: set[str] = set()
            for _, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, unittest.TestCase) and obj.__module__ == module.__name__ and self.class_predicate(obj):
                    local_classes.add(obj.__name__)

            # Collect classes actually included by the module's suite() into a flattened set
            included_classes: set[str] = set()
            stack: list[unittest.TestSuite] = [module.suite()]
            while stack:
                suite = stack.pop()
                for testcase in suite:  # may contain nested suites
                    if isinstance(testcase, unittest.TestSuite):
                        stack.append(testcase)
                    else:
                        included_classes.add(testcase.__class__.__name__)

            missing_classes = sorted(local_classes.difference(included_classes))
            if missing_classes:
                pretty = ", ".join(missing_classes)
                location = getattr(module, "__file__", module.__name__)
                failures.append(
                    f"- {module.__name__} ({location}): missing from suite(): {pretty}.\n"
                    "  Fix: add these TestCase classes to the module's suite() or adjust class_predicate()."
                )
        if failures:
            message = "Found test classes not included in their module suite():\n" + "\n".join(failures)
            self.fail(message)
