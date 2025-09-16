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
"""Various small tools for use in tests; Everything in this module relies only on the standard library so other modules
remain dependency free."""

from __future__ import (
    annotations,
)
import contextlib
import io
import logging
from typing import (
    Any,
    Iterator,
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
def capture_stderr() -> Iterator[io.StringIO]:
    """Capture stderr output for later inspection within a test."""
    buf = io.StringIO()
    with contextlib.redirect_stderr(buf):
        yield buf
