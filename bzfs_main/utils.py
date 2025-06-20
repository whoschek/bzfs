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

import os
import stat
from typing import Any, IO, List, Optional


def cut(field: int = -1, separator: str = "\t", lines: Optional[List[str]] = None) -> List[str]:
    """Retains only column number 'field' in a list of TSV/CSV lines; Analog to Unix 'cut' CLI command."""
    assert lines is not None
    assert isinstance(lines, list)
    assert len(separator) == 1
    if field == 1:
        return [line[0 : line.index(separator)] for line in lines]
    elif field == 2:
        return [line[line.index(separator) + 1 :] for line in lines]
    else:
        raise ValueError("Unsupported parameter value")


def open_nofollow(
    path: str,
    mode: str = "r",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    *,
    perm: int = stat.S_IRUSR | stat.S_IWUSR,  # rw------- (owner read + write)
    **kwargs: Any,
) -> IO:
    """Behaves exactly like built‑in open(), except that it refuses to follow symlinks, i.e. raises OSError with
    errno.ELOOP/EMLINK if basename of path is a symlink."""
    if not mode:
        raise ValueError("Must have exactly one of create/read/write/append mode and at most one plus")
    flags = {
        "r": os.O_RDONLY,
        "w": os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        "a": os.O_WRONLY | os.O_CREAT | os.O_APPEND,
        "x": os.O_WRONLY | os.O_CREAT | os.O_EXCL,
    }.get(mode[0])
    if flags is None:
        raise ValueError(f"invalid mode {mode!r}")
    if "+" in mode:
        flags = (flags & ~os.O_WRONLY) | os.O_RDWR
    flags |= os.O_NOFOLLOW | os.O_CLOEXEC
    fd = os.open(path, flags=flags, mode=perm)
    try:
        return os.fdopen(fd, mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline, **kwargs)
    except Exception:
        os.close(fd)
        raise
