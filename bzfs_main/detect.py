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
"""Detection of ZFS features and system capabilities on local and remote hosts."""

from __future__ import (
    annotations,
)
import re
import subprocess
import sys
import threading
import time
from dataclasses import (
    dataclass,
    field,
)
from subprocess import (
    DEVNULL,
    PIPE,
)
from typing import (
    TYPE_CHECKING,
    Final,
)

from bzfs_main.connection import (
    DEDICATED,
    SHARED,
    ConnectionPools,
    run_ssh_command,
    try_ssh_command,
)
from bzfs_main.utils import (
    LOG_TRACE,
    PROG_NAME,
    SynchronousExecutor,
    die,
    drain,
    list_formatter,
    stderr_to_str,
    xprint,
)

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from bzfs_main.bzfs import (
        Job,
    )
    from bzfs_main.configuration import (
        Params,
        Remote,
    )

# constants:
DISABLE_PRG: Final[str] = "-"
DUMMY_DATASET: Final[str] = "dummy"
ZFS_VERSION_IS_AT_LEAST_2_1_0: Final[str] = "zfs>=2.1.0"
ZFS_VERSION_IS_AT_LEAST_2_2_0: Final[str] = "zfs>=2.2.0"


#############################################################################
@dataclass(frozen=True)
class RemoteConfCacheItem:
    """Caches detected programs, zpool features and connection pools, per remote."""

    connection_pools: ConnectionPools
    available_programs: dict[str, str]
    zpool_features: dict[str, dict[str, str]]
    timestamp_nanos: int = field(default_factory=time.monotonic_ns)


def detect_available_programs(job: Job) -> None:
    """Detects programs, zpool features and connection pools for local and remote hosts."""
    p = params = job.params
    log = p.log
    available_programs: dict[str, dict[str, str]] = params.available_programs
    if "local" not in available_programs:
        cmd: list[str] = [p.shell_program_local, "-c", _find_available_programs(p)]
        sp = job.subprocesses
        proc = sp.subprocess_run(cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True, log=log)
        xprint(log=log, value=stderr_to_str(proc.stderr), file=sys.stderr, end="")
        stdout: str = proc.stdout
        available_programs["local"] = dict.fromkeys(stdout.splitlines(), "")
        cmd = [p.shell_program_local, "-c", "exit"]
        proc = sp.subprocess_run(cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True, log=log)
        xprint(log=log, value=stderr_to_str(proc.stderr), file=sys.stderr, end="")
        if proc.returncode != 0:
            _disable_program(p, "sh", ["local"])

    todo: list[Remote] = []
    for r in [p.dst, p.src]:
        loc: str = r.location
        remote_conf_cache_key: tuple = r.cache_key()
        cache_item: RemoteConfCacheItem | None = job.remote_conf_cache.get(remote_conf_cache_key)
        if cache_item is not None:
            # startup perf: cache avoids ssh connect setup and feature detection roundtrips on revisits to same site
            p.connection_pools[loc] = cache_item.connection_pools
            p.available_programs[loc] = cache_item.available_programs
            p.zpool_features[loc] = cache_item.zpool_features
            if time.monotonic_ns() - cache_item.timestamp_nanos < p.remote_conf_cache_ttl_nanos:
                if r.pool in cache_item.zpool_features:
                    continue  # cache hit, skip remote detection
            else:
                p.zpool_features[loc] = {}  # cache miss, invalidate features of zpools before refetching from remote
        else:
            p.connection_pools[loc] = ConnectionPools(
                r, {SHARED: r.max_concurrent_ssh_sessions_per_tcp_connection, DEDICATED: 1}
            )
        todo.append(r)

    lock: threading.Lock = threading.Lock()

    def run_detect(r: Remote) -> None:  # thread-safe
        loc: str = r.location
        remote_conf_cache_key: tuple = r.cache_key()
        available_programs: dict[str, str] = _detect_available_programs_remote(job, r, r.ssh_user_host)
        zpool_features: dict[str, str] = _detect_zpool_features(job, r, available_programs)
        with lock:
            r.params.available_programs[loc] = available_programs
            r.params.zpool_features[loc][r.pool] = zpool_features
            job.remote_conf_cache[remote_conf_cache_key] = RemoteConfCacheItem(
                p.connection_pools[loc], available_programs, r.params.zpool_features[loc]
            )
        if r.use_zfs_delegation and zpool_features.get("delegation") == "off":
            die(
                f"Permission denied as ZFS delegation is disabled for {r.location} "
                f"dataset: {r.basis_root_dataset}. Manually enable it via 'sudo zpool set delegation=on {r.pool}'"
            )

    with SynchronousExecutor.executor_for(max_workers=max(1, len(todo))) as executor:
        drain(executor.map(run_detect, todo))  # detect ZFS features + system capabilities on src+dst in parallel

    locations = ["src", "dst", "local"]
    if params.compression_program == DISABLE_PRG:
        _disable_program(p, "zstd", locations)
    if params.mbuffer_program == DISABLE_PRG:
        _disable_program(p, "mbuffer", locations)
    if params.ps_program == DISABLE_PRG:
        _disable_program(p, "ps", locations)
    if params.pv_program == DISABLE_PRG:
        _disable_program(p, "pv", locations)
    if params.shell_program == DISABLE_PRG:
        _disable_program(p, "sh", locations)
    if params.sudo_program == DISABLE_PRG:
        _disable_program(p, "sudo", locations)
    if params.zpool_program == DISABLE_PRG:
        _disable_program(p, "zpool", locations)

    for key, programs in available_programs.items():
        for program in list(programs.keys()):
            if program.startswith("uname-"):
                # uname-Linux foo 5.15.0-69-generic #76-Ubuntu SMP Fri Mar 17 17:19:29 UTC 2023 x86_64 x86_64 x86_64 GNU/Linux
                # uname-FreeBSD freebsd 14.1-RELEASE FreeBSD 14.1-RELEASE releng/14.1-n267679-10e31f0946d8 GENERIC amd64
                # uname-Darwin foo 23.6.0 Darwin Kernel Version 23.6.0: Mon Jul 29 21:13:04 PDT 2024; root:xnu-10063.141.2~1/RELEASE_ARM64_T6020 arm64
                programs.pop(program)
                uname: str = program[len("uname-") :]
                programs["uname"] = uname
                log.log(LOG_TRACE, f"available_programs[{key}][uname]: %s", uname)
                programs["os"] = uname.split(" ", maxsplit=1)[0]  # Linux|FreeBSD|Darwin
                log.log(LOG_TRACE, f"available_programs[{key}][os]: %s", programs["os"])
            elif program.startswith("default_shell-"):
                programs.pop(program)
                default_shell: str = program[len("default_shell-") :]
                programs["default_shell"] = default_shell
                log.log(LOG_TRACE, f"available_programs[{key}][default_shell]: %s", default_shell)
                ssh_user_host = p.src.ssh_user_host if key == "src" else p.dst.ssh_user_host if key == "dst" else ""
                _validate_default_shell(default_shell, key, ssh_user_host)
            elif program.startswith("getconf_cpu_count-"):
                programs.pop(program)
                getconf_cpu_count: str = program[len("getconf_cpu_count-") :]
                programs["getconf_cpu_count"] = getconf_cpu_count
                log.log(LOG_TRACE, f"available_programs[{key}][getconf_cpu_count]: %s", getconf_cpu_count)

    for key, programs in available_programs.items():
        log.debug(f"available_programs[{key}]: %s", list_formatter(programs, separator=", "))

    for r in [p.dst, p.src]:
        if is_dummy(r):
            continue
        if r.sudo and not p.is_program_available("sudo", r.location):
            die(f"{p.sudo_program} CLI is not available on {r.location} host: {r.ssh_user_host or 'localhost'}")

    if (
        len(p.args.preserve_properties) > 0
        and any(prop in p.zfs_send_program_opts for prop in ["--props", "-p"])
        and not p.is_program_available(ZFS_VERSION_IS_AT_LEAST_2_2_0, p.dst.location)
    ):
        die(
            "Cowardly refusing to proceed as --preserve-properties is unreliable on destination ZFS < 2.2.0 when using "
            "'zfs send --props'. Either upgrade destination ZFS, or remove '--props' from --zfs-send-program-opt(s)."
        )


def _disable_program(p: Params, program: str, locations: list[str]) -> None:
    """Removes the given program from the available_programs mapping."""
    for location in locations:
        p.available_programs[location].pop(program, None)


def _find_available_programs(p: Params) -> str:
    """POSIX shell script that checks for the existence of various programs; It uses `if` statements instead of `&&` plus
    `printf` instead of `echo` to ensure maximum compatibility across shells."""
    cmds: list[str] = []
    cmds.append("printf 'default_shell-%s\n' \"$SHELL\"")
    cmds.append("if command -v echo > /dev/null; then printf 'echo\n'; fi")
    cmds.append(f"if command -v {p.zpool_program} > /dev/null; then printf 'zpool\n'; fi")
    cmds.append(f"if command -v {p.ssh_program} > /dev/null; then printf 'ssh\n'; fi")
    cmds.append(f"if command -v {p.shell_program} > /dev/null; then printf 'sh\n'; fi")
    cmds.append(f"if command -v {p.sudo_program} > /dev/null; then printf 'sudo\n'; fi")
    cmds.append(f"if command -v {p.compression_program} > /dev/null; then printf 'zstd\n'; fi")
    cmds.append(f"if command -v {p.mbuffer_program} > /dev/null; then printf 'mbuffer\n'; fi")
    cmds.append(f"if command -v {p.pv_program} > /dev/null; then printf 'pv\n'; fi")
    cmds.append(f"if command -v {p.ps_program} > /dev/null; then printf 'ps\n'; fi")
    cmds.append(
        f"if command -v {p.getconf_program} > /dev/null; then "
        f"printf 'getconf_cpu_count-'; {p.getconf_program} _NPROCESSORS_ONLN; "
        "fi"
    )
    cmds.append(f"if command -v {p.uname_program} > /dev/null; then printf 'uname-'; {p.uname_program} -a || true; fi")
    return "; ".join(cmds)


def _detect_available_programs_remote(job: Job, remote: Remote, ssh_user_host: str) -> dict[str, str]:
    """Detects CLI tools available on ``remote`` and updates mapping correspondingly."""
    p, log = job.params, job.params.log
    location = remote.location
    available_programs_minimum = {"sudo": ""}
    available_programs: dict[str, str] = {}
    if is_dummy(remote):
        return available_programs
    lines: str | None = None
    try:
        # on Linux, 'zfs --version' returns with zero status and prints the correct info
        # on FreeBSD, 'zfs --version' always prints the same (correct) info as Linux, but nonetheless sometimes
        # returns with non-zero status (sometimes = if the zfs kernel module is not loaded)
        lines = run_ssh_command(job, remote, LOG_TRACE, print_stderr=False, cmd=[p.zfs_program, "--version"])
        assert lines
    except (FileNotFoundError, PermissionError):  # location is local and program file was not found
        die(f"{p.zfs_program} CLI is not available on {location} host: {ssh_user_host or 'localhost'}")
    except subprocess.CalledProcessError as e:
        if "unrecognized command '--version'" in e.stderr and "run: zfs help" in e.stderr:
            die(f"Unsupported ZFS platform: {e.stderr}")  # solaris is unsupported
        elif not e.stdout.startswith("zfs"):
            die(f"{p.zfs_program} CLI is not available on {location} host: {ssh_user_host or 'localhost'}")
        else:
            lines = e.stdout  # FreeBSD if the zfs kernel module is not loaded
            assert lines
    if lines:
        # Examples that should parse: "zfs-2.1.5~rc5-ubuntu3", "zfswin-2.2.3rc5"
        first_line: str = lines.splitlines()[0] if lines.splitlines() else ""
        match = re.search(r"(\d+)\.(\d+)\.(\d+)", first_line)
        if not match:
            die("Unparsable zfs version string: '" + first_line + "'")
        version = ".".join(match.groups())
        available_programs["zfs"] = version
        if is_version_at_least(version, "2.1.0"):
            available_programs[ZFS_VERSION_IS_AT_LEAST_2_1_0] = ""
        if is_version_at_least(version, "2.2.0"):
            available_programs[ZFS_VERSION_IS_AT_LEAST_2_2_0] = ""
    log.log(LOG_TRACE, f"available_programs[{location}][zfs]: %s", available_programs["zfs"])

    if p.shell_program != DISABLE_PRG:
        try:
            cmd: list[str] = [p.shell_program, "-c", _find_available_programs(p)]
            stdout: str = run_ssh_command(job, remote, LOG_TRACE, cmd=cmd)
            available_programs.update(dict.fromkeys(stdout.splitlines(), ""))
            return available_programs
        except (FileNotFoundError, PermissionError) as e:  # location is local and shell program file was not found
            if e.filename != p.shell_program:
                raise
        except subprocess.CalledProcessError:
            pass
        log.warning("%s", f"Failed to find {p.shell_program} on {location}. Continuing with minimal assumptions...")
    available_programs.update(available_programs_minimum)
    return available_programs


def is_dummy(r: Remote) -> bool:
    """Returns True if ``remote`` refers to the synthetic dummy dataset."""
    return r.root_dataset == DUMMY_DATASET


def _detect_zpool_features(job: Job, remote: Remote, available_programs: dict) -> dict[str, str]:
    """Fills ``job.params.zpool_features`` with detected zpool capabilities."""
    p = params = job.params
    r, loc, log = remote, remote.location, p.log
    lines: list[str] = []
    features: dict[str, str] = {}
    if is_dummy(r):
        return {}
    if params.zpool_program != DISABLE_PRG and (params.shell_program == DISABLE_PRG or "zpool" in available_programs):
        cmd: list[str] = params.split_args(f"{params.zpool_program} get -Hp -o property,value all", r.pool)
        try:
            lines = run_ssh_command(job, remote, LOG_TRACE, check=False, cmd=cmd).splitlines()
        except (FileNotFoundError, PermissionError) as e:
            if e.filename != params.zpool_program:
                raise
            log.warning("%s", f"Failed to detect zpool features on {loc}: {r.pool}. Continuing with minimal assumptions ...")
        else:
            props: dict[str, str] = dict(line.split("\t", 1) for line in lines)
            features = {k: v for k, v in props.items() if k.startswith("feature@") or k == "delegation"}
    if len(lines) == 0:
        cmd = p.split_args(f"{p.zfs_program} list -t filesystem -Hp -o name -s name", r.pool)
        if try_ssh_command(job, remote, LOG_TRACE, cmd=cmd) is None:
            die(f"Pool does not exist for {loc} dataset: {r.basis_root_dataset}. Manually create the pool first!")
    return features


def is_zpool_feature_enabled_or_active(p: Params, remote: Remote, feature: str) -> bool:
    """Returns True if the given zpool feature is active or enabled on ``remote``."""
    return p.zpool_features[remote.location][remote.pool].get(feature) in ("active", "enabled")


def are_bookmarks_enabled(p: Params, remote: Remote) -> bool:
    """Checks if bookmark related features are enabled on ``remote``."""
    return is_zpool_feature_enabled_or_active(p, remote, "feature@bookmark_v2") and is_zpool_feature_enabled_or_active(
        p, remote, "feature@bookmark_written"
    )


def is_caching_snapshots(p: Params, remote: Remote) -> bool:
    """Returns True if snapshot caching is supported and enabled on ``remote``."""
    return p.is_caching_snapshots and p.is_program_available(ZFS_VERSION_IS_AT_LEAST_2_2_0, remote.location)


def is_version_at_least(version_str: str, min_version_str: str) -> bool:
    """Checks if the version string is at least the minimum version string."""
    return tuple(map(int, version_str.split("."))) >= tuple(map(int, min_version_str.split(".")))


def _validate_default_shell(path_to_default_shell: str, location: str, ssh_user_host: str) -> None:
    """Fails if the remote user uses csh or tcsh as the default shell."""
    if path_to_default_shell in ("csh", "tcsh") or path_to_default_shell.endswith(("/csh", "/tcsh")):
        # On some old FreeBSD systems the default shell is still csh. Also see https://www.grymoire.com/unix/CshTop10.txt
        die(
            f"Cowardly refusing to proceed because {PROG_NAME} is not compatible with csh-style quoting of special "
            f"characters. The safe workaround is to first manually set 'sh' instead of '{path_to_default_shell}' as "
            f"the default shell of the Unix user on {location} host: {ssh_user_host or 'localhost'}, like so: "
            "chsh -s /bin/sh YOURUSERNAME"
        )
