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
"""Detection of runtime features and properties on local and remote hosts."""

from __future__ import annotations
import platform
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from subprocess import DEVNULL, PIPE
from typing import (
    TYPE_CHECKING,
)

from bzfs_main.connection import (
    DEDICATED,
    SHARED,
    ConnectionPools,
    run_ssh_command,
    try_ssh_command,
)
from bzfs_main.utils import (
    die,
    list_formatter,
    log_trace,
    prog_name,
)

if TYPE_CHECKING:  # pragma: no cover
    from bzfs_main.bzfs import Job, Params, Remote

# constants:
disable_prg = "-"
dummy_dataset = "dummy"
zfs_version_is_at_least_2_1_0 = "zfs>=2.1.0"
zfs_version_is_at_least_2_2_0 = "zfs>=2.2.0"


#############################################################################
@dataclass(frozen=True)
class RemoteConfCacheItem:
    """Caches detected programs, zpool features and connection pools, per remote."""

    connection_pools: ConnectionPools
    available_programs: dict[str, str]
    zpool_features: dict[str, str]
    timestamp_nanos: int = field(default_factory=time.monotonic_ns)


def detect_available_programs(job: Job) -> None:
    """Detects programs, zpool features and connection pools for local and remote hosts."""
    p = params = job.params
    log = p.log
    available_programs = params.available_programs
    if "local" not in available_programs:
        cmd = [p.shell_program_local, "-c", find_available_programs(p)]
        available_programs["local"] = dict.fromkeys(
            subprocess.run(cmd, stdin=DEVNULL, stdout=PIPE, stderr=sys.stderr, text=True).stdout.splitlines(), ""
        )
        cmd = [p.shell_program_local, "-c", "exit"]
        if subprocess.run(cmd, stdin=DEVNULL, stdout=PIPE, stderr=sys.stderr, text=True).returncode != 0:
            disable_program(p, "sh", ["local"])

    for r in [p.dst, p.src]:
        loc = r.location
        remote_conf_cache_key = r.cache_key()
        cache_item: RemoteConfCacheItem | None = job.remote_conf_cache.get(remote_conf_cache_key)
        if cache_item is not None:
            # startup perf: cache avoids ssh connect setup and feature detection roundtrips on revisits to same site
            p.connection_pools[loc] = cache_item.connection_pools
            if time.monotonic_ns() - cache_item.timestamp_nanos < p.remote_conf_cache_ttl_nanos:
                available_programs[loc] = cache_item.available_programs
                p.zpool_features[loc] = cache_item.zpool_features
                continue  # cache hit, skip remote detection
        else:
            p.connection_pools[loc] = ConnectionPools(
                r, {SHARED: r.max_concurrent_ssh_sessions_per_tcp_connection, DEDICATED: 1}
            )
        detect_zpool_features(job, r)
        detect_available_programs_remote(job, r, available_programs, r.ssh_user_host)
        job.remote_conf_cache[remote_conf_cache_key] = RemoteConfCacheItem(
            p.connection_pools[loc], available_programs[loc], p.zpool_features[loc]
        )
        if r.use_zfs_delegation and p.zpool_features[loc].get("delegation") == "off":
            die(
                f"Permission denied as ZFS delegation is disabled for {r.location} "
                f"dataset: {r.basis_root_dataset}. Manually enable it via 'sudo zpool set delegation=on {r.pool}'"
            )

    locations = ["src", "dst", "local"]
    if params.compression_program == disable_prg:
        disable_program(p, "zstd", locations)
    if params.mbuffer_program == disable_prg:
        disable_program(p, "mbuffer", locations)
    if params.ps_program == disable_prg:
        disable_program(p, "ps", locations)
    if params.pv_program == disable_prg:
        disable_program(p, "pv", locations)
    if params.shell_program == disable_prg:
        disable_program(p, "sh", locations)
    if params.sudo_program == disable_prg:
        disable_program(p, "sudo", locations)
    if params.zpool_program == disable_prg:
        disable_program(p, "zpool", locations)

    for key, programs in available_programs.items():
        for program in list(programs.keys()):
            if program.startswith("uname-"):
                # uname-Linux foo 5.15.0-69-generic #76-Ubuntu SMP Fri Mar 17 17:19:29 UTC 2023 x86_64 x86_64 x86_64 GNU/Linux
                # uname-FreeBSD freebsd 14.1-RELEASE FreeBSD 14.1-RELEASE releng/14.1-n267679-10e31f0946d8 GENERIC amd64
                # uname-SunOS solaris 5.11 11.4.42.111.0 i86pc i386 i86pc # https://blogs.oracle.com/solaris/post/building-open-source-software-on-oracle-solaris-114-cbe-release
                # uname-SunOS solaris 5.11 11.4.0.15.0 i86pc i386 i86pc
                # uname-Darwin foo 23.6.0 Darwin Kernel Version 23.6.0: Mon Jul 29 21:13:04 PDT 2024; root:xnu-10063.141.2~1/RELEASE_ARM64_T6020 arm64
                programs.pop(program)
                uname = program[len("uname-") :]
                programs["uname"] = uname
                log.log(log_trace, f"available_programs[{key}][uname]: %s", uname)
                programs["os"] = uname.split(" ")[0]  # Linux|FreeBSD|SunOS|Darwin
                log.log(log_trace, f"available_programs[{key}][os]: %s", programs["os"])
            elif program.startswith("default_shell-"):
                programs.pop(program)
                default_shell = program[len("default_shell-") :]
                programs["default_shell"] = default_shell
                log.log(log_trace, f"available_programs[{key}][default_shell]: %s", default_shell)
                validate_default_shell(default_shell, r)
            elif program.startswith("getconf_cpu_count-"):
                programs.pop(program)
                getconf_cpu_count = program[len("getconf_cpu_count-") :]
                programs["getconf_cpu_count"] = getconf_cpu_count
                log.log(log_trace, f"available_programs[{key}][getconf_cpu_count]: %s", getconf_cpu_count)

    for key, programs in available_programs.items():
        log.debug(f"available_programs[{key}]: %s", list_formatter(programs, separator=", "))

    for r in [p.dst, p.src]:
        if r.sudo and not p.is_program_available("sudo", r.location):
            die(f"{p.sudo_program} CLI is not available on {r.location} host: {r.ssh_user_host or 'localhost'}")

    if (
        len(p.args.preserve_properties) > 0
        and any(prop in p.zfs_send_program_opts for prop in ["--props", "-p"])
        and not p.is_program_available(zfs_version_is_at_least_2_2_0, p.dst.location)
    ):
        die(
            "Cowardly refusing to proceed as --preserve-properties is unreliable on destination ZFS < 2.2.0 when using "
            "'zfs send --props'. Either upgrade destination ZFS, or remove '--props' from --zfs-send-program-opt(s)."
        )


def disable_program(p: Params, program: str, locations: list[str]) -> None:
    """Removes the given program from the available_programs mapping."""
    for location in locations:
        p.available_programs[location].pop(program, None)


def find_available_programs(p: Params) -> str:
    """POSIX shell script that checks for the existence of various programs; It uses `if` statements instead of `&&` plus
    `printf` instead of `echo` to ensure maximum compatibility across shells."""
    cmds = []
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
        f"if command -v {p.psrinfo_program} > /dev/null; then "
        f"printf 'getconf_cpu_count-'; {p.psrinfo_program} -p; "
        f"elif command -v {p.getconf_program} > /dev/null; then "
        f"printf 'getconf_cpu_count-'; {p.getconf_program} _NPROCESSORS_ONLN; "
        "fi"
    )
    cmds.append(f"if command -v {p.uname_program} > /dev/null; then printf 'uname-'; {p.uname_program} -a || true; fi")
    return "; ".join(cmds)


def detect_available_programs_remote(job: Job, remote: Remote, available_programs: dict, ssh_user_host: str) -> None:
    """Detects CLI tools available on ``remote`` and updates mapping correspondingly."""
    p, log = job.params, job.params.log
    location = remote.location
    available_programs_minimum = {"zpool": None, "sudo": None}
    available_programs[location] = {}
    lines = None
    try:
        # on Linux, 'zfs --version' returns with zero status and prints the correct info
        # on FreeBSD, 'zfs --version' always prints the same (correct) info as Linux, but nonetheless sometimes
        # returns with non-zero status (sometimes = if the zfs kernel module is not loaded)
        # on Solaris, 'zfs --version' returns with non-zero status without printing useful info as the --version
        # option is not known there
        lines = run_ssh_command(job, remote, log_trace, print_stderr=False, cmd=[p.zfs_program, "--version"])
        assert lines
    except (FileNotFoundError, PermissionError):  # location is local and program file was not found
        die(f"{p.zfs_program} CLI is not available on {location} host: {ssh_user_host or 'localhost'}")
    except subprocess.CalledProcessError as e:
        if "unrecognized command '--version'" in e.stderr and "run: zfs help" in e.stderr:
            available_programs[location]["zfs"] = "notOpenZFS"  # solaris-11.4 zfs does not know --version flag
        elif not e.stdout.startswith("zfs"):
            die(f"{p.zfs_program} CLI is not available on {location} host: {ssh_user_host or 'localhost'}")
        else:
            lines = e.stdout  # FreeBSD if the zfs kernel module is not loaded
            assert lines
    if lines:
        line = lines.splitlines()[0]
        assert line.startswith("zfs")
        # Example: zfs-2.1.5~rc5-ubuntu3 -> 2.1.5, zfswin-2.2.3rc5 -> 2.2.3
        version = line.split("-")[1].strip()
        match = re.fullmatch(r"(\d+\.\d+\.\d+).*", version)
        assert match, "Unparsable zfs version string: " + version
        version = match.group(1)
        available_programs[location]["zfs"] = version
        if is_version_at_least(version, "2.1.0"):
            available_programs[location][zfs_version_is_at_least_2_1_0] = True
        if is_version_at_least(version, "2.2.0"):
            available_programs[location][zfs_version_is_at_least_2_2_0] = True
    log.log(log_trace, f"available_programs[{location}][zfs]: %s", available_programs[location]["zfs"])

    if p.shell_program != disable_prg:
        try:
            cmd = [p.shell_program, "-c", find_available_programs(p)]
            available_programs[location].update(dict.fromkeys(run_ssh_command(job, remote, log_trace, cmd=cmd).splitlines()))
            return
        except (FileNotFoundError, PermissionError) as e:  # location is local and shell program file was not found
            if e.filename != p.shell_program:
                raise
        except subprocess.CalledProcessError:
            pass
        log.warning("%s", f"Failed to find {p.shell_program} on {location}. Continuing with minimal assumptions...")
    available_programs[location].update(available_programs_minimum)


def is_solaris_zfs(p: Params, remote: Remote) -> bool:
    """Returns True if the remote ZFS implementation uses Solaris ZFS."""
    return is_solaris_zfs_location(p, remote.location)


def is_solaris_zfs_location(p: Params, location: str) -> bool:
    """Returns True if ``location`` uses Solaris ZFS."""
    if location == "local":
        return platform.system() == "SunOS"
    return p.available_programs[location].get("zfs") == "notOpenZFS"


def is_dummy(r: Remote) -> bool:
    """Returns True if ``remote`` refers to the synthetic dummy dataset."""
    return r.root_dataset == dummy_dataset


def detect_zpool_features(job: Job, remote: Remote) -> None:
    """Fills ``job.params.zpool_features`` with detected zpool capabilities."""
    p = params = job.params
    r, loc, log = remote, remote.location, p.log
    lines = []
    features = {}
    params.zpool_features.pop(loc, None)
    if is_dummy(r):
        params.zpool_features[loc] = {}
        return
    if params.zpool_program != disable_prg:
        cmd = params.split_args(f"{params.zpool_program} get -Hp -o property,value all", r.pool)
        try:
            lines = run_ssh_command(job, remote, log_trace, check=False, cmd=cmd).splitlines()
        except (FileNotFoundError, PermissionError) as e:
            if e.filename != params.zpool_program:
                raise
            log.warning("%s", f"Failed to detect zpool features on {loc}: {r.pool}. Continuing with minimal assumptions ...")
        else:
            props = {line.split("\t", 1)[0]: line.split("\t", 1)[1] for line in lines}
            features = {k: v for k, v in props.items() if k.startswith("feature@") or k == "delegation"}
    if len(lines) == 0:
        cmd = p.split_args(f"{p.zfs_program} list -t filesystem -Hp -o name -s name", r.pool)
        if try_ssh_command(job, remote, log_trace, cmd=cmd) is None:
            die(f"Pool does not exist for {loc} dataset: {r.basis_root_dataset}. Manually create the pool first!")
    params.zpool_features[loc] = features


def is_zpool_feature_enabled_or_active(p: Params, remote: Remote, feature: str) -> bool:
    """Returns True if the given zpool feature is active or enabled on ``remote``."""
    return p.zpool_features[remote.location].get(feature) in ("active", "enabled")


def are_bookmarks_enabled(p: Params, remote: Remote) -> bool:
    """Checks if bookmark related features are enabled on ``remote``."""
    return is_zpool_feature_enabled_or_active(p, remote, "feature@bookmark_v2") and is_zpool_feature_enabled_or_active(
        p, remote, "feature@bookmark_written"
    )


def is_caching_snapshots(p: Params, remote: Remote) -> bool:
    """Returns True if snapshot caching is supported and enabled on ``remote``."""
    return (
        p.is_caching_snapshots
        and p.is_program_available(zfs_version_is_at_least_2_2_0, remote.location)
        and is_zpool_feature_enabled_or_active(p, remote, "feature@extensible_dataset")
    )


def is_version_at_least(version_str: str, min_version_str: str) -> bool:
    """Checks if the version string is at least the minimum version string."""
    return tuple(map(int, version_str.split("."))) >= tuple(map(int, min_version_str.split(".")))


def validate_default_shell(path_to_default_shell: str, r: Remote) -> None:
    """Fails if the remote user uses csh or tcsh as the default shell."""
    if path_to_default_shell.endswith(("/csh", "/tcsh")):
        # On some old FreeBSD systems the default shell is still csh. Also see https://www.grymoire.com/unix/CshTop10.txt
        die(
            f"Cowardly refusing to proceed because {prog_name} is not compatible with csh-style quoting of special "
            f"characters. The safe workaround is to first manually set 'sh' instead of '{path_to_default_shell}' as "
            f"the default shell of the Unix user on {r.location} host: {r.ssh_user_host or 'localhost'}, like so: "
            "chsh -s /bin/sh YOURUSERNAME"
        )
