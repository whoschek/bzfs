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
"""Custom argparse actions shared by the 'bzfs' and 'bzfs_jobrunner' CLIs; These helpers validate and expand complex command
line syntax such as +file references, dataset pairs, and snapshot filters."""

from __future__ import (
    annotations,
)
import argparse
import ast
import os
import re
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    timedelta,
)
from typing import (
    Any,
    cast,
    final,
)

from bzfs_main.filter import (
    SNAPSHOT_FILTERS_VAR,
    SNAPSHOT_REGEX_FILTER_NAME,
    SNAPSHOT_REGEX_FILTER_NAMES,
    RankRange,
    UnixTimeRange,
)
from bzfs_main.util.check_range import (
    CheckRange,
)
from bzfs_main.util.utils import (
    SHELL_CHARS,
    UNIX_TIME_INFINITY_SECS,
    YEAR_WITH_FOUR_DIGITS_REGEX,
    RegexList,
    SnapshotPeriods,
    die,
    ninfix,
    nprefix,
    nsuffix,
    open_nofollow,
    parse_duration_to_milliseconds,
    unixtime_fromisoformat,
)


#############################################################################
@dataclass(order=True)
@final
class SnapshotFilter:
    """Represents a snapshot filter with matching options and time range."""

    name: str
    timerange: UnixTimeRange  # defined in bzfs_main.filter
    options: None | list[RankRange] | list[str] | tuple[list[str], list[str]] | tuple[RegexList, RegexList] = field(
        compare=False, default=None
    )


def _add_snapshot_filter(args: argparse.Namespace, _filter: SnapshotFilter) -> None:
    """Appends snapshot filter to namespace list, creating the list if absent."""
    if not hasattr(args, SNAPSHOT_FILTERS_VAR):
        args.snapshot_filters_var = [[]]
    args.snapshot_filters_var[-1].append(_filter)


def _add_time_and_rank_snapshot_filter(
    args: argparse.Namespace, dst: str, timerange: UnixTimeRange, rankranges: list[RankRange]
) -> None:
    """Creates and adds a SnapshotFilter using timerange and rank ranges."""
    if timerange is None or len(rankranges) == 0 or any(rankrange[0] == rankrange[1] for rankrange in rankranges):
        _add_snapshot_filter(args, SnapshotFilter("include_snapshot_times", timerange, None))
    else:
        assert timerange is not None
        _add_snapshot_filter(args, SnapshotFilter(dst, timerange, rankranges))


def has_timerange_filter(snapshot_filters: list[list[SnapshotFilter]]) -> bool:
    """Interacts with add_time_and_rank_snapshot_filter() and optimize_snapshot_filters()."""
    return any(f.timerange is not None for snapshot_filter in snapshot_filters for f in snapshot_filter)


def optimize_snapshot_filters(snapshot_filters: list[SnapshotFilter]) -> list[SnapshotFilter]:
    """Applies basic optimizations to the snapshot filter execution plan."""
    _merge_adjacent_snapshot_filters(snapshot_filters)
    _merge_adjacent_snapshot_regexes(snapshot_filters)
    snapshot_filters = [f for f in snapshot_filters if f.timerange or f.options]
    _reorder_snapshot_time_filters(snapshot_filters)
    return snapshot_filters


def _merge_adjacent_snapshot_filters(snapshot_filters: list[SnapshotFilter]) -> None:
    """Merge adjacent filters of the same type if possible.

    Merge filter operators of the same kind if they are next to each other and carry an option list, for example
    --include-snapshot-times-and-ranks and --include-snapshot-regex and --exclude-snapshot-regex. This improves execution
    perf and makes handling easier in later stages.
    Example: merges --include-snapshot-times-and-ranks notime oldest10% --include-snapshot-times-and-ranks notime latest20%
    into --include-snapshot-times-and-ranks notime oldest10% latest20%
    """
    i = len(snapshot_filters) - 1
    while i >= 0:
        filter_i: SnapshotFilter = snapshot_filters[i]
        if isinstance(filter_i.options, list):
            j = i - 1
            if j >= 0 and snapshot_filters[j] == filter_i:
                lst: list = cast(list, snapshot_filters[j].options)
                assert isinstance(lst, list)
                assert isinstance(filter_i.options, list)
                lst.extend(filter_i.options)
                snapshot_filters.pop(i)
        i -= 1


def _merge_adjacent_snapshot_regexes(snapshot_filters: list[SnapshotFilter]) -> None:
    """Combine consecutive regex filters of the same type for efficiency."""

    # Merge regex filter operators of the same kind as long as they are within the same group, aka as long as they are not
    # separated by a non-regex filter. This improves execution perf and makes handling easier in later stages.
    # Example: --include-snapshot-regex .*daily --exclude-snapshot-regex .*weekly --include-snapshot-regex .*hourly
    # --exclude-snapshot-regex .*monthly
    # gets merged into the following: --include-snapshot-regex .*daily .*hourly --exclude-snapshot-regex .*weekly .*monthly
    i = len(snapshot_filters) - 1
    while i >= 0:
        filter_i: SnapshotFilter = snapshot_filters[i]
        if filter_i.name in SNAPSHOT_REGEX_FILTER_NAMES:
            assert isinstance(filter_i.options, list)
            j = i - 1
            while j >= 0 and snapshot_filters[j].name in SNAPSHOT_REGEX_FILTER_NAMES:
                if snapshot_filters[j].name == filter_i.name:
                    lst: list = cast(list[str], snapshot_filters[j].options)
                    assert isinstance(lst, list)
                    assert isinstance(filter_i.options, list)
                    lst.extend(filter_i.options)
                    snapshot_filters.pop(i)
                    break
                j -= 1
        i -= 1

    # Merge --include-snapshot-regex and --exclude-snapshot-regex filters that are part of the same group (i.e. next to each
    # other) into a single combined filter operator that contains the info of both, and hence all info for the group, which
    # makes handling easier in later stages.
    # Example: --include-snapshot-regex .*daily .*hourly --exclude-snapshot-regex .*weekly .*monthly
    # gets merged into the following: snapshot-regex(excludes=[.*weekly, .*monthly], includes=[.*daily, .*hourly])
    i = len(snapshot_filters) - 1
    while i >= 0:
        filter_i = snapshot_filters[i]
        name: str = filter_i.name
        if name in SNAPSHOT_REGEX_FILTER_NAMES:
            j = i - 1
            if j >= 0 and snapshot_filters[j].name in SNAPSHOT_REGEX_FILTER_NAMES:
                filter_j = snapshot_filters[j]
                assert filter_j.name != name
                snapshot_filters.pop(i)
                i -= 1
            else:
                name_j: str = next(iter(SNAPSHOT_REGEX_FILTER_NAMES.difference({name})))
                filter_j = SnapshotFilter(name_j, None, [])
            sorted_filters: list[SnapshotFilter] = sorted([filter_i, filter_j])
            exclude_regexes = cast(list[str], sorted_filters[0].options)
            include_regexes = cast(list[str], sorted_filters[1].options)
            snapshot_filters[i] = SnapshotFilter(SNAPSHOT_REGEX_FILTER_NAME, None, (exclude_regexes, include_regexes))
        i -= 1


def _reorder_snapshot_time_filters(snapshot_filters: list[SnapshotFilter]) -> None:
    """Reorder time filters before regex filters within execution plan sections.

    In an execution plan that contains filter operators based on sort order (the --include-snapshot-times-and-ranks operator
    with non-empty ranks), filters cannot freely be reordered without violating correctness, but they can still be partially
    reordered for better execution performance.

    The filter list is partitioned into sections such that sections are separated by --include-snapshot-times-and-ranks
    operators with non-empty ranks. Within each section, we move include_snapshot_times operators aka
    --include-snapshot-times-and-ranks operators with empty ranks before --include/exclude-snapshot-regex operators because
    the former involves fast integer comparisons and the latter involves more expensive regex matching.

    Example: reorders --include-snapshot-regex .*daily --include-snapshot-times-and-ranks 2024-01-01..2024-04-01 into
    --include-snapshot-times-and-ranks 2024-01-01..2024-04-01 --include-snapshot-regex .*daily
    """

    def reorder_time_filters_within_section(i: int, j: int) -> None:
        while j > i:
            filter_j: SnapshotFilter = snapshot_filters[j]
            if filter_j.name == "include_snapshot_times":
                snapshot_filters.pop(j)
                snapshot_filters.insert(i + 1, filter_j)
            j -= 1

    i = len(snapshot_filters) - 1
    j = i
    while i >= 0:
        name: str = snapshot_filters[i].name
        if name == "include_snapshot_times_and_ranks":
            reorder_time_filters_within_section(i, j)
            j = i - 1
        i -= 1
    reorder_time_filters_within_section(i, j)


def validate_no_argument_file(
    path: str, namespace: argparse.Namespace, err_prefix: str, parser: argparse.ArgumentParser | None = None
) -> None:
    """Checks that command line options do not include +file when disabled."""
    if getattr(namespace, "no_argument_file", False):
        die(f"{err_prefix}Argument file inclusion is disabled: {path}", parser=parser)


#############################################################################
@final
class NonEmptyStringAction(argparse.Action):
    """Argparse action rejecting empty string values."""

    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        """Strip whitespace and reject empty values."""
        values = values.strip()
        if values == "":
            parser.error(f"{option_string}: Empty string is not valid")
        setattr(namespace, self.dest, values)


#############################################################################
@final
class DatasetPairsAction(argparse.Action):
    """Parses alternating source/destination dataset arguments."""

    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        """Validates dataset pair arguments and expand '+file' notation."""
        datasets: list[str] = []
        err_prefix: str = f"{option_string or self.dest}: "

        for value in values:
            if not value.startswith("+"):
                datasets.append(value)
            else:
                path: str = value[1:]
                validate_no_argument_file(path, namespace, err_prefix=err_prefix, parser=parser)
                if "bzfs_argument_file" not in os.path.basename(path):
                    parser.error(f"{err_prefix}basename must contain substring 'bzfs_argument_file': {path}")
                try:
                    with open_nofollow(path, "r", encoding="utf-8") as fd:
                        for i, line in enumerate(fd.read().splitlines()):
                            if line.startswith("#") or not line.strip():
                                continue
                            splits: list[str] = line.split("\t", 1)
                            if len(splits) <= 1:
                                parser.error(f"{err_prefix}Line must contain tab-separated SRC_DATASET and DST_DATASET: {i}")
                            src_root_dataset, dst_root_dataset = splits
                            if not src_root_dataset.strip() or not dst_root_dataset.strip():
                                parser.error(
                                    f"{err_prefix}SRC_DATASET and DST_DATASET must not be empty or whitespace-only: {i}"
                                )
                            datasets.append(src_root_dataset)
                            datasets.append(dst_root_dataset)
                except OSError as e:
                    parser.error(f"{err_prefix}{e}")

        if len(datasets) % 2 != 0:
            parser.error(f"{err_prefix}Each SRC_DATASET must have a corresponding DST_DATASET: {datasets}")
        root_dataset_pairs: list[tuple[str, str]] = [(datasets[i], datasets[i + 1]) for i in range(0, len(datasets), 2)]
        setattr(namespace, self.dest, root_dataset_pairs)


#############################################################################
@final
class SSHConfigFileNameAction(argparse.Action):
    """Validates SSH config file argument contains no whitespace or shell chars."""

    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        """Reject invalid file names with spaces or shell metacharacters."""
        values = values.strip()
        if values == "":
            parser.error(f"{option_string}: Empty string is not valid")
        if any(char in SHELL_CHARS or char.isspace() for char in values):
            parser.error(f"{option_string}: Invalid file name '{values}': must not contain whitespace or special chars.")
        setattr(namespace, self.dest, values)


#############################################################################
@final
class SafeFileNameAction(argparse.Action):
    """Ensures filenames lack path separators and weird whitespace."""

    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        """Rejects filenames containing path traversal or unusual whitespace."""
        if ".." in values or "/" in values or "\\" in values:
            parser.error(f"{option_string}: Invalid file name '{values}': must not contain '..' or '/' or '\\'.")
        if any(char.isspace() and char != " " for char in values):
            parser.error(f"{option_string}: Invalid file name '{values}': must not contain whitespace other than space.")
        setattr(namespace, self.dest, values)


#############################################################################
@final
class SafeDirectoryNameAction(argparse.Action):
    """Validates directory name argument, allowing only simple spaces."""

    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        """Rejects directory names with weird whitespace or emptiness."""
        values = values.strip()
        if values == "":
            parser.error(f"{option_string}: Empty string is not valid")
        if any(char.isspace() and char != " " for char in values):
            parser.error(f"{option_string}: Invalid dir name '{values}': must not contain whitespace other than space.")
        setattr(namespace, self.dest, values)


#############################################################################
@final
class NewSnapshotFilterGroupAction(argparse.Action):
    """Starts a new filter group when seen in command line arguments."""

    def __call__(
        self, parser: argparse.ArgumentParser, args: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        """Insert an empty group before adding new snapshot filters."""
        if not hasattr(args, SNAPSHOT_FILTERS_VAR):
            args.snapshot_filters_var = [[]]
        elif len(args.snapshot_filters_var[-1]) > 0:
            args.snapshot_filters_var.append([])


#############################################################################
@final
class FileOrLiteralAction(argparse.Action):
    """Allows '@file' style argument expansion with '+' prefix."""

    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        """Expands file arguments and appends them to the namespace."""

        current_values: list[str] | None = getattr(namespace, self.dest, None)
        if current_values is None:
            current_values = []
        extra_values: list[str] = []
        err_prefix: str = f"{option_string or self.dest}: "
        for value in values:
            if not value.startswith("+"):
                extra_values.append(value)
            else:
                path: str = value[1:]
                validate_no_argument_file(path, namespace, err_prefix=err_prefix, parser=parser)
                if "bzfs_argument_file" not in os.path.basename(path):
                    parser.error(f"{err_prefix}basename must contain substring 'bzfs_argument_file': {path}")
                try:
                    with open_nofollow(path, "r", encoding="utf-8") as fd:
                        for line in fd.read().splitlines():
                            if line.startswith("#") or not line.strip():
                                continue
                            extra_values.append(line)
                except OSError as e:
                    parser.error(f"{err_prefix}{e}")
        current_values += extra_values
        setattr(namespace, self.dest, current_values)
        if self.dest in SNAPSHOT_REGEX_FILTER_NAMES:
            _add_snapshot_filter(namespace, SnapshotFilter(self.dest, None, extra_values))


#############################################################################
class IncludeSnapshotPlanAction(argparse.Action):
    """Parses include plan dictionaries from the command line."""

    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        """Builds a list of snapshot filters from a serialized plan."""
        opts: list[str] | None = getattr(namespace, self.dest, None)
        opts = [] if opts is None else opts
        if not self._add_opts(opts, parser, values, option_string=option_string):
            opts += ["--new-snapshot-filter-group", "--include-snapshot-regex=!.*"]
        setattr(namespace, self.dest, opts)

    def _add_opts(
        self,
        opts: list[str],
        parser: argparse.ArgumentParser,
        values: str,
        option_string: str | None = None,
    ) -> bool:
        xperiods: SnapshotPeriods = SnapshotPeriods()
        has_at_least_one_filter_clause: bool = False
        for org, target_periods in ast.literal_eval(values).items():
            prefix: str = re.escape(nprefix(org))
            for target, periods in target_periods.items():
                infix: str = re.escape(ninfix(target)) if target else YEAR_WITH_FOUR_DIGITS_REGEX.pattern
                for period_unit, period_amount in periods.items():
                    if not isinstance(period_amount, int) or period_amount < 0:
                        parser.error(f"{option_string}: Period amount must be a non-negative integer: {period_amount}")
                    suffix: str = re.escape(nsuffix(period_unit))
                    regex: str = f"{prefix}{infix}.*{suffix}"
                    opts += ["--new-snapshot-filter-group", f"--include-snapshot-regex={regex}"]
                    duration_amount, duration_unit = xperiods.suffix_to_duration0(period_unit)
                    duration_unit_label: str | None = xperiods.period_labels.get(duration_unit)
                    opts += [
                        "--include-snapshot-times-and-ranks",
                        (
                            "notime"
                            if duration_unit_label is None or duration_amount * period_amount == 0
                            else f"{duration_amount * period_amount}{duration_unit_label}ago..anytime"
                        ),
                        f"latest{period_amount}",
                    ]
                    has_at_least_one_filter_clause = True
        return has_at_least_one_filter_clause


#############################################################################
@final
class DeleteDstSnapshotsExceptPlanAction(IncludeSnapshotPlanAction):
    """Specialized include plan used to decide which dst snapshots to keep."""

    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        """Parses plan while preventing disasters."""
        opts: list[str] | None = getattr(namespace, self.dest, None)
        opts = [] if opts is None else opts
        opts += ["--delete-dst-snapshots-except"]
        if not self._add_opts(opts, parser, values, option_string=option_string):
            parser.error(
                f"{option_string}: Cowardly refusing to delete all snapshots on"
                f"--delete-dst-snapshots-except-plan='{values}' (which means 'retain no snapshots' aka "
                "'delete all snapshots'). Assuming this is an unintended pilot error rather than intended carnage. "
                "Aborting. If this is really what is intended, use `--delete-dst-snapshots --include-snapshot-regex=.*` "
                "instead to force the deletion."
            )
        setattr(namespace, self.dest, opts)


#############################################################################
@final
class TimeRangeAndRankRangeAction(argparse.Action):
    """Parses --include-snapshot-times-and-ranks option values."""

    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        """Converts user-supplied time and rank ranges into snapshot filters."""

        def parse_time(time_spec: str) -> int | timedelta | None:
            time_spec = time_spec.strip()
            if time_spec == "*" or time_spec == "anytime":
                return None
            if time_spec.isdigit():
                return int(time_spec)
            try:
                return timedelta(milliseconds=parse_duration_to_milliseconds(time_spec, regex_suffix=r"\s*ago"))
            except ValueError:
                try:
                    return unixtime_fromisoformat(time_spec)
                except ValueError:
                    parser.error(f"{option_string}: Invalid duration, Unix time, or ISO 8601 datetime: {time_spec}")

        assert isinstance(values, list)
        assert len(values) > 0
        value: str = values[0].strip()
        if value == "notime":
            value = "0..0"
        if ".." not in value:
            parser.error(f"{option_string}: Invalid time range: Missing '..' separator: {value}")
        timerange_specs: list[int | timedelta | None] = [parse_time(time_spec) for time_spec in value.split("..", 1)]
        rankranges: list[RankRange] = self._parse_rankranges(parser, values[1:], option_string=option_string)
        setattr(namespace, self.dest, [timerange_specs] + rankranges)
        timerange: UnixTimeRange = self._get_include_snapshot_times(timerange_specs)
        _add_time_and_rank_snapshot_filter(namespace, self.dest, timerange, rankranges)

    @staticmethod
    def _get_include_snapshot_times(times: list[timedelta | int | None]) -> UnixTimeRange:
        """Convert start and end times to ``UnixTimeRange`` for filtering."""

        def utc_unix_time_in_seconds(time_spec: timedelta | int | None, default: int) -> timedelta | int:
            if isinstance(time_spec, timedelta):
                return time_spec
            if isinstance(time_spec, int):
                return int(time_spec)
            return default

        lo, hi = times
        if lo is None and hi is None:
            return None
        lo = utc_unix_time_in_seconds(lo, default=0)
        hi = utc_unix_time_in_seconds(hi, default=UNIX_TIME_INFINITY_SECS)
        if isinstance(lo, int) and isinstance(hi, int):
            return (lo, hi) if lo <= hi else (hi, lo)
        return lo, hi

    @staticmethod
    def _parse_rankranges(parser: argparse.ArgumentParser, values: Any, option_string: str | None = None) -> list[RankRange]:
        """Parses rank range strings like 'latest 3..latest 5' into tuples."""

        def parse_rank(spec: str) -> tuple[bool, str, int, bool]:
            spec = spec.strip()
            if not (match := re.fullmatch(r"(all\s*except\s*)?(oldest|latest)\s*(\d+)%?", spec)):
                parser.error(f"{option_string}: Invalid rank format: {spec}")
            assert match
            is_except: bool = bool(match.group(1))
            kind: str = match.group(2)
            num: int = int(match.group(3))
            is_percent: bool = spec.endswith("%")
            if is_percent and num > 100:
                parser.error(f"{option_string}: Invalid rank: Percent must not be greater than 100: {spec}")
            return is_except, kind, num, is_percent

        rankranges: list[RankRange] = []
        for value in values:
            value = value.strip()
            if ".." in value:
                lo_split, hi_split = value.split("..", 1)
                lo = parse_rank(lo_split)
                hi = parse_rank(hi_split)
                if lo[0] or hi[0]:
                    parser.error(f"{option_string}: Invalid rank range: {value}")
                if lo[1] != hi[1]:
                    parser.error(f"{option_string}: Ambiguous rank range: Must not compare oldest with latest: {value}")
            else:
                hi = parse_rank(value)
                is_except, kind, num, is_percent = hi
                if is_except:
                    if is_percent:
                        negated_kind: str = "oldest" if kind == "latest" else "latest"
                        lo = parse_rank(f"{negated_kind}0")
                        hi = parse_rank(f"{negated_kind}{100-num}%")
                    else:
                        lo = parse_rank(f"{kind}{num}")
                        hi = parse_rank(f"{kind}100%")
                else:
                    lo = parse_rank(f"{kind}0")
            rankranges.append((lo[1:], hi[1:]))
        return rankranges


#############################################################################
@final
class CheckPercentRange(CheckRange):
    """Argparse action verifying percentages fall within 0-100."""

    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        """Normalizes integer or percent values and store them."""
        assert isinstance(values, str)
        original = values
        values = values.strip()
        is_percent: bool = values.endswith("%")
        if is_percent:
            values = values[0:-1]
        try:
            values = float(values)
        except ValueError:
            parser.error(f"{option_string}: Invalid percentage or number: {original}")
        super().__call__(parser, namespace, values, option_string=option_string)
        setattr(namespace, self.dest, (getattr(namespace, self.dest), is_percent))
