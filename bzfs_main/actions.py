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

from __future__ import annotations
import argparse
import ast
import operator
import os
import re
from datetime import timedelta
from typing import Any, ClassVar, Optional, Tuple, Union

# Dependencies from bzfs are imported lazily within methods to avoid circular imports.


###############################################################################
class NonEmptyStringAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        values = values.strip()
        if values == "":
            parser.error(f"{option_string}: Empty string is not valid")
        setattr(namespace, self.dest, values)


###############################################################################
class DatasetPairsAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        from .bzfs import open_nofollow, validate_no_argument_file

        datasets = []
        err_prefix = f"{option_string or self.dest}: "
        for value in values:
            if not value.startswith("+"):
                datasets.append(value)
            else:
                path = value[1:]
                validate_no_argument_file(path, namespace, err_prefix=err_prefix, parser=parser)
                if "bzfs_argument_file" not in os.path.basename(path):
                    parser.error(f"{err_prefix}basename must contain substring 'bzfs_argument_file': {path}")
                try:
                    with open_nofollow(path, "r", encoding="utf-8") as fd:
                        for i, line in enumerate(fd.read().splitlines()):
                            if line.startswith("#") or not line.strip():
                                continue  # skip comment lines and empty lines
                            splits = line.split("\t", 1)
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
        root_dataset_pairs = [(datasets[i], datasets[i + 1]) for i in range(0, len(datasets), 2)]
        setattr(namespace, self.dest, root_dataset_pairs)


###############################################################################
class SSHConfigFileNameAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        from .bzfs import SHELL_CHARS

        values = values.strip()
        if values == "":
            parser.error(f"{option_string}: Empty string is not valid")
        if any(char in SHELL_CHARS or char.isspace() for char in values):
            parser.error(f"{option_string}: Invalid file name '{values}': must not contain whitespace or special chars.")
        setattr(namespace, self.dest, values)


###############################################################################
class SafeFileNameAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        if ".." in values or "/" in values or "\\" in values:
            parser.error(f"{option_string}: Invalid file name '{values}': must not contain '..' or '/' or '\\'.")
        if any(char.isspace() and char != " " for char in values):
            parser.error(f"{option_string}: Invalid file name '{values}': must not contain whitespace other than space.")
        setattr(namespace, self.dest, values)


###############################################################################
class SafeDirectoryNameAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        values = values.strip()
        if values == "":
            parser.error(f"{option_string}: Empty string is not valid")
        if any(char.isspace() and char != " " for char in values):
            parser.error(f"{option_string}: Invalid dir name '{values}': must not contain whitespace other than space.")
        setattr(namespace, self.dest, values)


###############################################################################
class NewSnapshotFilterGroupAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, args: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        from .bzfs import snapshot_filters_var

        if not hasattr(args, snapshot_filters_var):
            args.snapshot_filters_var = [[]]
        elif len(args.snapshot_filters_var[-1]) > 0:
            args.snapshot_filters_var.append([])


###############################################################################
class FileOrLiteralAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        from .bzfs import (
            SnapshotFilter,
            add_snapshot_filter,
            open_nofollow,
            snapshot_regex_filter_names,
            validate_no_argument_file,
        )

        current_values = getattr(namespace, self.dest, None)
        if current_values is None:
            current_values = []
        extra_values = []
        err_prefix = f"{option_string or self.dest}: "
        for value in values:
            if not value.startswith("+"):
                extra_values.append(value)
            else:
                path = value[1:]
                validate_no_argument_file(path, namespace, err_prefix=err_prefix, parser=parser)
                if "bzfs_argument_file" not in os.path.basename(path):
                    parser.error(f"{err_prefix}basename must contain substring 'bzfs_argument_file': {path}")
                try:
                    with open_nofollow(path, "r", encoding="utf-8") as fd:
                        for line in fd.read().splitlines():
                            if line.startswith("#") or not line.strip():
                                continue  # skip comment lines and empty lines
                            extra_values.append(line)
                except OSError as e:
                    parser.error(f"{err_prefix}{e}")
        current_values += extra_values
        setattr(namespace, self.dest, current_values)
        if self.dest in snapshot_regex_filter_names:
            add_snapshot_filter(namespace, SnapshotFilter(self.dest, None, extra_values))


###############################################################################
class IncludeSnapshotPlanAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        from .bzfs import getenv_bool

        opts = getattr(namespace, self.dest, None)
        opts = [] if opts is None else opts
        include_snapshot_times_and_ranks = getenv_bool("include_snapshot_plan_excludes_outdated_snapshots", True)
        if not self._add_opts(opts, include_snapshot_times_and_ranks, parser, values, option_string=option_string):
            opts += ["--new-snapshot-filter-group", "--include-snapshot-regex=!.*"]
        setattr(namespace, self.dest, opts)

    def _add_opts(
        self,
        opts: list[str],
        include_snapshot_times_and_ranks: bool,
        parser: argparse.ArgumentParser,
        values: str,
        option_string: str | None = None,
    ) -> bool:
        from .bzfs import SnapshotPeriods, ninfix, nprefix, nsuffix, year_with_four_digits_regex

        xperiods = SnapshotPeriods()
        has_at_least_one_filter_clause = False
        for org, target_periods in ast.literal_eval(values).items():
            prefix = re.escape(nprefix(org))
            for target, periods in target_periods.items():
                infix = re.escape(ninfix(target)) if target else year_with_four_digits_regex.pattern
                for period_unit, period_amount in periods.items():
                    if not isinstance(period_amount, int) or period_amount < 0:
                        parser.error(f"{option_string}: Period amount must be a non-negative integer: {period_amount}")
                    suffix = re.escape(nsuffix(period_unit))
                    regex = f"{prefix}{infix}.*{suffix}"
                    opts += ["--new-snapshot-filter-group", f"--include-snapshot-regex={regex}"]
                    if include_snapshot_times_and_ranks:
                        duration_amount, duration_unit = xperiods.suffix_to_duration0(period_unit)
                        duration_unit_label = xperiods.period_labels.get(duration_unit)
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


###############################################################################
class DeleteDstSnapshotsExceptPlanAction(IncludeSnapshotPlanAction):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        opts = getattr(namespace, self.dest, None)
        opts = [] if opts is None else opts
        opts += ["--delete-dst-snapshots-except"]
        if not self._add_opts(opts, True, parser, values, option_string=option_string):
            parser.error(
                f"{option_string}: Cowardly refusing to delete all snapshots on"
                f"--delete-dst-snapshots-except-plan='{values}' (which means 'retain no snapshots' aka "
                "'delete all snapshots'). Assuming this is an unintended pilot error rather than intended carnage. "
                "Aborting. If this is really what is intended, use `--delete-dst-snapshots --include-snapshot-regex=.*` "
                "instead to force the deletion."
            )
        setattr(namespace, self.dest, opts)


###############################################################################
RankRange = Tuple[Tuple[str, int, bool], Tuple[str, int, bool]]  # Type alias
UnixTimeRange = Optional[Tuple[Union[timedelta, int], Union[timedelta, int]]]  # Type alias


class TimeRangeAndRankRangeAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        from .bzfs import add_time_and_rank_snapshot_filter, parse_duration_to_milliseconds, unixtime_fromisoformat

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
        value = values[0].strip()
        if value == "notime":
            value = "0..0"
        if ".." not in value:
            parser.error(f"{option_string}: Invalid time range: Missing '..' separator: {value}")
        timerange_specs = [parse_time(time_spec) for time_spec in value.split("..", 1)]
        rankranges = self.parse_rankranges(parser, values[1:], option_string=option_string)
        setattr(namespace, self.dest, [timerange_specs] + rankranges)
        timerange = self.get_include_snapshot_times(timerange_specs)
        add_time_and_rank_snapshot_filter(namespace, self.dest, timerange, rankranges)

    @staticmethod
    def get_include_snapshot_times(times: list[timedelta | int | None]) -> UnixTimeRange:
        from .bzfs import unixtime_infinity_secs

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
        hi = utc_unix_time_in_seconds(hi, default=unixtime_infinity_secs)
        if isinstance(lo, int) and isinstance(hi, int):
            return (lo, hi) if lo <= hi else (hi, lo)
        return lo, hi

    @staticmethod
    def parse_rankranges(parser: argparse.ArgumentParser, values: Any, option_string: str | None = None) -> list[RankRange]:
        def parse_rank(spec: str) -> Tuple[bool, str, int, bool]:
            spec = spec.strip()
            if not (match := re.fullmatch(r"(all\s*except\s*)?(oldest|latest)\s*(\d+)%?", spec)):
                parser.error(f"{option_string}: Invalid rank format: {spec}")
            assert match
            is_except = bool(match.group(1))
            kind = match.group(2)
            num = int(match.group(3))
            is_percent = spec.endswith("%")
            if is_percent and num > 100:
                parser.error(f"{option_string}: Invalid rank: Percent must not be greater than 100: {spec}")
            return is_except, kind, num, is_percent

        rankranges = []
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
                        negated_kind = "oldest" if kind == "latest" else "latest"
                        lo = parse_rank(f"{negated_kind}0")
                        hi = parse_rank(f"{negated_kind}{100-num}%")
                    else:
                        lo = parse_rank(f"{kind}{num}")
                        hi = parse_rank(f"{kind}100%")
                else:
                    lo = parse_rank(f"{kind}0")
            rankranges.append((lo[1:], hi[1:]))
        return rankranges


###############################################################################
class LogConfigVariablesAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        from .bzfs import validate_log_config_variable

        current_values = getattr(namespace, self.dest, None)
        if current_values is None:
            current_values = []
        for variable in values:
            error_msg = validate_log_config_variable(variable)
            if error_msg:
                parser.error(error_msg)
            current_values.append(variable)
        setattr(namespace, self.dest, current_values)


###############################################################################
class CheckRange(argparse.Action):
    ops: ClassVar[dict[str, Any]] = {
        "inf": operator.gt,
        "min": operator.ge,
        "sup": operator.lt,
        "max": operator.le,
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "min" in kwargs and "inf" in kwargs:
            raise ValueError("either min or inf, but not both")
        if "max" in kwargs and "sup" in kwargs:
            raise ValueError("either max or sup, but not both")

        for name in self.ops:
            if name in kwargs:
                setattr(self, name, kwargs.pop(name))

        super().__init__(*args, **kwargs)

    def interval(self) -> str:
        if hasattr(self, "min"):
            lo = f"[{self.min}"
        elif hasattr(self, "inf"):
            lo = f"({self.inf}"
        else:
            lo = "(-infinity"

        if hasattr(self, "max"):
            up = f"{self.max}]"
        elif hasattr(self, "sup"):
            up = f"{self.sup})"
        else:
            up = "+infinity)"

        return f"valid range: {lo}, {up}"

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str | None = None,
    ) -> None:
        for name, op in self.ops.items():
            if hasattr(self, name) and not op(values, getattr(self, name)):
                raise argparse.ArgumentError(self, self.interval())
        setattr(namespace, self.dest, values)


###############################################################################
class CheckPercentRange(CheckRange):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        assert isinstance(values, str)
        original = values
        values = values.strip()
        is_percent = values.endswith("%")
        if is_percent:
            values = values[0:-1]
        try:
            values = float(values)
        except ValueError:
            parser.error(f"{option_string}: Invalid percentage or number: {original}")
        super().__call__(parser, namespace, values, option_string=option_string)
        setattr(namespace, self.dest, (getattr(namespace, self.dest), is_percent))
