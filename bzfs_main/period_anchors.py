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
"""Utility that snaps datetimes to calendar periods.

Anchors specify offsets within yearly, monthly and smaller cycles. These values are used by
``round_datetime_up_to_duration_multiple`` to snap datetimes to the next boundary. Keeping anchors in a dataclass simplifies
argument handling and makes the rounding logic reusable.
"""

from __future__ import (
    annotations,
)
import argparse
import calendar
import dataclasses
from collections.abc import (
    Collection,
)
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    datetime,
    timedelta,
)
from typing import (
    Final,
    final,
)

# constants:
METADATA_YEAR: Final = {"min": 1, "max": 9999, "help": None}
METADATA_MONTH: Final = {"min": 1, "max": 12, "help": "The month within a year"}
METADATA_WEEKDAY: Final = {"min": 0, "max": 6, "help": "The weekday within a week: 0=Sunday, 1=Monday, ..., 6=Saturday"}
METADATA_DAY: Final = {"min": 1, "max": 31, "help": "The day within a month"}
METADATA_HOUR: Final = {"min": 0, "max": 23, "help": "The hour within a day"}
METADATA_MINUTE: Final = {"min": 0, "max": 59, "help": "The minute within an hour"}
METADATA_SECOND: Final = {"min": 0, "max": 59, "help": "The second within a minute"}
METADATA_MILLISECOND: Final = {"min": 0, "max": 999, "help": "The millisecond within a second"}
METADATA_MICROSECOND: Final = {"min": 0, "max": 999, "help": "The microsecond within a millisecond"}


@dataclass(frozen=True)
@final
class PeriodAnchors:
    """Anchor offsets used to round datetimes up to periodic boundaries; Immutable."""

    # The anchors for a given duration unit are computed as follows:
    # yearly: Count from anchor.yearly_* vars
    yearly_year: int = field(default=2025, metadata=METADATA_YEAR)  # 1 <= x <= 9999
    yearly_month: int = field(default=1, metadata=METADATA_MONTH)  # 1 <= x <= 12
    yearly_monthday: int = field(default=1, metadata=METADATA_DAY)  # 1 <= x <= 31
    yearly_hour: int = field(default=0, metadata=METADATA_HOUR)  # 0 <= x <= 23
    yearly_minute: int = field(default=0, metadata=METADATA_MINUTE)  # 0 <= x <= 59
    yearly_second: int = field(default=0, metadata=METADATA_SECOND)  # 0 <= x <= 59

    # monthly: Count months from anchor.monthly_* vars
    monthly_year: int = field(default=2025, metadata=METADATA_YEAR)  # 1 <= x <= 9999
    monthly_month: int = field(default=1, metadata={"min": 1, "max": 12, "help": "The anchor month of multi-month periods"})
    monthly_monthday: int = field(default=1, metadata=METADATA_DAY)  # 1 <= x <= 31
    monthly_hour: int = field(default=0, metadata=METADATA_HOUR)  # 0 <= x <= 23
    monthly_minute: int = field(default=0, metadata=METADATA_MINUTE)  # 0 <= x <= 59
    monthly_second: int = field(default=0, metadata=METADATA_SECOND)  # 0 <= x <= 59

    # weekly: Count weeks from the first weekly_weekday on or after January 1 of weekly_year
    weekly_year: int = field(default=2025, metadata=METADATA_YEAR)  # 1 <= x <= 9999
    weekly_weekday: int = field(default=0, metadata=METADATA_WEEKDAY)  # 0 <= x <= 6 (0=Sunday, ..., 6=Saturday)
    weekly_hour: int = field(default=0, metadata=METADATA_HOUR)  # 0 <= x <= 23
    weekly_minute: int = field(default=0, metadata=METADATA_MINUTE)  # 0 <= x <= 59
    weekly_second: int = field(default=0, metadata=METADATA_SECOND)  # 0 <= x <= 59

    # daily: Count days from January 1 of daily_year
    daily_year: int = field(default=2025, metadata=METADATA_YEAR)  # 1 <= x <= 9999
    daily_hour: int = field(default=0, metadata=METADATA_HOUR)  # 0 <= x <= 23
    daily_minute: int = field(default=0, metadata=METADATA_MINUTE)  # 0 <= x <= 59
    daily_second: int = field(default=0, metadata=METADATA_SECOND)  # 0 <= x <= 59

    # hourly: Anchor(dt) = midnight of dt + anchor.hourly_* vars
    hourly_minute: int = field(default=0, metadata=METADATA_MINUTE)  # 0 <= x <= 59
    hourly_second: int = field(default=0, metadata=METADATA_SECOND)  # 0 <= x <= 59

    # minutely: Anchor(dt) = midnight of dt + anchor.minutely_* vars
    minutely_second: int = field(default=0, metadata=METADATA_SECOND)  # 0 <= x <= 59

    # secondly: Anchor(dt) = midnight of dt + anchor.secondly_* vars
    secondly_millisecond: int = field(default=0, metadata=METADATA_MILLISECOND)  # 0 <= x <= 999

    # millisecondly: Anchor(dt) = midnight of dt + anchor.millisecondly_* vars
    millisecondly_microsecond: int = field(default=0, metadata=METADATA_MICROSECOND)  # 0 <= x <= 999

    @classmethod
    def parse(cls, args: argparse.Namespace, exclude: Collection[str] = frozenset()) -> PeriodAnchors:
        """Creates a ``PeriodAnchors`` instance from parsed CLI arguments."""
        kwargs: dict[str, int] = {f.name: getattr(args, f.name) for f in dataclasses.fields(cls) if f.name not in exclude}
        return cls(**kwargs)

    def round_datetime_up_to_duration_multiple(self, dt: datetime, duration_amount: int, duration_unit: str) -> datetime:
        """Given a timezone-aware datetime and a duration, returns a datetime (in the same timezone) that is greater than or
        equal to dt, and rounded up (ceiled) and snapped to an anchor plus a multiple of the duration.

        The snapping is done relative to the anchors object and the rules defined therein.
        Supported units: "millisecondly", "secondly", "minutely", "hourly", "daily", "weekly", "monthly", "yearly".
        If dt is already exactly on a boundary (i.e. exactly on a multiple), it is returned unchanged.
        Examples:
        Default hourly anchor is midnight
        14:00:00, 1 hours --> 14:00:00
        14:05:01, 1 hours --> 15:00:00
        15:05:01, 1 hours --> 16:00:00
        16:05:01, 1 hours --> 17:00:00
        23:55:01, 1 hours --> 00:00:00 on the next day
        14:05:01, 2 hours --> 16:00:00
        15:00:00, 2 hours --> 16:00:00
        15:05:01, 2 hours --> 16:00:00
        16:00:00, 2 hours --> 16:00:00
        16:05:01, 2 hours --> 18:00:00
        23:55:01, 2 hours --> 00:00:00 on the next day
        """

        if duration_amount == 0:
            return dt

        assert duration_amount > 0
        period: timedelta | None = None
        anchor: datetime
        if duration_unit == "millisecondly":
            anchor = dt.replace(hour=0, minute=0, second=0, microsecond=self.millisecondly_microsecond)
            period = timedelta(milliseconds=duration_amount)

        elif duration_unit == "secondly":
            anchor = dt.replace(hour=0, minute=0, second=0, microsecond=self.secondly_millisecond * 1000)
            period = timedelta(seconds=duration_amount)

        elif duration_unit == "minutely":
            anchor = dt.replace(hour=0, minute=0, second=self.minutely_second, microsecond=0)
            period = timedelta(minutes=duration_amount)

        elif duration_unit == "hourly":
            anchor = dt.replace(hour=0, minute=self.hourly_minute, second=self.hourly_second, microsecond=0)
            period = timedelta(hours=duration_amount)

        elif duration_unit == "daily":
            anchor = dt.replace(
                year=self.daily_year,
                month=1,
                day=1,
                hour=self.daily_hour,
                minute=self.daily_minute,
                second=self.daily_second,
                microsecond=0,
            )
            period = timedelta(days=duration_amount)

        elif duration_unit == "weekly":
            anchor = dt.replace(
                year=self.weekly_year,
                month=1,
                day=1,
                hour=self.weekly_hour,
                minute=self.weekly_minute,
                second=self.weekly_second,
                microsecond=0,
            )
            # Convert cron weekday (0=Sunday, 1=Monday, ..., 6=Saturday) to Python's weekday (0=Monday, ..., 6=Sunday)
            target_py_weekday: int = (self.weekly_weekday - 1) % 7
            diff_days: int = (target_py_weekday - anchor.weekday()) % 7
            anchor = anchor + timedelta(days=diff_days)
            period = timedelta(weeks=duration_amount)

        if period is not None:  # "millisecondly", "secondly", "minutely", "hourly", "daily", "weekly"
            delta: timedelta = dt - anchor
            period_micros: int = (period.days * 86400 + period.seconds) * 1_000_000 + period.microseconds
            delta_micros: int = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
            remainder: int = delta_micros % period_micros
            if remainder == 0:
                return dt
            return dt + timedelta(microseconds=period_micros - remainder)

        elif duration_unit == "monthly":
            months_since_anchor: int = (dt.year - self.monthly_year) * 12 + dt.month - self.monthly_month
            offset_months: int = -months_since_anchor % duration_amount
            anchor = self._add_months(dt, offset_months)
            if anchor < dt:
                anchor = self._add_months(dt, offset_months + duration_amount)
            return anchor

        elif duration_unit == "yearly":
            offset_years: int = (self.yearly_year - dt.year) % duration_amount
            anchor = self._add_years(dt, offset_years)
            if anchor < dt:
                anchor = self._add_years(dt, offset_years + duration_amount)
            return anchor

        else:
            raise ValueError(f"Unsupported duration unit: {duration_unit}")

    def _add_months(self, dt: datetime, months: int) -> datetime:
        """Build a boundary offset from dt's month, clamping the configured day only against the target month's end."""
        total_month: int = dt.month - 1 + months
        new_year: int = dt.year + total_month // 12
        new_month: int = total_month % 12 + 1
        last_day: int = calendar.monthrange(new_year, new_month)[1]  # last valid day of the target month
        return dt.replace(
            year=new_year,
            month=new_month,
            day=min(self.monthly_monthday, last_day),
            hour=self.monthly_hour,
            minute=self.monthly_minute,
            second=self.monthly_second,
            microsecond=0,
        )

    def _add_years(self, dt: datetime, years: int) -> datetime:
        """Build a boundary offset from dt's year using yearly anchors, clamping the configured day to the target month's end."""
        new_year: int = dt.year + years
        last_day: int = calendar.monthrange(new_year, self.yearly_month)[1]  # last valid day of the target month
        return dt.replace(
            year=new_year,
            month=self.yearly_month,
            day=min(self.yearly_monthday, last_day),
            hour=self.yearly_hour,
            minute=self.yearly_minute,
            second=self.yearly_second,
            microsecond=0,
        )
