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

from __future__ import annotations
import argparse
import calendar
import dataclasses
from dataclasses import dataclass, field
from datetime import datetime, timedelta

# constants:
METADATA_MONTH = {"min": 1, "max": 12, "help": "The month within a year"}
METADATA_WEEKDAY = {"min": 0, "max": 6, "help": "The weekday within a week: 0=Sunday, 1=Monday, ..., 6=Saturday"}
METADATA_DAY = {"min": 1, "max": 31, "help": "The day within a month"}
METADATA_HOUR = {"min": 0, "max": 23, "help": "The hour within a day"}
METADATA_MINUTE = {"min": 0, "max": 59, "help": "The minute within an hour"}
METADATA_SECOND = {"min": 0, "max": 59, "help": "The second within a minute"}
METADATA_MILLISECOND = {"min": 0, "max": 999, "help": "The millisecond within a second"}
METADATA_MICROSECOND = {"min": 0, "max": 999, "help": "The microsecond within a millisecond"}


@dataclass(frozen=True)
class PeriodAnchors:
    """Anchor offsets used to round datetimes up to periodic boundaries."""

    # The anchors for a given duration unit are computed as follows:
    # yearly: Anchor(dt) = latest T where T <= dt and T == Start of January 1 of dt + anchor.yearly_* vars
    yearly_year: int = field(default=2025, metadata={"min": 1, "max": 9999, "help": "The anchor year of multi-year periods"})
    yearly_month: int = field(default=1, metadata=METADATA_MONTH)  # 1 <= x <= 12
    yearly_monthday: int = field(default=1, metadata=METADATA_DAY)  # 1 <= x <= 31
    yearly_hour: int = field(default=0, metadata=METADATA_HOUR)  # 0 <= x <= 23
    yearly_minute: int = field(default=0, metadata=METADATA_MINUTE)  # 0 <= x <= 59
    yearly_second: int = field(default=0, metadata=METADATA_SECOND)  # 0 <= x <= 59

    # monthly: Anchor(dt) = latest T where T <= dt && T == Start of first day of month of dt + anchor.monthly_* vars
    monthly_month: int = field(default=1, metadata={"min": 1, "max": 12, "help": "The anchor month of multi-month periods"})
    monthly_monthday: int = field(default=1, metadata=METADATA_DAY)  # 1 <= x <= 31
    monthly_hour: int = field(default=0, metadata=METADATA_HOUR)  # 0 <= x <= 23
    monthly_minute: int = field(default=0, metadata=METADATA_MINUTE)  # 0 <= x <= 59
    monthly_second: int = field(default=0, metadata=METADATA_SECOND)  # 0 <= x <= 59

    # weekly: Anchor(dt) = latest T where T <= dt && T == Latest midnight from Sunday to Monday of dt + anchor.weekly_* vars
    weekly_weekday: int = field(default=0, metadata=METADATA_WEEKDAY)  # 0 <= x <= 7
    weekly_hour: int = field(default=0, metadata=METADATA_HOUR)  # 0 <= x <= 23
    weekly_minute: int = field(default=0, metadata=METADATA_MINUTE)  # 0 <= x <= 59
    weekly_second: int = field(default=0, metadata=METADATA_SECOND)  # 0 <= x <= 59

    # daily: Anchor(dt) = latest T where T <= dt && T == Latest midnight of dt + anchor.daily_* vars
    daily_hour: int = field(default=0, metadata=METADATA_HOUR)  # 0 <= x <= 23
    daily_minute: int = field(default=0, metadata=METADATA_MINUTE)  # 0 <= x <= 59
    daily_second: int = field(default=0, metadata=METADATA_SECOND)  # 0 <= x <= 59

    # hourly: Anchor(dt) = latest T where T <= dt && T == Latest midnight of dt + anchor.hourly_* vars
    hourly_minute: int = field(default=0, metadata=METADATA_MINUTE)  # 0 <= x <= 59
    hourly_second: int = field(default=0, metadata=METADATA_SECOND)  # 0 <= x <= 59

    # minutely: Anchor(dt) = latest T where T <= dt && T == Latest midnight of dt + anchor.minutely_* vars
    minutely_second: int = field(default=0, metadata=METADATA_SECOND)  # 0 <= x <= 59

    # secondly: Anchor(dt) = latest T where T <= dt && T == Latest midnight of dt + anchor.secondly_* vars
    secondly_millisecond: int = field(default=0, metadata=METADATA_MILLISECOND)  # 0 <= x <= 999

    # secondly: Anchor(dt) = latest T where T <= dt && T == Latest midnight of dt + anchor.millisecondly_* vars
    millisecondly_microsecond: int = field(default=0, metadata=METADATA_MICROSECOND)  # 0 <= x <= 999

    @staticmethod
    def parse(args: argparse.Namespace) -> "PeriodAnchors":
        """Creates a ``PeriodAnchors`` instance from parsed CLI arguments."""
        kwargs: dict[str, int] = {f.name: getattr(args, f.name) for f in dataclasses.fields(PeriodAnchors)}
        return PeriodAnchors(**kwargs)


def round_datetime_up_to_duration_multiple(
    dt: datetime, duration_amount: int, duration_unit: str, anchors: PeriodAnchors
) -> datetime:
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

    def add_months(dt: datetime, months: int) -> datetime:
        """Returns ``dt`` plus ``months`` with day clamped to month's end."""
        total_month: int = dt.month - 1 + months
        new_year: int = dt.year + total_month // 12
        new_month: int = total_month % 12 + 1
        last_day: int = calendar.monthrange(new_year, new_month)[1]  # last valid day of the current month
        return dt.replace(year=new_year, month=new_month, day=min(dt.day, last_day))

    def add_years(dt: datetime, years: int) -> datetime:
        """Returns ``dt`` plus ``years`` with day clamped to month's end."""
        new_year: int = dt.year + years
        last_day: int = calendar.monthrange(new_year, dt.month)[1]  # last valid day of the current month
        return dt.replace(year=new_year, day=min(dt.day, last_day))

    if duration_amount == 0:
        return dt

    period: timedelta | None = None
    anchor: datetime
    daily_base: datetime
    last_day: int
    if duration_unit == "millisecondly":
        anchor = dt.replace(hour=0, minute=0, second=0, microsecond=anchors.millisecondly_microsecond)
        anchor = anchor if anchor <= dt else anchor - timedelta(milliseconds=1)
        period = timedelta(milliseconds=duration_amount)

    elif duration_unit == "secondly":
        anchor = dt.replace(hour=0, minute=0, second=0, microsecond=anchors.secondly_millisecond * 1000)
        anchor = anchor if anchor <= dt else anchor - timedelta(seconds=1)
        period = timedelta(seconds=duration_amount)

    elif duration_unit == "minutely":
        anchor = dt.replace(second=anchors.minutely_second, microsecond=0)
        anchor = anchor if anchor <= dt else anchor - timedelta(minutes=1)
        period = timedelta(minutes=duration_amount)

    elif duration_unit == "hourly":
        daily_base = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        anchor = daily_base + timedelta(minutes=anchors.hourly_minute, seconds=anchors.hourly_second)
        anchor = anchor if anchor <= dt else anchor - timedelta(days=1)
        period = timedelta(hours=duration_amount)

    elif duration_unit == "daily":
        daily_base = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        anchor = daily_base + timedelta(hours=anchors.daily_hour, minutes=anchors.daily_minute, seconds=anchors.daily_second)
        anchor = anchor if anchor <= dt else anchor - timedelta(days=1)
        period = timedelta(days=duration_amount)

    elif duration_unit == "weekly":
        daily_base = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        anchor = daily_base + timedelta(
            hours=anchors.weekly_hour, minutes=anchors.weekly_minute, seconds=anchors.weekly_second
        )
        # Convert cron weekday (0=Sunday, 1=Monday, ..., 6=Saturday) to Python's weekday (0=Monday, ..., 6=Sunday)
        target_py_weekday: int = (anchors.weekly_weekday - 1) % 7
        diff_days: int = (anchor.weekday() - target_py_weekday) % 7
        anchor = anchor - timedelta(days=diff_days)
        anchor = anchor if anchor <= dt else anchor - timedelta(weeks=1)
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
        last_day = calendar.monthrange(dt.year, dt.month)[1]  # last valid day of the current month
        anchor = dt.replace(  # Compute the base anchor for the month ensuring the day is valid
            month=anchors.monthly_month,
            day=min(anchors.monthly_monthday, last_day),
            hour=anchors.monthly_hour,
            minute=anchors.monthly_minute,
            second=anchors.monthly_second,
            microsecond=0,
        )
        if anchor > dt:
            anchor = add_months(anchor, -duration_amount)
        diff_months: int = (dt.year - anchor.year) * 12 + (dt.month - anchor.month)
        anchor_boundary: datetime = add_months(anchor, duration_amount * (diff_months // duration_amount))
        if anchor_boundary < dt:
            anchor_boundary = add_months(anchor_boundary, duration_amount)
        return anchor_boundary

    elif duration_unit == "yearly":
        # Calculate the start of the cycle period that `dt` falls into.
        year_offset: int = (dt.year - anchors.yearly_year) % duration_amount
        period_start_year: int = dt.year - year_offset
        last_day = calendar.monthrange(period_start_year, anchors.yearly_month)[1]  # last valid day of the month
        anchor = dt.replace(
            year=period_start_year,
            month=anchors.yearly_month,
            day=min(anchors.yearly_monthday, last_day),
            hour=anchors.yearly_hour,
            minute=anchors.yearly_minute,
            second=anchors.yearly_second,
            microsecond=0,
        )
        if anchor < dt:
            return add_years(anchor, duration_amount)
        return anchor

    else:
        raise ValueError(f"Unsupported duration unit: {duration_unit}")
