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
"""Unit tests helpers that align times to fixed period anchors."""

from __future__ import (
    annotations,
)
import unittest
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from zoneinfo import (
    ZoneInfo,
)

from bzfs_main.period_anchors import (
    PeriodAnchors,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestRoundDatetimeUpToDurationMultiple,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
def round_datetime_up_to_duration_multiple(
    dt: datetime, duration_amount: int, duration_unit: str, anchors: PeriodAnchors | None = None
) -> datetime:
    anchors = PeriodAnchors() if anchors is None else anchors
    return anchors.round_datetime_up_to_duration_multiple(dt, duration_amount, duration_unit)


class TestRoundDatetimeUpToDurationMultiple(unittest.TestCase):

    def setUp(self) -> None:
        # Use a fixed timezone (e.g. Eastern Standard Time, UTC-5) for all tests.
        self.tz = timezone(timedelta(hours=-5))

    def test_examples(self) -> None:
        def make_dt(hour: int, minute: int, second: int) -> datetime:
            return datetime(2024, 11, 29, hour, minute, second, 0, tzinfo=self.tz)

        def round_up(dt: datetime, duration_amount: int) -> datetime:
            return round_datetime_up_to_duration_multiple(dt, duration_amount, "hourly")

        """
        Using default anchors (e.g. hourly snapshots at minute=0, second=0)
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

        dt = make_dt(hour=14, minute=0, second=0)
        self.assertEqual(dt, round_up(dt, duration_amount=1))

        dt = make_dt(hour=14, minute=5, second=1)
        self.assertEqual(dt.replace(hour=15, minute=0, second=0), round_up(dt, duration_amount=1))

        dt = make_dt(hour=15, minute=5, second=1)
        self.assertEqual(dt.replace(hour=16, minute=0, second=0), round_up(dt, duration_amount=1))

        dt = make_dt(hour=16, minute=5, second=1)
        self.assertEqual(dt.replace(hour=17, minute=0, second=0), round_up(dt, duration_amount=1))

        dt = make_dt(hour=23, minute=55, second=1)
        self.assertEqual(dt.replace(day=dt.day + 1, hour=0, minute=0, second=0), round_up(dt, duration_amount=1))

        dt = make_dt(hour=14, minute=5, second=1)
        self.assertEqual(dt.replace(hour=16, minute=0, second=0), round_up(dt, duration_amount=2))

        dt = make_dt(hour=15, minute=0, second=0)
        self.assertEqual(dt.replace(hour=16, minute=0, second=0), round_up(dt, duration_amount=2))

        dt = make_dt(hour=15, minute=5, second=1)
        self.assertEqual(dt.replace(hour=16, minute=0, second=0), round_up(dt, duration_amount=2))

        dt = make_dt(hour=16, minute=0, second=0)
        self.assertEqual(dt, round_up(dt, duration_amount=2))

        dt = make_dt(hour=16, minute=5, second=1)
        self.assertEqual(dt.replace(hour=18, minute=0, second=0), round_up(dt, duration_amount=2))

        dt = make_dt(hour=23, minute=55, second=1)
        self.assertEqual(dt.replace(day=dt.day + 1, hour=0, minute=0, second=0), round_up(dt, duration_amount=2))

    def test_zero_unixtime(self) -> None:
        dt = datetime.fromtimestamp(0, tz=timezone.utc)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        expected = dt
        self.assertEqual(expected, result)
        self.assertEqual(timezone.utc, result.tzinfo)

        tz = timezone(timedelta(hours=-7))
        dt = datetime.fromtimestamp(0, tz=tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        expected = dt
        self.assertEqual(expected, result)
        self.assertEqual(tz, result.tzinfo)

    def test_zero_unixtime_plus_one_microsecond(self) -> None:
        dt = datetime.fromtimestamp(1 / 1_000_000, tz=timezone.utc)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        expected = dt.replace(microsecond=0) + timedelta(hours=1)
        self.assertEqual(expected, result)
        self.assertEqual(timezone.utc, result.tzinfo)

        tz = timezone(timedelta(hours=-7))
        dt = datetime.fromtimestamp(1 / 1_000_000, tz=tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        expected = dt.replace(microsecond=0) + timedelta(hours=1)
        self.assertEqual(expected, result)
        self.assertEqual(tz, result.tzinfo)

    def test_zero_duration_amount(self) -> None:
        dt = datetime(2025, 2, 11, 14, 5, 1, 123456, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 0, "hourly")
        expected = dt
        self.assertEqual(expected, result)
        self.assertEqual(self.tz, result.tzinfo)

    def test_milliseconds_non_boundary(self) -> None:
        """Rounding up to the next millisecond when dt is not on a millisecond boundary."""
        dt = datetime(2025, 2, 11, 14, 5, 1, 123456, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "millisecondly")
        expected = dt.replace(microsecond=0) + timedelta(milliseconds=123 + 1)
        self.assertEqual(expected, result)
        self.assertEqual(self.tz, result.tzinfo)

    def test_milliseconds_boundary(self) -> None:
        """Rounding up to the next second when dt is exactly on a second boundary returns dt."""
        dt = datetime(2025, 2, 11, 14, 5, 1, 123000, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "millisecondly")
        self.assertEqual(dt, result)

    def test_seconds_non_boundary(self) -> None:
        """Rounding up to the next second when dt is not on a second boundary."""
        dt = datetime(2025, 2, 11, 14, 5, 1, 123456, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "secondly")
        expected = dt.replace(microsecond=0) + timedelta(seconds=1)
        self.assertEqual(expected, result)
        self.assertEqual(self.tz, result.tzinfo)

    def test_seconds_boundary(self) -> None:
        """Rounding up to the next second when dt is exactly on a second boundary returns dt."""
        dt = datetime(2025, 2, 11, 14, 5, 1, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "secondly")
        self.assertEqual(dt, result)

    def test_minutes_non_boundary(self) -> None:
        """Rounding up to the next minute when dt is not on a minute boundary."""
        dt = datetime(2025, 2, 11, 14, 5, 1, 500000, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "minutely")
        expected = dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
        self.assertEqual(expected, result)

    def test_minutes_boundary(self) -> None:
        """Rounding up to the next minute when dt is exactly on a minute boundary returns dt."""
        dt = datetime(2025, 2, 11, 14, 5, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "minutely")
        self.assertEqual(dt, result)

    def test_hours_non_boundary(self) -> None:
        """Rounding up to the next hour when dt is not on an hour boundary."""
        dt = datetime(2025, 2, 11, 14, 5, 1, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        expected = dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        self.assertEqual(expected, result)

    def test_hours_non_boundary2(self) -> None:
        """Rounding up to the next hour when dt is not on an hour boundary."""
        dt = datetime(2025, 2, 11, 0, 5, 1, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly", PeriodAnchors(hourly_minute=59))
        expected = dt.replace(minute=59, second=0, microsecond=0) + timedelta(hours=0)
        self.assertEqual(expected, result)

    def test_hours_non_boundary3(self) -> None:
        """Rounding up to the next hour when dt is not on an hour boundary."""
        dt = datetime(2025, 2, 11, 2, 5, 1, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly", PeriodAnchors(hourly_minute=59))
        expected = dt.replace(minute=59, second=0, microsecond=0) + timedelta(hours=0)
        self.assertEqual(expected, result)

    def test_hours_boundary(self) -> None:
        """Rounding up to the next hour when dt is exactly on an hour boundary returns dt."""
        dt = datetime(2025, 2, 11, 14, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        self.assertEqual(dt, result)

    def test_days_non_boundary(self) -> None:
        """Rounding up to the next day when dt is not on a day boundary."""
        dt = datetime(2025, 2, 11, 14, 5, 1, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "daily")
        expected = dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        self.assertEqual(expected, result)

    def test_days_non_boundary2(self) -> None:
        """Rounding up to the next day when dt is not on a day boundary."""
        dt = datetime(2025, 2, 11, 1, 59, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "daily", PeriodAnchors(daily_hour=2))
        expected = dt.replace(hour=2, minute=0, second=0, microsecond=0) + timedelta(days=0)
        self.assertEqual(expected, result)

    def test_days_boundary(self) -> None:
        """Rounding up to the next day when dt is exactly at midnight returns dt."""
        dt = datetime(2025, 2, 11, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "daily")
        self.assertEqual(dt, result)

    def test_weeks_non_boundary_saturday(self) -> None:
        # anchor is the most recent between Friday and Saturday
        dt = datetime(2025, 2, 11, 14, 5, 1, tzinfo=self.tz)  # Tuesday
        expected = datetime(2025, 2, 15, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=PeriodAnchors(weekly_weekday=6))
        self.assertEqual(expected, result)

    def test_weeks_non_boundary_sunday(self) -> None:
        # anchor is the most recent midnight between Saturday and Sunday
        dt = datetime(2025, 2, 11, 14, 5, 1, tzinfo=self.tz)  # Tuesday
        expected = datetime(2025, 2, 16, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=PeriodAnchors(weekly_weekday=0))
        self.assertEqual(expected, result)

    def test_weeks_non_boundary_monday(self) -> None:
        # anchor is the most recent midnight between Sunday and Monday
        dt = datetime(2025, 2, 11, 14, 5, 1, tzinfo=self.tz)  # Tuesday
        expected = datetime(2025, 2, 17, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=PeriodAnchors(weekly_weekday=1))
        self.assertEqual(expected, result)

    def test_weeks_boundary_saturday(self) -> None:
        # dt is exactly at midnight between Friday and Saturday
        dt = datetime(2025, 2, 15, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=PeriodAnchors(weekly_weekday=6))
        self.assertEqual(dt, result)

    def test_weeks_boundary_sunday(self) -> None:
        # dt is exactly at midnight between Saturday and Sunday
        dt = datetime(2025, 2, 16, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=PeriodAnchors(weekly_weekday=0))
        self.assertEqual(dt, result)

    def test_weeks_boundary_monday(self) -> None:
        # dt is exactly at midnight between Sunday and Monday
        dt = datetime(2025, 2, 17, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=PeriodAnchors(weekly_weekday=1))
        self.assertEqual(dt, result)

    def test_months_non_boundary2a(self) -> None:
        """Rounding up to the next multiple of months when dt is not on a boundary."""
        # For a 2-month step, using dt = March 15, 2025 should round up to May 1, 2025.
        dt = datetime(2025, 3, 15, 10, 30, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "monthly")
        expected = datetime(2025, 5, 1, 0, 0, 0, tzinfo=self.tz)
        self.assertEqual(expected, result)

    def test_months_non_boundary2b(self) -> None:
        """Rounding up to the next multiple of months when dt is not on a boundary."""
        # For a 2-month step (Jan, Mar, May...), using dt = April 15, 2025 should round up to May 1, 2025.
        dt = datetime(2025, 4, 15, 10, 30, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "monthly")
        expected = datetime(2025, 5, 1, 0, 0, 0, tzinfo=self.tz)
        self.assertEqual(expected, result)

    def test_months_boundary(self) -> None:
        """When dt is exactly on a month boundary that is a multiple, dt is returned unchanged."""
        dt = datetime(2025, 3, 1, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "monthly")
        self.assertEqual(dt, result)

    def test_years_non_boundary2a(self) -> None:
        """Rounding up to the next multiple of years when dt is not on a boundary."""
        # For a 2-year step with an anchor phase starting in 2025 (e.g. 2025, 2027...),
        # using dt = Feb 11, 2024 should round up to Jan 1, 2025.
        dt = datetime(2024, 2, 11, 14, 5, tzinfo=self.tz)
        anchors = PeriodAnchors(yearly_year=2025)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly", anchors=anchors)
        expected = datetime(2025, 1, 1, 0, 0, 0, tzinfo=self.tz)
        self.assertEqual(expected, result)

    def test_years_non_boundary2b(self) -> None:
        """Rounding up to the next multiple of years when dt is not on a boundary."""
        # For a 2-year step, using dt = Feb 11, 2025 should round up to Jan 1, 2027.
        dt = datetime(2025, 2, 11, 14, 5, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly")
        expected = datetime(2027, 1, 1, 0, 0, 0, tzinfo=self.tz)
        self.assertEqual(expected, result)

    def test_years_boundary(self) -> None:
        """When dt is exactly on a year boundary that is a multiple, dt is returned unchanged."""
        # January 1, 2025 is on a valid boundary if (2025-1) % 2 == 0.
        dt = datetime(2025, 1, 1, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly")
        self.assertEqual(dt, result)

    def test_invalid_unit(self) -> None:
        """Passing an unsupported time unit should raise a ValueError."""
        dt = datetime(2025, 2, 11, 14, 5, tzinfo=self.tz)
        with self.assertRaises(ValueError):
            round_datetime_up_to_duration_multiple(dt, 2, "fortnights")

    def test_preserves_timezone(self) -> None:
        """The returned datetime must have the same timezone as the input."""
        dt = datetime(2025, 2, 11, 14, 5, 1, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        self.assertEqual(dt.tzinfo, result.tzinfo)

    def test_custom_hourly_anchor(self) -> None:
        # Custom hourly: snapshots occur at :15:30 each hour.
        dt = datetime(2025, 2, 11, 14, 20, 0, tzinfo=self.tz)
        # Global base (daily) is 00:00:00, so effective hourly base = 00:15:30.
        # dt is 14:20:00; offset = 14h04m30s -> next multiple with 1-hour step: 15:15:30.
        expected = datetime(2025, 2, 11, 15, 15, 30, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(
            dt, 1, "hourly", anchors=PeriodAnchors(hourly_minute=15, hourly_second=30)
        )
        self.assertEqual(expected, result)

    def test_custom_hourly_anchor2a(self) -> None:
        # Custom hourly: snapshots occur at :15:30 every other hour.
        dt = datetime(2025, 2, 11, 14, 20, 0, tzinfo=self.tz)
        # Global base (daily) is 00:00:00, so effective hourly base = 00:15:30.
        # dt is 14:20:00; offset = 14h04m30s -> next multiple with 1-hour step: 15:15:30.
        expected = datetime(2025, 2, 11, 16, 15, 30, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(
            dt, 2, "hourly", anchors=PeriodAnchors(hourly_minute=15, hourly_second=30)
        )
        self.assertEqual(expected, result)

    def test_custom_hourly_anchor2b(self) -> None:
        # Custom hourly: snapshots occur at :15:30 every other hour.
        dt = datetime(2025, 2, 11, 15, 20, 0, tzinfo=self.tz)
        # Global base (daily) is 00:00:00, so effective hourly base = 00:15:30.
        # dt is 14:20:00; offset = 14h04m30s -> next multiple with 1-hour step: 15:15:30.
        expected = datetime(2025, 2, 11, 16, 15, 30, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(
            dt, 2, "hourly", anchors=PeriodAnchors(hourly_minute=15, hourly_second=30)
        )
        self.assertEqual(expected, result)

    def test_custom_daily_anchor(self) -> None:
        # Custom daily: snapshots occur at 01:30:00.
        custom = PeriodAnchors(daily_hour=1, daily_minute=30, daily_second=0)
        dt = datetime(2025, 2, 11, 0, 45, 0, tzinfo=self.tz)
        # Global base = previous day at 01:30:00, so next boundary = that + 1 day.
        yesterday = dt.replace(hour=custom.daily_hour, minute=custom.daily_minute, second=custom.daily_second)
        if yesterday > dt:
            yesterday -= timedelta(days=1)
        expected = yesterday + timedelta(days=1)
        result = round_datetime_up_to_duration_multiple(dt, 1, "daily", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_weekly_anchor1a(self) -> None:
        # Custom weekly: every week, weekly_weekday=2, weekly time = 03:00:00.
        custom = PeriodAnchors(weekly_weekday=2, weekly_hour=3, weekly_minute=0, weekly_second=0)
        dt = datetime(2025, 2, 13, 5, 0, 0, tzinfo=self.tz)  # Thursday
        anchor = (dt - timedelta(days=2)).replace(hour=3, minute=0, second=0, microsecond=0)
        expected = anchor + timedelta(weeks=1)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_monthly_anchor1a(self) -> None:
        # Custom monthly: snapshots every month on the 15th at 12:00:00.
        custom = PeriodAnchors(monthly_monthday=15, monthly_hour=12, monthly_minute=0, monthly_second=0)
        dt = datetime(2025, 4, 20, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 5, 15, 12, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_monthly_anchor1b(self) -> None:
        # Custom monthly: snapshots every month on the 15th at 12:00:00.
        custom = PeriodAnchors(monthly_monthday=15, monthly_hour=12, monthly_minute=0, monthly_second=0)
        dt = datetime(2025, 5, 12, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 5, 15, 12, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_monthly_anchor2a(self) -> None:
        # Custom monthly: snapshots every other month on the 15th at 12:00:00.
        custom = PeriodAnchors(monthly_monthday=15, monthly_hour=12, monthly_minute=0, monthly_second=0)
        dt = datetime(2025, 4, 20, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 5, 15, 12, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "monthly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_monthly_anchor2b(self) -> None:
        # Custom monthly: snapshots every other month on the 15th at 12:00:00.
        custom = PeriodAnchors(monthly_monthday=15, monthly_hour=12, monthly_minute=0, monthly_second=0)
        dt = datetime(2025, 5, 12, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 5, 15, 12, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "monthly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_monthly_anchor2c(self) -> None:
        # Custom monthly: snapshots every other month on the 15th at 12:00:00.
        custom = PeriodAnchors(monthly_monthday=15, monthly_hour=12, monthly_minute=0, monthly_second=0)
        dt = datetime(2025, 5, 17, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 7, 15, 12, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "monthly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_yearly_anchor1a(self) -> None:
        # Custom yearly: snapshots on June 30 at 02:00:00.
        custom = PeriodAnchors(yearly_month=6, yearly_monthday=30, yearly_hour=2, yearly_minute=0, yearly_second=0)
        dt = datetime(2024, 3, 15, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2024, 6, 30, 2, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "yearly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_yearly_anchor1b(self) -> None:
        # Custom yearly: snapshots on June 30 at 02:00:00.
        custom = PeriodAnchors(yearly_month=6, yearly_monthday=30, yearly_hour=2, yearly_minute=0, yearly_second=0)
        dt = datetime(2025, 3, 15, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 6, 30, 2, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "yearly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_yearly_anchor2a(self) -> None:
        # Custom yearly: snapshots every other year on June 30 at 02:00:00.
        custom = PeriodAnchors(yearly_month=6, yearly_monthday=30, yearly_hour=2, yearly_minute=0, yearly_second=0)
        dt = datetime(2024, 3, 15, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 6, 30, 2, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_yearly_anchor2b(self) -> None:
        # Custom yearly: snapshots every other year on June 30 at 02:00:00.
        custom = PeriodAnchors(yearly_month=6, yearly_monthday=30, yearly_hour=2, yearly_minute=0, yearly_second=0)
        dt = datetime(2025, 3, 15, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 6, 30, 2, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_yearly_anchor2c(self) -> None:
        # Schedule: Every 2 years, starting in 2025.
        # dt: 2025-02-11 (which is AFTER the default anchor of 2025-01-01)
        # Expected result: 2027-01-01
        dt = datetime(2025, 2, 11, 14, 5, tzinfo=self.tz)
        anchors = PeriodAnchors(yearly_year=2025)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly", anchors=anchors)
        expected = datetime(2027, 1, 1, 0, 0, 0, tzinfo=self.tz)
        self.assertEqual(expected, result)

    def test_timezone_preservation(self) -> None:
        # Create a timezone with DST
        tz = ZoneInfo("America/New_York")

        # Create a timestamp just before DST transition
        dt_before_dst = datetime(2023, 3, 12, 1, 30, 0, tzinfo=tz)  # Just before "spring forward"

        # Create custom anchors
        custom = PeriodAnchors(daily_hour=3, daily_minute=0, daily_second=0)

        # Round to next daily anchor - should be 3:00 same day, respecting DST change
        result = round_datetime_up_to_duration_multiple(dt_before_dst, 1, "daily", anchors=custom)

        # Expected result should be 3:00 AM after DST shift (which is actually 4:00 AM in wall clock time)
        # If timezone is handled incorrectly, this will be off by an hour
        expected = datetime(2023, 3, 12, 3, 0, 0, tzinfo=tz)

        # This will fail if the timezone handling is incorrect
        self.assertEqual(expected.hour, result.hour)
        self.assertEqual(expected.tzinfo, result.tzinfo)
        self.assertEqual(expected.utcoffset(), result.utcoffset())

    def test_monthly_anchor_invalid_day(self) -> None:
        # Custom monthly: snapshots every month on the 31st
        custom = PeriodAnchors(monthly_monthday=31, monthly_hour=12, monthly_minute=0, monthly_second=0)

        # February case - should round to Feb 28th (or 29th in leap year) but instead will incorrectly handle this
        dt = datetime(2025, 2, 15, 10, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=custom)

        # This will fail if the function tries to create an invalid date (Feb 31st)
        # or incorrectly jumps to March 31st instead of using Feb 28th
        expected = datetime(2025, 2, 28, 12, 0, 0, tzinfo=self.tz)
        self.assertEqual(expected, result)

    def test_monthly_with_custom_month_anchor(self) -> None:
        """Tests that the `monthly_month` anchor correctly sets the cycle phase."""
        # Anchor: every 1 month, starting in March (month=3), on the 1st day.
        custom_anchors = PeriodAnchors(monthly_month=3, monthly_monthday=1)

        # If it's Jan 15, the next anchor month is February.
        dt = datetime(2025, 1, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2025, 2, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=custom_anchors)
        self.assertEqual(expected, result)

        # If it's March 15, the next anchor month is April.
        dt2 = datetime(2025, 3, 15, 10, 0, tzinfo=self.tz)
        expected2 = datetime(2025, 4, 1, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 1, "monthly", anchors=custom_anchors)
        self.assertEqual(expected2, result2)

    def test_multi_month_with_custom_month_anchor(self) -> None:
        """Tests multi-month logic with a custom `monthly_month` anchor."""
        # Anchor: every 2 months (bi-monthly), starting in April (month=4).
        # Schedule: Apr 1, Jun 1, Aug 1, Oct 1, Dec 1, Feb 1 (+1 year), etc.
        custom_anchors = PeriodAnchors(monthly_month=4, monthly_monthday=2)

        # Test Case 1: Current date is Mar 15. Next snapshot should be Apr 1.
        dt1 = datetime(2025, 3, 15, 10, 0, tzinfo=self.tz)
        expected1 = datetime(2025, 4, 2, 0, 0, tzinfo=self.tz)
        result1 = round_datetime_up_to_duration_multiple(dt1, 2, "monthly", anchors=custom_anchors)
        self.assertEqual(expected1, result1)

        # Test Case 2: Current date is April 15. Next snapshot should be June 1.
        dt2 = datetime(2025, 4, 15, 10, 0, tzinfo=self.tz)
        expected2 = datetime(2025, 6, 2, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 2, "monthly", anchors=custom_anchors)
        self.assertEqual(expected2, result2)

        # Test Case 3: Current date is Dec 15. Next snapshot is Feb 1 of next year.
        dt3 = datetime(2025, 12, 15, 10, 0, tzinfo=self.tz)
        expected3 = datetime(2026, 2, 2, 0, 0, tzinfo=self.tz)
        result3 = round_datetime_up_to_duration_multiple(dt3, 2, "monthly", anchors=custom_anchors)
        self.assertEqual(expected3, result3)

    def test_quarterly_scheduling(self) -> None:
        """Tests a common use case: quarterly snapshots."""
        # Anchor: every 3 months, starting in January (month=1).
        # Schedule: Jan 1, Apr 1, Jul 1, Oct 1.
        custom_anchors = PeriodAnchors(monthly_month=1, monthly_monthday=1)

        # Test Case 1: In February. Next is Apr 1.
        dt1 = datetime(2025, 2, 20, 10, 0, tzinfo=self.tz)
        expected1 = datetime(2025, 4, 1, 0, 0, tzinfo=self.tz)
        result1 = round_datetime_up_to_duration_multiple(dt1, 3, "monthly", anchors=custom_anchors)
        self.assertEqual(expected1, result1)

        # Test Case 2: In June. Next is Jul 1.
        dt2 = datetime(2025, 6, 1, 12, 0, tzinfo=self.tz)
        expected2 = datetime(2025, 7, 1, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 3, "monthly", anchors=custom_anchors)
        self.assertEqual(expected2, result2)

        # Test Case 3: In November. Next is Jan 1 of next year.
        dt3 = datetime(2025, 11, 5, 10, 0, tzinfo=self.tz)
        expected3 = datetime(2026, 1, 1, 0, 0, tzinfo=self.tz)
        result3 = round_datetime_up_to_duration_multiple(dt3, 3, "monthly", anchors=custom_anchors)
        self.assertEqual(expected3, result3)

    def test_multi_year_scheduling_is_correct(self) -> None:
        """Verifies multi-year (3-year) scheduling logic with default anchors."""
        # Schedule: Jan 1 of 2025, 2028, 2031, etc.
        anchors = PeriodAnchors(yearly_year=2025)

        # Case 1: In 2026. Next is 2028.
        dt1 = datetime(2026, 6, 15, 10, 0, tzinfo=self.tz)
        expected1 = datetime(2028, 1, 1, 0, 0, tzinfo=self.tz)
        result1 = round_datetime_up_to_duration_multiple(dt1, 3, "yearly", anchors=anchors)
        self.assertEqual(expected1, result1)

        # Case 2: In 2028, but after the anchor date. Next is 2031.
        dt2 = datetime(2028, 2, 1, 12, 0, tzinfo=self.tz)
        expected2 = datetime(2031, 1, 1, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 3, "yearly", anchors=anchors)
        self.assertEqual(expected2, result2)

    def test_multi_year_with_custom_anchor_properties(self) -> None:
        """Verifies multi-year scheduling with custom month and day anchors."""
        # Schedule: Every 2 years, on July 4th, starting in year 2024.
        # Valid snapshots: 2024-07-04, 2026-07-04, 2028-07-04...
        anchors = PeriodAnchors(yearly_year=2024, yearly_month=7, yearly_monthday=4)

        # Case 1: In 2025. Next boundary is 2026-07-04.
        dt1 = datetime(2025, 8, 1, 10, 0, tzinfo=self.tz)
        expected1 = datetime(2026, 7, 4, 0, 0, tzinfo=self.tz)
        result1 = round_datetime_up_to_duration_multiple(dt1, 2, "yearly", anchors=anchors)
        self.assertEqual(expected1, result1)

        # Case 2: In 2026, but before the anchor date. Next is 2026-07-04.
        dt2 = datetime(2026, 3, 10, 10, 0, tzinfo=self.tz)
        expected2 = datetime(2026, 7, 4, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 2, "yearly", anchors=anchors)
        self.assertEqual(expected2, result2)

        # Case 3 (Boundary): Exactly on the anchor date.
        dt3 = datetime(2026, 7, 4, 0, 0, tzinfo=self.tz)
        result3 = round_datetime_up_to_duration_multiple(dt3, 2, "yearly", anchors=anchors)
        self.assertEqual(dt3, result3)

        # Case 4 (Just after Boundary): One second after the anchor date.
        dt4 = datetime(2026, 7, 4, 0, 0, 1, tzinfo=self.tz)
        expected4 = datetime(2028, 7, 4, 0, 0, tzinfo=self.tz)
        result4 = round_datetime_up_to_duration_multiple(dt4, 2, "yearly", anchors=anchors)
        self.assertEqual(expected4, result4)

    def test_monthly_boundary_exact_time(self) -> None:
        """Tests that a dt exactly on a monthly anchor is returned unchanged."""
        anchors = PeriodAnchors(monthly_monthday=15, monthly_hour=10)
        dt = datetime(2025, 4, 15, 10, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(dt, result)

    def test_monthly_just_after_boundary(self) -> None:
        """Tests that a dt just after a monthly anchor correctly advances to the next month."""
        anchors = PeriodAnchors(monthly_monthday=15, monthly_hour=10)
        dt = datetime(2025, 4, 15, 10, 0, 1, tzinfo=self.tz)  # 1 microsecond after
        expected = datetime(2025, 5, 15, 10, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_before_anchor_day_in_same_month(self) -> None:
        """Tests when dt is in the correct month but before the anchor day."""
        anchors = PeriodAnchors(monthly_monthday=25)
        dt = datetime(2025, 4, 10, 10, 0, tzinfo=self.tz)
        expected = datetime(2025, 4, 25, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_end_of_month_rollover(self) -> None:
        """Tests monthly scheduling at the end of a year."""
        anchors = PeriodAnchors(monthly_monthday=1)
        dt = datetime(2025, 12, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2026, 1, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_anchor_on_31st_for_short_month(self) -> None:
        """Tests that an anchor on day 31 correctly resolves to the last day of a shorter month."""
        anchors = PeriodAnchors(monthly_monthday=31)
        # Next boundary after April 15 should be April 30th
        dt = datetime(2025, 4, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2025, 4, 30, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)
        # Next boundary after Feb 15 (non-leap) should be Feb 28th
        dt2 = datetime(2025, 2, 15, 10, 0, tzinfo=self.tz)
        expected2 = datetime(2025, 2, 28, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 1, "monthly", anchors=anchors)
        self.assertEqual(expected2, result2)

    def test_multi_month_with_phase_wrapping_year(self) -> None:
        """Tests a multi-month schedule where the anchor phase causes cycles to straddle year boundaries."""
        # Schedule: Every 4 months, starting in November.
        # Cycles: ..., Jul 2024, Nov 2024, Mar 2025, Jul 2025, ...
        anchors = PeriodAnchors(monthly_month=11)
        # dt is Jan 2025. The last anchor was Nov 2024. Next is Mar 2025.
        dt = datetime(2025, 1, 20, 10, 0, tzinfo=self.tz)
        expected = datetime(2025, 3, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 4, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_cycle_with_future_anchor_month(self) -> None:
        """Tests that the monthly cycle works correctly when the anchor month is after the current month.

        This is already covered by other tests, but this one makes it explicit.
        """
        # Schedule: Every 3 months (quarterly). Anchor phase is set to start in November (month 11).
        # Schedule is Feb, May, Aug, Nov.
        anchors = PeriodAnchors(monthly_month=11)

        # Current date is March 2025. The last anchor was Nov 2024. The one before that was Aug 2024.
        # The period `dt` falls into started in Feb 2025. The next boundary is May 2025.
        dt = datetime(2025, 3, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2025, 5, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 3, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    # --- Comprehensive Tests for Yearly Scheduling ---

    def test_yearly_just_before_boundary(self) -> None:
        """Tests that a dt just before a yearly anchor correctly snaps to that anchor."""
        anchors = PeriodAnchors(yearly_month=7, yearly_monthday=4)
        dt = datetime(2025, 7, 3, 23, 59, 59, tzinfo=self.tz)
        expected = datetime(2025, 7, 4, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "yearly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_yearly_on_exact_boundary_with_offset_cycle(self) -> None:
        """Tests a dt exactly on a multi-year anchor boundary."""
        # Schedule: Every 3 years, phase starting 2022. (2022, 2025, 2028...)
        anchors = PeriodAnchors(yearly_year=2022)
        dt = datetime(2025, 1, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 3, "yearly", anchors=anchors)
        self.assertEqual(dt, result)

    def test_yearly_just_after_boundary_with_offset_cycle(self) -> None:
        """Tests a dt just after a multi-year anchor boundary."""
        # Schedule: Every 3 years, phase starting 2022. (2022, 2025, 2028...)
        anchors = PeriodAnchors(yearly_year=2022)
        dt = datetime(2025, 1, 1, 0, 0, 1, tzinfo=self.tz)  # 1 microsecond after
        expected = datetime(2028, 1, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 3, "yearly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_yearly_leap_year_anchor_from_non_leap(self) -> None:
        """Tests scheduling for a Feb 29 anchor when the current year is not a leap year."""
        # Schedule: Every year on Feb 29.
        anchors = PeriodAnchors(yearly_month=2, yearly_monthday=29)
        # dt is in 2025 (non-leap). The next valid Feb 29 is in 2028.
        # The anchor for 2025 is 2025-02-28. Since dt is past it, the next is 2026-02-28.
        dt = datetime(2025, 3, 1, 10, 0, tzinfo=self.tz)
        expected = datetime(2026, 2, 28, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "yearly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_yearly_leap_year_anchor_from_leap(self) -> None:
        """Tests scheduling for a Feb 29 anchor when the current year is a leap year."""
        # Schedule: Every year on Feb 29.
        anchors = PeriodAnchors(yearly_month=2, yearly_monthday=29)
        # dt is Jan 2028 (leap). The next boundary is Feb 29, 2028.
        dt = datetime(2028, 1, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2028, 2, 29, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "yearly", anchors=anchors)
        self.assertEqual(expected, result)
        # dt is Mar 2028 (leap), after the anchor. Next boundary is Feb 28, 2029.
        dt2 = datetime(2028, 3, 1, 10, 0, tzinfo=self.tz)
        expected2 = datetime(2029, 2, 28, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 1, "yearly", anchors=anchors)
        self.assertEqual(expected2, result2)

    def test_yearly_cycle_with_future_anchor_year(self) -> None:
        """Tests that the cycle phase works correctly even if the anchor year is in the future.

        The anchor year should only define the phase (e.g., odd/even years), not a starting point.
        """
        # Schedule: Every 2 years. The anchor phase is defined by year 2051 (an odd year).
        # This means snapshots should occur in ..., 2023, 2025, 2027, ...
        anchors = PeriodAnchors(yearly_year=2051)

        # Current date is in 2024. The next snapshot should be in 2025.
        dt = datetime(2024, 5, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2025, 1, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly", anchors=anchors)
        self.assertEqual(expected, result)

        anchors = PeriodAnchors(yearly_year=2050)

        # Current date is in 2024. The next snapshot should be in 2026.
        dt = datetime(2024, 5, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2026, 1, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_anchor_invalid_day_with_custom_anchor_month(self) -> None:
        """Verifies clamping to the last valid day of the anchor month.

        When the requested anchor day exceeds the anchor month length (e.g., 31 in February), the day is clamped to that
        month's last valid day. If the computed anchor is after dt, subtracting one period can yield the corresponding
        clamped day in the previous month.
        """
        tz = self.tz
        anchors = PeriodAnchors(
            monthly_month=2,  # February
            monthly_monthday=31,
            monthly_hour=12,
            monthly_minute=0,
            monthly_second=0,
        )
        dt = datetime(2025, 1, 15, 10, 0, 0, tzinfo=tz)
        # With anchor phase February and day=31 -> base anchor is Feb 28; subtracting one period yields Jan 28.
        expected = datetime(2025, 1, 28, 12, 0, 0, tzinfo=tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    # --- Additional Monthly Tests ---

    def test_monthly_feb29_anchor_in_leap_year(self) -> None:
        """Anchoring on Feb 29: in a leap year, day-of-month 29 is used — next boundary is Jan 29 for Jan dt."""
        anchors = PeriodAnchors(monthly_month=2, monthly_monthday=29, monthly_hour=12, monthly_minute=0, monthly_second=0)
        dt = datetime(2024, 1, 15, 10, 0, tzinfo=self.tz)  # leap year
        expected = datetime(2024, 1, 29, 12, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_feb29_anchor_in_non_leap_year(self) -> None:
        """Anchoring on Feb 29: in a non-leap year, clamps to 28 — next boundary is Jan 28 for Jan dt."""
        anchors = PeriodAnchors(monthly_month=2, monthly_monthday=29, monthly_hour=12, monthly_minute=0, monthly_second=0)
        dt = datetime(2025, 1, 15, 10, 0, tzinfo=self.tz)  # non-leap year
        expected = datetime(2025, 1, 28, 12, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_multi_month_quarterly_with_day31(self) -> None:
        """Quarterly schedule with day=31 clamps correctly in 30-day months."""
        anchors = PeriodAnchors(monthly_month=1, monthly_monthday=31)
        # For quarterly schedule (Jan, Apr, Jul, Oct), April boundary is Apr 30.
        dt = datetime(2025, 4, 10, 9, 0, tzinfo=self.tz)
        expected = datetime(2025, 4, 30, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 3, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_multi_month_quarterly_boundary_exact(self) -> None:
        """Quarterly schedule with day=31: exactly on an Apr 30 boundary stays unchanged."""
        anchors = PeriodAnchors(monthly_month=1, monthly_monthday=31)
        dt = datetime(2025, 4, 30, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 3, "monthly", anchors=anchors)
        self.assertEqual(dt, result)

    def test_monthly_multi_month_before_anchor_day_in_month(self) -> None:
        """For duration=3 and day=15, a date before the 15th snaps to the 15th of the next valid cycle month."""
        anchors = PeriodAnchors(monthly_monthday=15)
        dt = datetime(2025, 3, 10, 10, 0, tzinfo=self.tz)
        expected = datetime(2025, 4, 15, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 3, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_future_anchor_month_still_snap_in_current_month(self) -> None:
        """Anchor month after current month: still snaps within current month if due before anchor time."""
        anchors = PeriodAnchors(monthly_month=9, monthly_monthday=10)  # September phase
        dt = datetime(2025, 3, 3, 12, 0, tzinfo=self.tz)
        expected = datetime(2025, 3, 10, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_custom_anchor_month_31_from_leap_feb(self) -> None:
        """If anchor month is February with day=31, in a leap year base anchor is Feb 29; next is Mar 29."""
        anchors = PeriodAnchors(monthly_month=2, monthly_monthday=31)
        dt = datetime(2024, 3, 1, 10, 0, tzinfo=self.tz)  # leap year; past Feb anchor
        expected = datetime(2024, 3, 29, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_large_duration_over_year(self) -> None:
        """Duration > 12 months: 14-month schedule jumps into next year at the correct month/day."""
        anchors = PeriodAnchors()  # default Jan 1
        dt = datetime(2025, 5, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2026, 3, 1, 0, 0, tzinfo=self.tz)  # Jan 1 + 14 months = Mar 1 next year
        result = round_datetime_up_to_duration_multiple(dt, 14, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_exact_boundary_with_custom_time(self) -> None:
        """Exactly on boundary with custom time-of-day returns dt unchanged."""
        anchors = PeriodAnchors(monthly_month=10, monthly_monthday=5, monthly_hour=6, monthly_minute=7, monthly_second=8)
        dt = datetime(2025, 10, 5, 6, 7, 8, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(dt, result)

    def test_monthly_just_before_boundary_with_custom_time(self) -> None:
        """Just before boundary should snap up to the boundary on the same day and time-of-day."""
        anchors = PeriodAnchors(monthly_month=10, monthly_monthday=5, monthly_hour=6, monthly_minute=7, monthly_second=8)
        dt = datetime(2025, 10, 5, 6, 7, 7, tzinfo=self.tz)
        expected = datetime(2025, 10, 5, 6, 7, 8, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_multi_month_just_after_boundary_with_phase(self) -> None:
        """Multi-month with custom phase: just after boundary advances by full duration months with time preserved."""
        anchors = PeriodAnchors(monthly_month=11, monthly_monthday=30, monthly_hour=2)
        dt = datetime(2025, 11, 30, 2, 0, 0, 1, tzinfo=self.tz)  # just after boundary
        expected = datetime(2026, 1, 30, 2, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "monthly", anchors=anchors)
        self.assertEqual(expected, result)
