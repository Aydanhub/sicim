"""Schedule specs — interval, one-shot and cron — and scheduled run ids.

A schedule is a standing instruction to *start a run* of a workflow at certain
times (``Runtime.schedule``). Three spec kinds exist:

* ``every=N``  — every N seconds (a number, a ``timedelta`` or a duration
  string such as ``"30s"``, ``"5m"``, ``"1h30m"``), aligned to the previous
  fire time;
* ``at=T``     — once, at UNIX timestamp T (or a ``datetime``);
* ``cron="…"`` — classic five-field cron (``minute hour day-of-month month
  day-of-week``) with ``*``, lists, ranges, steps, month/weekday names and the
  ``@hourly``-style aliases. A sixth, *leading* field adds seconds
  (``"*/10 * * * * *"``: every ten seconds). ``@every <duration>`` is accepted
  as an alias for an interval. Evaluated in UTC unless an IANA ``tz`` is given.

Specs serialize to short strings (``"every 3600"``, ``"at 1725000000.0"``,
``"cron */5 * * * *"``) so schedule records stay plain data in the store.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from typing import Union
from zoneinfo import ZoneInfo

_MONTHS = {
    name: number
    for number, name in enumerate(
        ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"), start=1
    )
}
_WEEKDAYS = {name: number for number, name in enumerate(("sun", "mon", "tue", "wed", "thu", "fri", "sat"))}
_ALIASES = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}
#: Day steps to search before declaring a cron expression unsatisfiable
#: (Feb 29 on a given weekday recurs within 28 years; 9 years covers every
#: realistic expression, and the parse-time check runs once).
_SEARCH_DAYS = 366 * 9

_DURATION_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0, "w": 604800.0}
_DURATION_RE = re.compile(r"(\d+(?:\.\d+)?)(ms|s|m|h|d|w)?")


def parse_duration(text: str) -> float:
    """Parse a duration string into seconds.

    Accepts a bare number of seconds (``"90"``) or unit-suffixed parts that
    add up (``"90s"``, ``"5m"``, ``"1h30m"``, ``"2d12h"``, ``"1.5h"``,
    ``"250ms"``); units are ``ms s m h d w``. Whitespace between parts is
    ignored. Raises ``ValueError`` for anything else.
    """
    if not isinstance(text, str):
        raise TypeError(f"duration must be a string, got {type(text).__name__}")
    compact = "".join(text.split()).lower()
    if not compact:
        raise ValueError("duration must not be empty")
    total, pos = 0.0, 0
    while pos < len(compact):
        match = _DURATION_RE.match(compact, pos)
        if match is None:
            raise ValueError(f"invalid duration {text!r}")
        value, unit = match.groups()
        if unit is None and (pos > 0 or match.end() != len(compact)):
            raise ValueError(f"invalid duration {text!r} (examples: 90, 30s, 5m, 1h30m, 2d)")
        total += float(value) * _DURATION_UNITS.get(unit or "s", 1.0)
        pos = match.end()
    return total


def _parse_field(text: str, low: int, high: int, names: dict[str, int]) -> tuple[frozenset[int], bool]:
    """Parse one cron field into its value set; also report whether the field
    is unrestricted (starts with ``*``), which matters for the day-of-month /
    day-of-week OR rule."""

    def number(token: str) -> int:
        token = token.strip().lower()
        if token in names:
            return names[token]
        try:
            return int(token)
        except ValueError:
            raise ValueError(f"invalid cron token {token!r} in field {text!r}") from None

    values: set[int] = set()
    for part in text.split(","):
        body, _, step_text = part.partition("/")
        step = 1
        if step_text:
            if not step_text.isdigit() or int(step_text) < 1:
                raise ValueError(f"invalid cron step {step_text!r} in field {text!r}")
            step = int(step_text)
        if body == "*":
            start, end = low, high
        elif "-" in body:
            first, last = body.split("-", 1)
            start, end = number(first), number(last)
        else:
            start = number(body)
            end = high if step_text else start
        if not (low <= start <= high and low <= end <= high) or start > end:
            raise ValueError(f"cron field {text!r} is outside the range {low}-{high}")
        values.update(range(start, end + 1, step))
    return frozenset(values), text.startswith("*")


@dataclass(frozen=True)
class CronExpr:
    """A parsed cron expression: five fields, or six with a leading seconds field."""

    text: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]  # 0 = Sunday … 6 = Saturday
    any_day: bool
    any_weekday: bool
    seconds: frozenset[int] = frozenset({0})

    @classmethod
    def parse(cls, text: str) -> "CronExpr":
        expr = _ALIASES.get(text.strip().lower(), text.strip())
        fields = expr.split()
        if len(fields) == 6:
            seconds, _ = _parse_field(fields[0], 0, 59, {})
            fields = fields[1:]
        elif len(fields) == 5:
            seconds = frozenset({0})
        else:
            raise ValueError(
                f"cron expression {text!r} must have 5 fields "
                "(minute hour day-of-month month day-of-week) or 6 (a leading seconds field)"
            )
        minutes, _ = _parse_field(fields[0], 0, 59, {})
        hours, _ = _parse_field(fields[1], 0, 23, {})
        days, any_day = _parse_field(fields[2], 1, 31, {})
        months, _ = _parse_field(fields[3], 1, 12, _MONTHS)
        weekdays, any_weekday = _parse_field(fields[4], 0, 7, _WEEKDAYS)
        spec = cls(
            text=text.strip(),
            minutes=minutes,
            hours=hours,
            days=days,
            months=months,
            weekdays=frozenset(day % 7 for day in weekdays),  # 7 is Sunday too
            any_day=any_day,
            any_weekday=any_weekday,
            seconds=seconds,
        )
        if spec.next_after(0.0, dt.timezone.utc) is None:
            raise ValueError(f"cron expression {text!r} never fires")
        return spec

    @property
    def has_seconds(self) -> bool:
        """True when the expression carries an explicit seconds field."""
        return self.seconds != frozenset({0})

    def _day_matches(self, day: dt.date) -> bool:
        by_month_day = day.day in self.days
        by_weekday = day.isoweekday() % 7 in self.weekdays
        if self.any_day and self.any_weekday:
            return True
        if self.any_day:
            return by_weekday
        if self.any_weekday:
            return by_month_day
        return by_month_day or by_weekday  # both restricted: classic cron ORs them

    def next_after(self, ts: float, tz: dt.tzinfo) -> float | None:
        """First firing time strictly after ``ts`` (UNIX seconds), or None."""
        current = dt.datetime.fromtimestamp(ts, tz).replace(microsecond=0)
        current += dt.timedelta(seconds=1)
        hours, minutes, seconds = sorted(self.hours), sorted(self.minutes), sorted(self.seconds)
        for _ in range(_SEARCH_DAYS):
            if current.month in self.months and self._day_matches(current.date()):
                for hour in hours:
                    if hour < current.hour:
                        continue
                    same_hour = hour == current.hour
                    for minute in minutes:
                        if same_hour and minute < current.minute:
                            continue
                        same_minute = same_hour and minute == current.minute
                        for second in seconds:
                            if same_minute and second < current.second:
                                continue
                            candidate = current.replace(hour=hour, minute=minute, second=second).timestamp()
                            if candidate > ts:  # DST folds can map a wall time backwards
                                return candidate
            current = (current + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0)
        return None


@dataclass(frozen=True)
class Every:
    """Fixed interval, aligned to the previous fire time (no drift)."""

    seconds: float

    @property
    def text(self) -> str:
        return f"every {self.seconds!r}"

    def first_fire_at(self, now: float) -> float:
        return now + self.seconds

    def next_after(self, ts: float) -> float | None:
        return ts + self.seconds


@dataclass(frozen=True)
class Once:
    """A single firing at a fixed time (fires immediately if already past)."""

    at: float

    @property
    def text(self) -> str:
        return f"at {self.at!r}"

    def first_fire_at(self, now: float) -> float:
        return self.at

    def next_after(self, ts: float) -> float | None:
        return self.at if self.at > ts else None


@dataclass(frozen=True)
class Cron:
    """A cron expression evaluated in ``tz`` (IANA name; None = UTC)."""

    expr: CronExpr
    tz: str | None = None

    @property
    def text(self) -> str:
        return f"cron {self.expr.text}"

    def _tzinfo(self) -> dt.tzinfo:
        return ZoneInfo(self.tz) if self.tz else dt.timezone.utc

    def first_fire_at(self, now: float) -> float | None:
        return self.next_after(now)

    def next_after(self, ts: float) -> float | None:
        return self.expr.next_after(ts, self._tzinfo())


Spec = Union[Every, Once, Cron]


def _interval(seconds: float, what: str) -> Every:
    if not seconds > 0:
        raise ValueError(f"{what} must be a positive duration")
    return Every(seconds)


def build_spec(
    *,
    every: float | dt.timedelta | str | None = None,
    cron: str | None = None,
    at: float | dt.datetime | None = None,
    tz: str | None = None,
) -> Spec:
    """Validate ``Runtime.schedule`` arguments and build the spec."""
    given = [name for name, value in (("every", every), ("cron", cron), ("at", at)) if value is not None]
    if len(given) != 1:
        raise ValueError("exactly one of every=, cron= or at= is required")
    if every is not None:
        if isinstance(every, dt.timedelta):
            seconds = every.total_seconds()
        elif isinstance(every, str):
            seconds = parse_duration(every)
        else:
            seconds = float(every)
        if tz is not None:
            raise ValueError("tz= only applies to cron= schedules")
        return _interval(seconds, "every=")
    if at is not None:
        if tz is not None:
            raise ValueError("tz= only applies to cron= schedules")
        return Once(at.timestamp() if isinstance(at, dt.datetime) else float(at))
    assert cron is not None
    text = cron.strip()
    if text.lower().startswith("@every"):
        if tz is not None:
            raise ValueError("tz= does not apply to '@every' intervals")
        return _interval(parse_duration(text[len("@every"):]), "'@every' interval")
    if tz is not None:
        try:
            ZoneInfo(tz)
        except Exception:
            raise ValueError(f"unknown time zone {tz!r}") from None
    return Cron(CronExpr.parse(text), tz)


def parse_spec(text: str, tz: str | None = None) -> Spec:
    """Rebuild a spec from its stored ``ScheduleRecord.spec`` string."""
    kind, _, rest = text.partition(" ")
    if kind == "every":
        return Every(float(rest))
    if kind == "at":
        return Once(float(rest))
    if kind == "cron":
        return Cron(CronExpr.parse(rest), tz)
    raise ValueError(f"unknown schedule spec {text!r}")


def scheduled_run_id(schedule_id: str, fire_at: float) -> str:
    """Deterministic run id for one tick of a schedule.

    Every worker that sees the same due tick derives the same id, so the
    store's primary key — not coordination — guarantees that a tick starts at
    most one run. Millisecond resolution; example ``nightly@2026-09-03T02:00:00.000Z``.
    """
    stamp = dt.datetime.fromtimestamp(fire_at, dt.timezone.utc)
    return f"{schedule_id}@{stamp.strftime('%Y-%m-%dT%H:%M:%S')}.{stamp.microsecond // 1000:03d}Z"
