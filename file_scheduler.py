"""Daily UTC scheduling with an optional short demonstration interval."""

import asyncio
import logging
import math
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone


class FileSchedule:
    def __init__(self, hour: int = 2, minute: int = 0, interval_seconds: float | None = None):
        if not 0 <= hour <= 23 or not 0 <= minute <= 59:
            raise ValueError("Invalid nightly hour or minute")
        if interval_seconds is not None and (
            not math.isfinite(interval_seconds) or interval_seconds <= 0
        ):
            raise ValueError("Demo interval must be positive and finite")
        self.hour = hour
        self.minute = minute
        self.interval_seconds = interval_seconds

    def next_run(self, now: datetime) -> datetime:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Scheduling requires timezone-aware time")
        now = now.astimezone(timezone.utc)
        if self.interval_seconds is not None:
            return now + timedelta(seconds=self.interval_seconds)
        target = now.replace(hour=self.hour, minute=self.minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return target


class FileScheduler:
    def __init__(self, schedule: FileSchedule):
        self.schedule = schedule

    async def run(
        self,
        action: Callable[[], Awaitable[None]],
        stop: asyncio.Event,
        run_immediately: bool = False,
    ) -> None:
        if run_immediately and not stop.is_set():
            await action()
        while not stop.is_set():
            now = datetime.now(timezone.utc)
            target = self.schedule.next_run(now)
            logging.info("FILES next collection at %s", target.isoformat())
            try:
                await asyncio.wait_for(stop.wait(), timeout=(target - now).total_seconds())
            except asyncio.TimeoutError:
                if not stop.is_set():
                    await action()
        logging.info("FILES scheduler stopped")
