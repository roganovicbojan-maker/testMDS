import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from file_scheduler import FileSchedule, FileScheduler


class ScheduleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    async def test_daily_time_before_at_and_after_deadline(self):
        schedule = FileSchedule(2, 0)
        for hour, expected_day in ((1, 16), (2, 17), (3, 17)):
            with self.subTest(hour=hour):
                now = datetime(2026, 9, 16, hour, tzinfo=timezone.utc)
                actual = schedule.next_run(now)
                expected = datetime(2026, 9, expected_day, 2, tzinfo=timezone.utc)
                print(f"  Now: {now}; expected: {expected}; actual: {actual}", flush=True)
                self.assertEqual(actual, expected)

    async def test_interval_crosses_midnight(self):
        now = datetime(2026, 9, 16, 23, 59, 59, tzinfo=timezone.utc)
        actual = FileSchedule(interval_seconds=2).next_run(now)
        self.assertEqual(actual, datetime(2026, 9, 17, 0, 0, 1, tzinfo=timezone.utc))
        print(f"  Expected and actual: two seconds later across midnight: {actual}", flush=True)

    async def test_repeated_collections_can_stop_without_waiting_for_another_run(self):
        stop = asyncio.Event()
        calls = []

        async def action():
            calls.append(len(calls) + 1)
            if len(calls) == 2:
                stop.set()

        scheduler = FileScheduler(FileSchedule(interval_seconds=0.01))
        await asyncio.wait_for(scheduler.run(action, stop, run_immediately=True), timeout=2)
        self.assertEqual(calls, [1, 2])
        print(f"  Expected two collections, then stop; actual: {calls}", flush=True)

    async def test_stop_interrupts_long_nightly_wait(self):
        stop = asyncio.Event()
        action = AsyncMock()
        task = asyncio.create_task(FileScheduler(FileSchedule()).run(action, stop))
        await asyncio.sleep(0)
        stop.set()
        await asyncio.wait_for(task, timeout=2)
        action.assert_not_awaited()
        print("  Actual: stop interrupted nightly wait; no collection started", flush=True)

    async def test_invalid_schedule_is_rejected(self):
        for kwargs in ({"hour": 24}, {"minute": 60}, {"interval_seconds": 0}, {"interval_seconds": float("nan")}):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    FileSchedule(**kwargs)
        with self.assertRaises(ValueError):
            FileSchedule().next_run(datetime(2026, 9, 16))
        print("  Invalid schedule values and timezone-free time rejected", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
