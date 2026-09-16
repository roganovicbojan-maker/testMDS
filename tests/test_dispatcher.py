import asyncio
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock

from work_dispatcher import WorkDispatcher


class DispatcherTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    async def test_file_and_message_jobs_share_ten_worker_limit(self):
        all_started, release, message_started = Event(), Event(), Event()
        lock = Lock()
        started = 0

        def file_job():
            nonlocal started
            with lock:
                started += 1
                if started == 10:
                    all_started.set()
            if not release.wait(5):
                raise TimeoutError("Test did not release workers")
            return "file"

        def message_job():
            message_started.set()
            return "message"

        with ThreadPoolExecutor(max_workers=10) as pool:
            dispatcher = WorkDispatcher(pool)
            for index in range(10):
                dispatcher.submit(f"file {index}", file_job)
            try:
                self.assertTrue(await asyncio.to_thread(all_started.wait, 2))
                dispatcher.submit("message", message_job)
                self.assertFalse(message_started.is_set())
                print("  Ten file workers occupied; message accepted into the same queue", flush=True)
            finally:
                release.set()
            results = await asyncio.wait_for(dispatcher.drain(), timeout=3)
        self.assertEqual(results, 11)
        self.assertEqual(started, 10)
        self.assertTrue(message_started.is_set())
        print("  Actual: all 11 jobs completed after workers were released", flush=True)

    async def test_failure_is_reported_after_other_jobs_finish(self):
        completed = Event()

        def fail():
            raise OSError("Example worker failure")

        def succeed():
            completed.set()
            return "ok"

        with ThreadPoolExecutor(max_workers=10) as pool:
            dispatcher = WorkDispatcher(pool)
            dispatcher.submit("bad job", fail)
            dispatcher.submit("good job", succeed)
            with self.assertLogs(level="ERROR") as logs:
                with self.assertRaisesRegex(RuntimeError, "1 processing jobs failed"):
                    await dispatcher.drain()
            self.assertTrue(completed.is_set())
            self.assertTrue(any("bad job" in line for line in logs.output))
        print("  Actual: failed job identified; successful job also completed", flush=True)

    async def test_completed_jobs_are_removed_before_shutdown(self):
        with ThreadPoolExecutor(max_workers=10) as pool:
            dispatcher = WorkDispatcher(pool)
            dispatcher.submit("short job", lambda: 42)
            async def wait_for_completion():
                while dispatcher.completed == 0:
                    await asyncio.sleep(0)
            await asyncio.wait_for(wait_for_completion(), timeout=2)
            self.assertEqual(dispatcher.pending_count, 0)
            self.assertEqual(dispatcher.completed, 1)
        print("  Actual: completed Future removed before drain; only counter retained", flush=True)

    async def test_empty_drain_returns_no_results(self):
        with ThreadPoolExecutor(max_workers=10) as pool:
            results = await WorkDispatcher(pool).drain()
        self.assertEqual(results, 0)
        print("  Expected and actual: no jobs, no results", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)

