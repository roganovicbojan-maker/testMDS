import asyncio
import unittest
from threading import Event
from unittest.mock import AsyncMock, Mock, patch

import app
from message_batching import Message, MessageBatch, TimeBatcher
from message_service import MessageBatchService


class CloseableSource:
    def __init__(self, values):
        self.read = AsyncMock(side_effect=values)
        self.aclose = AsyncMock()

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.read()


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    async def test_eof_closes_source_and_flushes_once(self):
        source = CloseableSource([Message(1, "a"), StopAsyncIteration])
        submit = Mock()
        await MessageBatchService(TimeBatcher(), submit).run(source)
        source.aclose.assert_awaited_once_with()
        submit.assert_called_once()
        self.assertEqual(submit.call_args.args[0].messages, (Message(1, "a"),))
        print("  Actual: EOF closes source and submits the partial batch once", flush=True)

    async def test_submission_failure_closes_suspended_generator(self):
        closed = asyncio.Event()
        async def source():
            try:
                yield Message(2, "new message")
            finally:
                closed.set()
        batcher = Mock(spec=TimeBatcher)
        batcher.deadline = None
        batcher.add.return_value = MessageBatch((Message(1, "previous"),), 0, 300)
        batcher.flush.return_value = None
        failure = RuntimeError("Submit failed")
        generator = source()
        try:
            with self.assertRaises(RuntimeError) as caught:
                await MessageBatchService(batcher, Mock(side_effect=failure)).run(generator)
            self.assertIs(caught.exception, failure)
            self.assertTrue(closed.is_set())
        finally:
            await generator.aclose()
        print("  Actual: suspended generator closed before submission error returns", flush=True)

    async def test_cleanup_failure_preserves_original_error_and_flushes(self):
        original = OSError("Source failed")
        source = CloseableSource([Message(1, "accepted"), original])
        source.aclose.side_effect = RuntimeError("Close failed")
        submit = Mock()
        with self.assertLogs(level="ERROR") as logs:
            with self.assertRaises(OSError) as caught:
                await MessageBatchService(TimeBatcher(), submit).run(source)
        self.assertIs(caught.exception, original)
        source.aclose.assert_awaited_once()
        submit.assert_called_once()
        self.assertTrue(any("Close failed" in line for line in logs.output))
        print("  Actual: original source error preserved; close failure logged; partial batch submitted", flush=True)

    async def test_close_failure_on_normal_eof_is_reported(self):
        source = CloseableSource([StopAsyncIteration])
        source.aclose.side_effect = RuntimeError("Close failed")
        with self.assertLogs(level="ERROR"):
            with self.assertRaisesRegex(RuntimeError, "Close failed"):
                await MessageBatchService(TimeBatcher(), Mock()).run(source)
        print("  Actual: cleanup error is not hidden on an otherwise successful run", flush=True)

    async def test_iterator_without_aclose_is_supported(self):
        class Source:
            async def __anext__(self):
                raise StopAsyncIteration
            def __aiter__(self):
                return self
        submit = Mock()
        await MessageBatchService(TimeBatcher(), submit).run(Source())
        submit.assert_not_called()
        print("  Actual: standard async iterator without aclose remains supported", flush=True)

    async def test_direct_app_cancellation_drains_worker_and_leaves_no_tasks(self):
        ready, worker_started = asyncio.Event(), asyncio.Event()
        release_worker = Event()
        closed = asyncio.Event()
        loop = asyncio.get_running_loop()
        baseline = asyncio.all_tasks()
        received = []
        async def messages():
            try:
                yield Message(1, "accepted")
                ready.set()
                await asyncio.Event().wait()
            finally:
                closed.set()
        def process(batch, delay_seconds=3):
            received.append(batch)
            loop.call_soon_threadsafe(worker_started.set)
            if not release_worker.wait(5):
                raise TimeoutError("Test did not release worker")
            return len(batch.messages)
        source = Mock()
        source.messages.side_effect = messages
        with patch.object(app, "PoissonMessageSource", return_value=source), patch.object(app, "process_batch", process):
            task = asyncio.create_task(app.run(10, 300))
            try:
                await asyncio.wait_for(ready.wait(), 2)
                task.cancel()
                await asyncio.wait_for(worker_started.wait(), 2)
                self.assertFalse(task.done())
                task.cancel()  # A second request must not cancel the drain.
                await asyncio.sleep(0)
                self.assertFalse(task.done())
            finally:
                release_worker.set()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 2)
        self.assertTrue(closed.is_set())
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].messages, (Message(1, "accepted"),))
        self.assertEqual(asyncio.all_tasks() - baseline, set())
        print("  Actual: repeated cancellation drained the worker, closed source and left no tasks", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
