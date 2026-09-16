import asyncio
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import Mock

from message_batching import Message, TimeBatcher
from message_service import MessageBatchService


class MessageServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    async def test_deadline_submits_while_source_is_still_waiting(self):
        submitted = asyncio.Event()
        release_source = asyncio.Event()
        batches = []

        async def source():
            yield Message(1, "only message")
            await release_source.wait()

        def submit(batch):
            batches.append(batch)
            submitted.set()

        service = MessageBatchService(TimeBatcher(0.02), submit)
        task = asyncio.create_task(service.run(source()))
        try:
            await asyncio.wait_for(submitted.wait(), timeout=2)
            self.assertFalse(task.done())
            self.assertEqual([m.message_id for m in batches[0].messages], [1])
            print("  Actual: timer submitted message 1 while the source remained open and idle", flush=True)
        finally:
            release_source.set()
            await asyncio.wait_for(task, timeout=2)
        self.assertEqual(len(batches), 1)

    async def test_next_batch_is_submitted_while_previous_worker_is_blocked(self):
        first_started = asyncio.Event()
        release_worker = Event()
        loop = asyncio.get_running_loop()
        jobs = []
        batches = []

        async def source():
            yield Message(1, "first batch")
            await first_started.wait()
            yield Message(2, "next batch")

        def process(batch):
            if batch.messages[0].message_id == 1:
                loop.call_soon_threadsafe(first_started.set)
                if not release_worker.wait(timeout=5):
                    raise TimeoutError("Test did not release first worker")
            return len(batch.messages)

        with ThreadPoolExecutor(max_workers=10) as pool:
            def submit(batch):
                batches.append(batch)
                jobs.append(pool.submit(process, batch))

            service = MessageBatchService(TimeBatcher(0.02), submit)
            try:
                await asyncio.wait_for(service.run(source()), timeout=2)
                self.assertEqual([b.messages[0].message_id for b in batches], [1, 2])
                self.assertFalse(jobs[0].done())
                print("  Actual: second batch received and submitted while first worker remains blocked", flush=True)
            finally:
                release_worker.set()
                await asyncio.gather(*(asyncio.wrap_future(job) for job in jobs))

    async def test_end_of_source_flushes_partial_batch(self):
        async def source():
            yield Message(1, "a")
            yield Message(2, "b")

        submit = Mock()
        await MessageBatchService(TimeBatcher(), submit).run(source())
        submit.assert_called_once()
        batch = submit.call_args.args[0]
        self.assertEqual([m.message_id for m in batch.messages], [1, 2])
        print("  Expected and actual: EOF submits messages [1, 2] without waiting 300 seconds", flush=True)

    async def test_empty_source_submits_nothing(self):
        async def source():
            for message in []:
                yield message

        submit = Mock()
        await MessageBatchService(TimeBatcher(), submit).run(source())
        submit.assert_not_called()
        print("  Expected and actual: empty source produces no batch", flush=True)

    async def test_source_error_is_visible_and_accepted_messages_are_flushed(self):
        async def source():
            yield Message(1, "accepted")
            raise OSError("Source disconnected")

        submit = Mock()
        with self.assertRaisesRegex(OSError, "Source disconnected"):
            await MessageBatchService(TimeBatcher(), submit).run(source())
        submit.assert_called_once()
        self.assertEqual(submit.call_args.args[0].messages, (Message(1, "accepted"),))
        print("  Actual: source error propagated; accepted message submitted once", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)

