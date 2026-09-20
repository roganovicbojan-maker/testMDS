import asyncio
import unittest
from threading import Event
from unittest.mock import Mock, patch

import app
from file_pipeline import BYTES_PER_MB, FileItem, SimulatedFileProcessor
from message_batching import Message
from work_dispatcher import WorkDispatcher


class AppIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    async def test_both_pipelines_finish_through_one_dispatcher_without_orphan_tasks(self):
        loop = asyncio.get_running_loop()
        baseline = asyncio.all_tasks()
        file_processed = asyncio.Event()
        first_started = asyncio.Event()
        release_first = Event()
        first_was_blocked = []
        batches = []
        dispatchers = []
        files = [FileItem(str(i), size * BYTES_PER_MB)
                 for i, size in enumerate((6, 6, 4))]
        file_source = Mock()
        file_source.collect.return_value = files
        actual_file_processor = SimulatedFileProcessor(delay_seconds=0)

        def process_files(bucket_id, bucket):
            result = actual_file_processor.process(bucket_id, bucket)
            loop.call_soon_threadsafe(file_processed.set)
            return result

        file_processor = Mock()
        file_processor.process.side_effect = process_files

        async def messages():
            # Events control ordering; no assumptions about random arrival times.
            await file_processed.wait()
            yield Message(1, "first")
            await first_started.wait()
            yield Message(2, "second")

        def process_messages(batch, delay_seconds=0):
            batches.append(batch)
            if batch.messages[0].message_id == 1:
                loop.call_soon_threadsafe(first_started.set)
                if not release_first.wait(5):
                    raise TimeoutError("Second message worker did not start")
            else:
                first_was_blocked.append(not release_first.is_set())
                release_first.set()
            return len(batch.messages)

        def create_dispatcher(pool):
            dispatcher = WorkDispatcher(pool)
            dispatchers.append(dispatcher)
            return dispatcher

        message_source = Mock()
        message_source.messages.side_effect = messages
        with patch.object(app, "ExponentialFileSource", return_value=file_source), \
                patch.object(app, "PoissonMessageSource", return_value=message_source), \
                patch.object(app, "SimulatedFileProcessor", return_value=file_processor), \
                patch.object(app, "process_batch", process_messages), \
                patch.object(app, "WorkDispatcher", side_effect=create_dispatcher):
            try:
                await asyncio.wait_for(app.run(10, 0.02, files_now=True, message_delay=0), 3)
            finally:
                release_first.set()

        file_source.collect.assert_called_once()
        processed = [call.args[1] for call in file_processor.process.call_args_list]
        self.assertCountEqual(processed, [(files[0],), (files[1], files[2])])
        self.assertEqual([b.messages for b in batches],
                         [(Message(1, "first"),), (Message(2, "second"),)])
        self.assertEqual(first_was_blocked, [True])
        self.assertEqual(len(dispatchers), 1)
        self.assertEqual(dispatchers[0].completed, 4)
        self.assertEqual(dispatchers[0].failures, 0)
        self.assertEqual(dispatchers[0].pending_count, 0)
        self.assertEqual(asyncio.all_tasks() - baseline, set())
        print("  Actual: two file buckets and two message batches finished in one dispatcher; "
              "second message worker ran while the first waited; no pending jobs or tasks", flush=True)

    async def test_each_source_failure_preserves_cause_and_traceback(self):
        for failing_source in ("messages", "files"):
            with self.subTest(source=failing_source):
                baseline = asyncio.all_tasks()
                failure = OSError(f"{failing_source} unavailable")

                async def messages():
                    if failing_source == "messages":
                        raise failure
                    await asyncio.Event().wait()
                    yield Message(1, "unreachable")

                source = Mock()
                source.messages.side_effect = messages
                files = Mock()
                if failing_source == "files":
                    files.collect.side_effect = failure
                else:
                    files.collect.return_value = []
                with patch.object(app, "PoissonMessageSource", return_value=source), \
                        patch.object(app, "ExponentialFileSource", return_value=files), \
                        self.assertLogs(level="ERROR") as logs:
                    with self.assertRaisesRegex(RuntimeError, "A data source failed") as caught:
                        await asyncio.wait_for(app.run(10, 300, files_now=True), 2)
                self.assertIs(caught.exception.__cause__, failure)
                self.assertIsNotNone(failure.__traceback__)
                self.assertTrue(any(record.exc_info and record.exc_info[1] is failure
                                    for record in logs.records))
                self.assertEqual(asyncio.all_tasks() - baseline, set())
        print("  Actual: either source failure keeps its original cause and traceback; "
              "the other producer stops without orphan tasks", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
