import asyncio
import unittest
from unittest.mock import Mock, patch

import app
from message_batching import Message


class ShutdownTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    async def test_stop_flushes_partial_batch_and_finishes_worker(self):
        waiting = asyncio.Event()
        stop = asyncio.Event()

        async def messages():
            yield Message(1, "accepted before stop")
            waiting.set()
            await asyncio.Event().wait()

        source = Mock()
        source.messages.side_effect = messages
        processor = Mock(return_value=1)
        with patch.object(app, "PoissonMessageSource", return_value=source), patch.object(app, "process_batch", processor):
            task = asyncio.create_task(app.run(10, 300, stop=stop))
            try:
                await asyncio.wait_for(waiting.wait(), timeout=2)
            finally:
                stop.set()
                await asyncio.wait_for(task, timeout=2)
        processor.assert_called_once()
        self.assertEqual(processor.call_args.args[0].messages, (Message(1, "accepted before stop"),))
        print("  Actual: stop ended idle source, flushed message 1 and completed processing", flush=True)

    async def test_file_source_failure_stops_continuous_message_source(self):
        async def messages():
            await asyncio.Event().wait()
            yield Message(1, "never emitted")

        source = Mock()
        source.messages.side_effect = messages
        files = Mock()
        files.prepare_buckets.side_effect = OSError("File source failed")
        with patch.object(app, "PoissonMessageSource", return_value=source), patch.object(app, "FileBatchService", return_value=files):
            with self.assertLogs(level="ERROR"):
                with self.assertRaisesRegex(RuntimeError, "A data source failed"):
                    await asyncio.wait_for(app.run(10, 300, files_now=True), timeout=2)
        print("  Actual: file error stopped both producers and remained visible", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
