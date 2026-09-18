import unittest
from unittest.mock import Mock, patch

import app
from message_batching import Message, MessageBatch
from message_service import process_batch


class SimulationOptionsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    async def test_seed_reproduces_distinct_source_streams(self):
        async def samples(seed):
            with patch.object(app, "ExponentialFileSource", wraps=app.ExponentialFileSource) as files, patch.object(app, "PoissonMessageSource", wraps=app.PoissonMessageSource) as messages:
                await app.run(10, 300, count=0, seed=seed)
                file_rng = files.call_args.kwargs["rng"]
                message_rng = messages.call_args.kwargs["rng"]
                return ([file_rng.random() for _ in range(3)], [message_rng.random() for _ in range(3)])
        first = await samples(42)
        repeated = await samples(42)
        changed = await samples(99)
        self.assertEqual(first, repeated)
        self.assertNotEqual(first[0], first[1])
        self.assertNotEqual(first[0], changed[0])
        self.assertNotEqual(first[1], changed[1])
        print("  Actual: same seed reproduces results; file/message streams differ; new seed changes both", flush=True)

    async def test_processor_uses_configured_delay_and_returns_count(self):
        batch = MessageBatch((Message(1, "a"), Message(2, "b")), 0, 300)
        for delay in (0, 0.25):
            with self.subTest(delay=delay), patch("message_service.time.sleep") as sleep:
                self.assertEqual(process_batch(batch, delay_seconds=delay), 2)
                sleep.assert_called_once_with(delay)
        print("  Actual: configured delays 0 and 0.25 passed to sleep; both messages counted", flush=True)

    async def test_app_passes_delay_to_worker(self):
        async def messages():
            yield Message(1, "a")
        source = Mock()
        source.messages.side_effect = messages
        with patch.object(app, "PoissonMessageSource", return_value=source), patch.object(app, "process_batch", return_value=1) as processor:
            await app.run(10, 300, message_delay=0.5)
        processor.assert_called_once()
        self.assertEqual(processor.call_args.kwargs, {"delay_seconds": 0.5})
        self.assertEqual(processor.call_args.args[0].messages, (Message(1, "a"),))
        print("  Actual: application forwards message_delay=0.5 to the submitted worker", flush=True)

    async def test_invalid_delay_is_rejected_before_sources_start(self):
        batch = MessageBatch((Message(1, "a"),), 0, 300)
        for delay in (-1, float("nan"), float("inf")):
            with self.subTest(delay=delay), patch.object(app, "PoissonMessageSource") as source:
                with self.assertRaises(ValueError):
                    await app.run(10, 300, count=0, message_delay=delay)
                source.assert_not_called()
                with self.assertRaises(ValueError):
                    process_batch(batch, delay_seconds=delay)
        print("  Actual: negative, NaN and infinite delays rejected before work starts", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
