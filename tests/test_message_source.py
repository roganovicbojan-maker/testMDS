import asyncio
import random
import unittest
from unittest.mock import AsyncMock, Mock

from message_source import PoissonMessageSource


class PoissonSourceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    async def test_delays_use_rate_per_second_and_emit_messages(self):
        rng = Mock(spec=random.Random)
        rng.expovariate.side_effect = [2.5, 8.0]
        sleep = AsyncMock()
        source = PoissonMessageSource(max_messages=2, rng=rng, sleep=sleep)
        messages = [message async for message in source.messages()]
        print("  Mock delays: 2.5s, 8s; expected IDs: [1, 2]", flush=True)
        print(f"  Actual IDs: {[m.message_id for m in messages]}; waits: {sleep.await_args_list}", flush=True)
        self.assertEqual([m.message_id for m in messages], [1, 2])
        self.assertEqual([call.args[0] for call in sleep.await_args_list], [2.5, 8.0])
        self.assertEqual(rng.expovariate.call_count, 2)
        for call in rng.expovariate.call_args_list:
            self.assertEqual(call.args, (10 / 60,))

    async def test_zero_count_does_not_wait_or_sample(self):
        rng, sleep = Mock(spec=random.Random), AsyncMock()
        source = PoissonMessageSource(max_messages=0, rng=rng, sleep=sleep)
        self.assertEqual([m async for m in source.messages()], [])
        rng.expovariate.assert_not_called()
        sleep.assert_not_awaited()
        print("  Expected and actual: 0 messages, no sampling and no waiting", flush=True)

    async def test_same_seed_reproduces_delays(self):
        delays = []
        for _ in range(2):
            sleep = AsyncMock()
            source = PoissonMessageSource(max_messages=3, rng=random.Random(42), sleep=sleep)
            _messages = [m async for m in source.messages()]
            delays.append([call.args[0] for call in sleep.await_args_list])
        print(f"  Two fresh seeded sources: delays {delays}", flush=True)
        self.assertEqual(delays[0], delays[1])

    async def test_cancellation_interrupts_wait_without_emitting(self):
        waiting = asyncio.Event()

        async def sleep(_delay):
            waiting.set()
            await asyncio.Event().wait()

        source = PoissonMessageSource(sleep=sleep).messages()
        pending = asyncio.create_task(anext(source))
        try:
            await asyncio.wait_for(waiting.wait(), timeout=2)
        finally:
            pending.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await pending
            await source.aclose()
        print("  Actual: cancellation interrupts source wait; no message emitted", flush=True)

    async def test_invalid_configuration_is_rejected(self):
        for rate in (0, -1, float("nan"), float("inf")):
            with self.subTest(rate=rate):
                with self.assertRaises(ValueError):
                    PoissonMessageSource(rate_per_minute=rate)
        with self.assertRaises(ValueError):
            PoissonMessageSource(max_messages=-1)
        print("  Expected and actual: invalid rates and negative count rejected", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
