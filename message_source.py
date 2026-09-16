"""Poisson arrivals simulated using exponential inter-arrival delays."""

import asyncio
import logging
import math
import random
from collections.abc import AsyncIterator, Awaitable, Callable

from message_batching import Message


class PoissonMessageSource:
    def __init__(
        self,
        rate_per_minute: float = 10,
        max_messages: int | None = None,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if not math.isfinite(rate_per_minute) or rate_per_minute <= 0:
            raise ValueError("Message rate must be positive and finite")
        if max_messages is not None and max_messages < 0:
            raise ValueError("Message limit cannot be negative")
        self.rate_per_minute = rate_per_minute
        self.max_messages = max_messages
        self.rng = rng if rng is not None else random.Random()
        self.sleep = sleep

    async def messages(self) -> AsyncIterator[Message]:
        message_id = 0
        while self.max_messages is None or message_id < self.max_messages:
            delay_seconds = self.rng.expovariate(self.rate_per_minute / 60)
            logging.info("SOURCE next message in %.2f seconds", delay_seconds)
            await self.sleep(delay_seconds)
            message_id += 1
            yield Message(message_id, f"payload-{message_id}")
