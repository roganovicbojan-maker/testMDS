"""A shared pool that releases completed futures during continuous operation."""

import asyncio
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any


class WorkDispatcher:
    """All methods and completion callbacks run on the coordinator event loop."""

    def __init__(self, pool: ThreadPoolExecutor) -> None:
        self.pool = pool
        self._pending: set[asyncio.Future] = set()
        self.completed = 0
        self.failures = 0

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def submit(self, label: str, operation: Callable[..., Any], *args: Any) -> None:
        future = asyncio.wrap_future(self.pool.submit(operation, *args))
        self._pending.add(future)
        future.add_done_callback(partial(self._finished, label))
        logging.info("QUEUED %s", label)

    def _finished(self, label: str, future: asyncio.Future) -> None:
        self._pending.discard(future)
        self.completed += 1
        try:
            future.result()
        except BaseException as error:
            self.failures += 1
            logging.error("FAILED %s: %s", label, error)

    async def drain(self) -> int:
        """After producers stop, wait for pending jobs and report lifetime totals."""
        if self._pending:
            await asyncio.gather(*tuple(self._pending), return_exceptions=True)
        logging.info("DRAIN complete: %s jobs, %s failures", self.completed, self.failures)
        if self.failures:
            raise RuntimeError(f"{self.failures} processing jobs failed")
        return self.completed
