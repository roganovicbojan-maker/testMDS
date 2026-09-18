"""Async coordination of message reception and batch deadlines."""

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import suppress

from message_batching import Message, MessageBatch, TimeBatcher


class MessageBatchService:
    def __init__(
        self,
        batcher: TimeBatcher,
        submit_batch: Callable[[MessageBatch], None],
    ) -> None:
        self.batcher = batcher
        self.submit_batch = submit_batch

    def _submit(self, batch: MessageBatch | None) -> None:
        if batch is not None:
            logging.info("SUBMIT batch: messages %s", [m.message_id for m in batch.messages])
            self.submit_batch(batch)

    async def run(self, source: AsyncIterator[Message]) -> None:
        loop = asyncio.get_running_loop()
        pending = None
        primary_error = None
        try:
            while True:
                if pending is None:
                    pending = asyncio.create_task(anext(source))
                deadline = self.batcher.deadline
                timeout = None if deadline is None else max(0, deadline - loop.time())
                done, _ = await asyncio.wait({pending}, timeout=timeout)

                if pending in done:
                    try:
                        message = pending.result()
                    except StopAsyncIteration:
                        break
                    pending = None
                    # Reception time is the time this coordinator accepts the message.
                    self._submit(self.batcher.add(message, loop.time()))
                    logging.info("RECEIVED message %s", message.message_id)
                else:
                    self._submit(self.batcher.close_if_due(loop.time()))
                    # Keep the same pending read: a deadline must not cancel the source.
        except BaseException as error:
            primary_error = error
            raise
        finally:
            # run owns this iterator: stop its active read before closing it.
            if pending is not None:
                pending.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await pending
            cleanup_errors = []
            close = getattr(source, "aclose", None)
            if close is not None:
                try:
                    await close()
                except Exception as error:
                    cleanup_errors.append(error)
            # Closing failure must not prevent submission of accepted messages.
            try:
                self._submit(self.batcher.flush())
            except Exception as error:
                cleanup_errors.append(error)
            for error in cleanup_errors:
                logging.error("Message cleanup failed: %s", error)
            if cleanup_errors and primary_error is None:
                raise cleanup_errors[0]
            # Otherwise preserve the original source/submission error or cancellation.


def process_batch(batch: MessageBatch) -> int:
    ids = [message.message_id for message in batch.messages]
    logging.info("START processing %s", ids)
    time.sleep(3)
    logging.info("DONE processing %s", ids)
    return len(batch.messages)
