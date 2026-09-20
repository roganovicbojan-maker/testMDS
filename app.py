"""Run scheduled file collections alongside a continuous or finite stream using one shared pool."""

import argparse
import asyncio
import logging
import math
import random
import signal
from functools import partial
from contextlib import suppress
from concurrent.futures import ThreadPoolExecutor

from file_pipeline import ExponentialFileSource, FileBatchService, NextFitBucketing, FirstFitDecreasingBucketing, SimulatedFileProcessor
from message_batching import TimeBatcher
from message_service import MessageBatchService, process_batch
from message_source import PoissonMessageSource
from work_dispatcher import WorkDispatcher
from file_scheduler import FileSchedule, FileScheduler


async def submit_file_collection(service, processor, dispatcher, collection_id=1):
    # Current simulated collection is small and entirely in memory.
    buckets = service.prepare_buckets()
    logging.info("FILES collection %s: collected and grouped %s buckets", collection_id, len(buckets))
    for number, bucket in enumerate(buckets, start=1):
        dispatcher.submit(f"collection {collection_id}, file bucket {number}", processor.process, number, tuple(bucket))
        await asyncio.sleep(0)


async def run(
    rate: float, window: float, count: int | None = None,
    files_now: bool = False, file_interval: float | None = None,
    nightly_hour: int = 2, nightly_minute: int = 0,
    stop: asyncio.Event | None = None,
    seed: int = 42, message_delay: float = 3,
    bucketing: str = "next-fit",
) -> None:
    if not math.isfinite(message_delay) or message_delay < 0:
        raise ValueError("Message processing delay must be nonnegative and finite")
    stop = stop if stop is not None else asyncio.Event()
    schedule = FileSchedule(nightly_hour, nightly_minute, file_interval)
    if bucketing == "next-fit":
        strategy = NextFitBucketing()
    elif bucketing == "ffd":
        strategy = FirstFitDecreasingBucketing()
    else:
        raise ValueError("Unknown bucketing strategy: " + bucketing)
    file_service = FileBatchService(
        ExponentialFileSource(rng=random.Random(seed)), strategy
    )
    file_processor = SimulatedFileProcessor(delay_seconds=1)
    message_source = PoissonMessageSource(
        rate_per_minute=rate, max_messages=count, rng=random.Random(seed + 1)
    )
    # Separate streams, reproducible from one user-selected seed.
    message_processor = partial(process_batch, delay_seconds=message_delay)
    batcher = TimeBatcher(window)
    logging.info("APP: one pool, 10 workers; rate=%s/min, window=%ss, messages=%s", rate, window, count)
    with ThreadPoolExecutor(max_workers=10, thread_name_prefix="shared-worker") as pool:
        dispatcher = WorkDispatcher(pool)

        def submit_messages(batch):
            dispatcher.submit(
                f"message batch starting with {batch.messages[0].message_id}", message_processor, batch
            )

        message_service = MessageBatchService(batcher, submit_messages)

        collection_id = 0

        async def collect_files():
            nonlocal collection_id
            collection_id += 1
            await submit_file_collection(file_service, file_processor, dispatcher, collection_id)

        async def receive_messages():
            receiver = asyncio.create_task(message_service.run(message_source.messages()))
            stopper = asyncio.create_task(stop.wait())
            try:
                await asyncio.wait({receiver, stopper}, return_when=asyncio.FIRST_COMPLETED)
                if not receiver.done():
                    logging.info("STOP: stopping message reception and flushing partial batch")
                    receiver.cancel()
                with suppress(asyncio.CancelledError):
                    await receiver
            finally:
                stop.set()
                stopper.cancel()
                with suppress(asyncio.CancelledError):
                    await stopper

        async def schedule_files():
            try:
                await FileScheduler(schedule).run(collect_files, stop, run_immediately=files_now)
            finally:
                # Policy: a failed file source stops the other producer too.
                stop.set()

        cancellation_requested = False

        async def finish_step(operation):
            # Direct app cancellation requests graceful shutdown, just like SIGINT.
            # Shield keeps producers/drain alive until their owned resources finish.
            nonlocal cancellation_requested
            task = asyncio.ensure_future(operation)
            while True:
                try:
                    return await asyncio.shield(task)
                except asyncio.CancelledError:
                    if task.cancelled():
                        raise
                    cancellation_requested = True
                    stop.set()

        producer_results = await finish_step(asyncio.gather(
            receive_messages(), schedule_files(), return_exceptions=True,
        ))
        producer_errors = [result for result in producer_results if isinstance(result, BaseException)]
        for error in producer_errors:
            logging.error("Producer failed: %s", error,
                          exc_info=(type(error), error, error.__traceback__))
        # All producers have stopped; inspect all accepted worker results.
        try:
            await finish_step(dispatcher.drain())
        except Exception:
            if not cancellation_requested:
                raise
            logging.exception("Worker failure during cancelled application shutdown")
        if cancellation_requested:
            raise asyncio.CancelledError
        if producer_errors:
            raise RuntimeError("A data source failed") from producer_errors[0]
    logging.info("APP finished successfully")


async def run_cli(args) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop(signum, _frame):
        loop.call_soon_threadsafe(stop.set)

    # signal.signal also works with the Windows asyncio event loop.
    previous = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, request_stop)
        await run(
            args.rate, args.window, args.count, args.files_now, args.file_interval,
            args.nightly_hour, args.nightly_minute, stop=stop,
            seed=args.seed, message_delay=args.message_delay, bucketing=args.bucketing,
        )
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combined file and message processing demo")
    parser.add_argument("--rate", type=float, default=10, help="Messages per minute")
    parser.add_argument("--window", type=float, default=300, help="Batch window in seconds")
    parser.add_argument("--count", type=int, default=None, help="Stop after N messages; omitted means continuous operation")
    parser.add_argument("--files-now", action="store_true", help="Also collect files immediately on startup")
    parser.add_argument("--file-interval", type=float, default=None, help="Demo: repeat file collection every N seconds")
    parser.add_argument("--nightly-hour", type=int, default=2, help="Daily collection hour in UTC (default: 2)")
    parser.add_argument("--nightly-minute", type=int, default=0, help="Daily collection minute (default: 0)")
    parser.add_argument("--seed", type=int, default=42, help="Reproducible seed: files use seed, messages use seed + 1")
    parser.add_argument("--message-delay", type=float, default=3, help="Simulated message processing seconds (default: 3; 0 disables delay)")
    parser.add_argument("--bucketing", choices=("next-fit", "ffd"), default="next-fit", help="File grouping strategy (default: next-fit)")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(run_cli(args))
