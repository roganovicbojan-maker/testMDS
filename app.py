"""Run scheduled file collections alongside a continuous or finite stream using one shared pool."""

import argparse
import asyncio
import logging
import random
import signal
from contextlib import suppress
from concurrent.futures import ThreadPoolExecutor

from file_pipeline import ExponentialFileSource, FileBatchService, NextFitBucketing, SimulatedFileProcessor
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
) -> None:
    stop = stop if stop is not None else asyncio.Event()
    schedule = FileSchedule(nightly_hour, nightly_minute, file_interval)
    file_service = FileBatchService(
        ExponentialFileSource(rng=random.Random(42)), NextFitBucketing()
    )
    file_processor = SimulatedFileProcessor(delay_seconds=1)
    message_source = PoissonMessageSource(
        rate_per_minute=rate, max_messages=count, rng=random.Random(42)
    )
    batcher = TimeBatcher(window)
    logging.info("APP: one pool, 10 workers; rate=%s/min, window=%ss, messages=%s", rate, window, count)
    with ThreadPoolExecutor(max_workers=10, thread_name_prefix="shared-worker") as pool:
        dispatcher = WorkDispatcher(pool)

        def submit_messages(batch):
            dispatcher.submit(
                f"message batch starting with {batch.messages[0].message_id}", process_batch, batch
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
                # A failed file source must also stop the continuous message source.
                stop.set()

        producer_results = await asyncio.gather(
            receive_messages(), schedule_files(), return_exceptions=True,
        )
        producer_errors = [result for result in producer_results if isinstance(result, BaseException)]
        for error in producer_errors:
            logging.error("Producer failed: %s", error)
        # All producers have stopped; inspect all accepted worker results.
        await dispatcher.drain()
        if producer_errors:
            raise RuntimeError("A data source failed")
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
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(run_cli(args))


