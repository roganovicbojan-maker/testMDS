# Concurrent file and message processing

Python 3.10+ implementation using only the standard library.
Two independent producers submit processing jobs to one pool of 10 threads.
Sources and processing are simulated; no external services or real CSV files are required.

## Quick start

From the project directory:

```sh
python -m unittest discover -s tests -t . -v
python app.py --rate 120 --window 2 --count 10 --files-now --file-interval 2
```

The second command is a finite, accelerated demonstration. It prints message
arrivals, collection IDs, queued jobs, worker starts/finishes and a final summary.
Timing and batch counts may vary with scheduling; a seeded source makes generated
sizes and requested delays reproducible, not the entire concurrent execution order.

## Continuous operation

```sh
python app.py
```

Defaults: 10 messages/minute on average, a 300-second message window, 100 files
per nightly collection, collection at 02:00 UTC, 10 shared processing threads.
Press Ctrl+C in the terminal to stop receiving, flush accepted messages and wait
for submitted jobs. Without `--files-now`, files are first collected at the next
scheduled nightly time. `--count N` stops after N messages and ends file scheduling.

Useful options:

| Option | Meaning |
| --- | --- |
| `--rate 10` | Average message arrivals per minute |
| `--window 300` | Message window in seconds |
| `--count 60` | Finite run; omit for continuous operation |
| `--bucketing ffd` | Choose next-fit (default) or First Fit Decreasing |
| `--seed 42` | Reproducible sources: file seed 42, message seed 43 |
| `--message-delay 3` | Simulated message processing seconds; use 0 for no delay |
| `--files-now` | Also collect files immediately on startup |
| `--file-interval 10` | Demo only: repeat collection after this many seconds |
| `--nightly-hour 2 --nightly-minute 0` | Daily UTC collection time |

## Docker

Use Docker with Linux containers:

```sh
docker build -t testmds .
docker run --rm --entrypoint python testmds -m unittest discover -s tests -t . -v
docker run --rm testmds --rate 120 --window 2 --count 10 --files-now --file-interval 2
```

Continuous background run and graceful stop:

```sh
docker run -d --name testmds-run testmds
docker stop --time 60 testmds-run
docker logs testmds-run
docker rm testmds-run
```

The image runs tests during build and runs the application as a non-root user.
SIGTERM requests graceful shutdown. The stop timeout must allow queued work to
finish; Docker forcibly stops the process after its timeout. The initial build
needs network access to obtain the Python base image. Runtime needs no network.

## Design

```text
PoissonMessageSource -> MessageBatchService / TimeBatcher --+
                                                          +-> WorkDispatcher -> 10 threads
FileScheduler -> FileBatchService -> BucketingStrategy ----+
```

- `file_pipeline.py`: file metadata, source/strategy contracts, exponential source,
  Next Fit / First Fit Decreasing grouping and simulated file processor.
- `message_batching.py`: message models and deterministic window rules.
- `message_source.py`: exponential inter-arrival delays for Poisson arrivals.
- `message_service.py`: asynchronous reception/deadline coordination and simulated message processing.
- `file_scheduler.py`: daily UTC schedule and interruptible waiting.
- `work_dispatcher.py`: shared job submission, error observation and draining.
- `app.py`: dependency construction, lifecycle and signal handling.
- `tests/`: unit, mock and concurrency tests with explanatory output.



Dependencies are supplied through constructors or callbacks. A replacement
bucketing algorithm only needs to implement the `BucketingStrategy.pack` contract;
the collection service and dispatcher do not need to change. Both Next Fit and
First Fit Decreasing are implemented and selectable through `--bucketing`.
Next Fit scans the input once. FFD sorts a copy by decreasing size and searches
existing buckets for the first fit; this simple implementation has O(n^2) worst-case
packing time. For [6, 6, 4, 4] MB, Next Fit uses three buckets and FFD uses two.
Neither is claimed to be an optimal solver. Both use the same oversized-file policy. Protocols describe
interfaces for static checking; tests verify behavior.

## Assumptions and edge cases

- The first received message opens a window; later arrivals do not extend it.
  Windows are half-open: a message accepted exactly at the deadline opens the next
  window. Reception time is the coordinator's monotonic time, not source event time.
- An idle source does not prevent deadline closure. No empty batches are produced.
  EOF, source error and graceful cancellation flush the accepted partial batch.
  The message service owns the supplied iterator for the duration of a run: it
  stops its pending read and awaits `aclose()` when that method is available.
  Cleanup failures are logged without replacing an existing source/submission error.
  Direct cancellation of `app.run()` requests the same orderly stop, waits for
  producers and workers, then propagates cancellation. Repeated cancellation
  requests do not abandon the drain; this is not a forced-stop mechanism.
- 1 MB = 1,000,000 bytes. Files remain whole. A file exceeding 10 MB gets its own
  oversized bucket; this is an explicit exception to the ordinary bucket limit.
  The final partial bucket is also submitted.
- The exponential file-size mean is configurable in `ExponentialFileSource` and
  defaults to 3 MB, since the task does not specify it. Sizes are truncated to bytes.
- The application gives the file source `seed` and the message source `seed + 1`,
  so they do not replay the same random stream. The default seed is 42; changing
  `--seed` changes both streams while keeping runs reproducible.
- Message processing delay is configurable with `--message-delay` (default 3 s).
  It changes simulated worker duration, not the batching window or arrival rate.
- Both producers share the same pool. File processing and message processing
  simulate I/O with a delay; the task does not specify a business transformation.
- Nightly scheduling uses UTC, does not replay missed executions, and runs inside
  the application process. The optional short interval starts after submission of
  the previous collection, without waiting for its workers.

## Limitations

This is an in-memory model, not a durable ingestion platform. No retries,
checkpointing, exactly-once delivery, broker, or recovery after process failure
are implemented. The pending job queue has no bound; sustained overload can grow
memory. Completed jobs are removed promptly. No fairness guarantee is provided
between file and message jobs. Source collection is a short in-memory operation;
a blocking external file source would need an appropriate I/O adapter.
Worker failures are logged immediately; other jobs continue, and shutdown reports
failure with a nonzero exit code. Source failures stop the application producers.

## Optional tournament bonus

`tournament.py` implements a fixed-round social tournament with full tables,
balanced resting when necessary, points-based standings and an explicit tie-break.
Small instances use exhaustive optimization; larger ones use a bounded heuristic
and report whether optimality was proved. See [BONUS.md](BONUS.md) for assumptions,
the exact objective, limitations and examples.

```sh
python tournament.py --players 8 --tables 4 --group-size 2 --rounds 3 --simulate
```
