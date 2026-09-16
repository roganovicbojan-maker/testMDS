from dataclasses import dataclass
from typing import Protocol
import math
import random
import logging
import time



BYTES_PER_MB = 1_000_000


@dataclass(frozen=True)
class FileItem:
    """File metadata used for grouping; the file contents are not loaded."""

    name: str
    size_bytes: int

    def __post_init__(self) -> None:
        if self.size_bytes < 0:
            raise ValueError("File size cannot be negative")


class FileSource(Protocol):
    """Provide metadata for one collection of files."""

    def collect(self) -> list[FileItem]:
        ...


class ExponentialFileSource:
    """Simulate file metadata without creating physical files."""

    def __init__(
        self,
        count: int = 100,
        mean_size_bytes: float = 3 * BYTES_PER_MB,
        rng: random.Random | None = None,
    ) -> None:
        if count < 0:
            raise ValueError("File count cannot be negative")
        if not math.isfinite(mean_size_bytes) or mean_size_bytes <= 0:
            raise ValueError("Mean file size must be positive and finite")
        self.count = count
        self.mean_size_bytes = mean_size_bytes
        self.rng = rng if rng is not None else random.Random()

    def collect(self) -> list[FileItem]:
        return [
            FileItem(
                name=f"file_{index:03d}.csv",
                size_bytes=int(self.rng.expovariate(1 / self.mean_size_bytes)),
            )
            for index in range(1, self.count + 1)
        ]


class BucketingStrategy(Protocol):
    """Contract for algorithms that group files into buckets."""

    def pack(self, files: list[FileItem]) -> list[list[FileItem]]:
        ...


class NextFitBucketing:
    """Pack files in input order, keeping only one bucket open at a time."""

    def __init__(self, max_size_bytes: int = 10 * BYTES_PER_MB) -> None:
        if max_size_bytes <= 0:
            raise ValueError("Bucket size limit must be positive")
        self.max_size_bytes = max_size_bytes

    def pack(self, files: list[FileItem]) -> list[list[FileItem]]:
        buckets: list[list[FileItem]] = []
        current_bucket: list[FileItem] = []
        current_size = 0

        for file in files:
            if current_bucket and current_size + file.size_bytes > self.max_size_bytes:
                buckets.append(current_bucket)
                current_bucket = []
                current_size = 0

            # An oversized file is kept whole in a separate bucket.
            if file.size_bytes > self.max_size_bytes:
                buckets.append([file])
                continue

            current_bucket.append(file)
            current_size += file.size_bytes

        if current_bucket:
            buckets.append(current_bucket)

        return buckets


class FileBatchService:
    """Collect and group one delivery of files using supplied dependencies."""

    def __init__(self, source: FileSource, strategy: BucketingStrategy) -> None:
        self.source = source
        self.strategy = strategy

    def prepare_buckets(self) -> list[list[FileItem]]:
        files = self.source.collect()
        return self.strategy.pack(files)


class FileProcessor(Protocol):
    def process(self, bucket_id: int, files: tuple[FileItem, ...]) -> int:
        """Process a bucket and return the number of processed bytes."""
        ...


class SimulatedFileProcessor:
    def __init__(self, delay_seconds: float = 0.2) -> None:
        if not math.isfinite(delay_seconds) or delay_seconds < 0:
            raise ValueError("Processing delay must be nonnegative and finite")
        self.delay_seconds = delay_seconds

    def process(self, bucket_id: int, files: tuple[FileItem, ...]) -> int:
        total_bytes = sum(file.size_bytes for file in files)
        logging.info("START bucket %s: %s files, %.2f MB", bucket_id, len(files), total_bytes / BYTES_PER_MB)
        time.sleep(self.delay_seconds)
        logging.info("DONE  bucket %s: %.2f MB", bucket_id, total_bytes / BYTES_PER_MB)
        return total_bytes
