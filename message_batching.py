"""Message batching rules, independent of timers, sources and worker threads."""

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class Message:
    message_id: int
    payload: str


@dataclass(frozen=True)
class MessageBatch:
    messages: tuple[Message, ...]
    opened_at: float
    deadline: float


class TimeBatcher:
    """One owner supplies nondecreasing monotonic reception/check times."""

    def __init__(self, window_seconds: float = 300) -> None:
        if not math.isfinite(window_seconds) or window_seconds <= 0:
            raise ValueError("Window must be positive and finite")
        self.window_seconds = window_seconds
        self._messages: list[Message] = []
        self._opened_at: float | None = None
        self._deadline: float | None = None
        self._last_time: float | None = None

    @property
    def deadline(self) -> float | None:
        return self._deadline

    def _check_time(self, now: float) -> None:
        if not math.isfinite(now):
            raise ValueError("Time must be finite")
        if self._last_time is not None and now < self._last_time:
            raise ValueError("Time cannot move backwards")
        self._last_time = now

    def add(self, message: Message, now: float) -> MessageBatch | None:
        """Return a previous expired batch, then accept the new message."""
        expired = self.close_if_due(now)
        if self._deadline is None:
            self._opened_at = now
            self._deadline = now + self.window_seconds
        self._messages.append(message)
        return expired

    def close_if_due(self, now: float) -> MessageBatch | None:
        """Called by the coordinator at the deadline, even without arrivals."""
        self._check_time(now)
        if self._deadline is not None and now >= self._deadline:
            return self.flush()
        return None

    def flush(self) -> MessageBatch | None:
        """Close any remaining batch, including an early shutdown batch."""
        if not self._messages:
            return None
        assert self._opened_at is not None and self._deadline is not None
        batch = MessageBatch(tuple(self._messages), self._opened_at, self._deadline)
        self._messages = []
        self._opened_at = None
        self._deadline = None
        return batch
