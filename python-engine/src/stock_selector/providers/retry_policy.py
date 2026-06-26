from collections.abc import Callable
from dataclasses import dataclass
from time import sleep
from typing import TypeVar


T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 2.0

    def run(self, operation: Callable[[], T]) -> T:
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                return operation()
            except Exception as exc:
                last_error = exc
                if attempt < self.max_attempts:
                    sleep(self.backoff_seconds)
        assert last_error is not None
        raise last_error

