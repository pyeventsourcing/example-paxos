from concurrent.futures import Future, TimeoutError
from time import time
from typing import Any, Optional

from replicatedstatemachine.exceptions import CommandTimeoutError


class CommandFuture(Future[Any]):
    def __init__(self, cmd_text: str):
        super(CommandFuture, self).__init__()
        self.original_cmd_text = cmd_text
        self.started: float = time()
        self.finished: Optional[float] = None
        self.reproposal_count = 0

    def result(self, timeout: Optional[float] = None) -> Any:
        try:
            return super().result(timeout)
        except TimeoutError as e:
            raise CommandTimeoutError() from e

    def set_exception(self, exception: Optional[BaseException]) -> None:
        self.finished = time()
        super().set_exception(exception)

    def set_result(self, result: Any) -> None:
        self.finished = time()
        super().set_result(result)

    @property
    def duration(self) -> float:
        return self.finished - self.started