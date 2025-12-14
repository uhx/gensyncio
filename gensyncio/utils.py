import time
from typing import Any, Generator, Tuple, TypeVar

from gensyncio.loop import Loop
from gensyncio.task import Task
from .globs import get_running_loop

_G = TypeVar("_G")
_R = TypeVar("_R")


def sleep(delay: float) -> Generator[None, None, None]:
    start = time.time()
    while time.time() - start < delay:
        yield


def gather(
    *futures: Generator[Any, Any, Any] | Task[Any, Any],
    timeout: float | None = None,
) -> Generator[None, None, Tuple[Any, ...]]:
    loop = get_running_loop()
    start_time = time.time()
    tasks = [loop.create_task(future) for future in futures]
    results = [None] * len(tasks)
    while tasks:
        for i, task in enumerate(tasks[:]):
            if task.done():
                results[i] = task.result
                tasks.remove(task)
        if timeout and time.time() - start_time > timeout:
            raise TimeoutError(f"Timeout on tasks: {ordered_ids}")
        yield
    return tuple(results)


def run(coro: Generator[Any, Any, Any] | Task[Any, Any]) -> Any:
    try:
        loop = get_running_loop()
    except RuntimeError:
        loop = Loop()
    return loop.run_until_complete(coro)


def create_task(coro: Generator[_G, Any, _R] | Task[_G, _R]) -> Task[_G, _R]:
    try:
        loop = get_running_loop()
    except RuntimeError:
        loop = Loop()
    return loop.create_task(coro)
