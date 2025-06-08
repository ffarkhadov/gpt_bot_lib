import asyncio
from typing import Any, NamedTuple, Callable

_report_queue: asyncio.Queue["Task"] | None = None


class Task(NamedTuple):
    coro: Callable[..., Any]
    args: tuple
    kwargs: dict


def get_queue() -> asyncio.Queue:
    global _report_queue
    if _report_queue is None:
        _report_queue = asyncio.Queue()
    return _report_queue


async def enqueue(coro: Callable[..., Any], *args, **kwargs) -> None:
    await get_queue().put(Task(coro, args, kwargs))


async def start_workers(n: int):
    async def worker(worker_id: int):
        q = get_queue()
        while True:
            task: Task = await q.get()
            try:
                await task.coro(*task.args, **task.kwargs)
            except Exception as e:
                print(f"[WORKER {worker_id}] Error:", e)
            finally:
                q.task_done()

    await asyncio.gather(*(worker(i + 1) for i in range(n)))
