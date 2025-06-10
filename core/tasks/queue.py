import asyncio, logging
from typing import NamedTuple, Callable, Any

log = logging.getLogger(__name__)

class Task(NamedTuple):
    coro: Callable[..., Any]
    args: tuple
    kwargs: dict

_queue: asyncio.Queue[Task] | None = None


def get_queue() -> asyncio.Queue:
    global _queue
    if _queue is None:
        _queue = asyncio.Queue()
    return _queue


async def enqueue(coro: Callable[..., Any], *args, **kwargs):
    await get_queue().put(Task(coro, args, kwargs))
    log.info("ENQUEUE %s", coro.__name__)


async def start_workers(n: int):
    async def worker(idx: int):
        log.info("WORKER %s started", idx)
        q = get_queue()
        while True:
            t = await q.get()
            try:
                log.info("WORKER %s → %s", idx, t.coro.__name__)
                await t.coro(*t.args, **t.kwargs)
            except Exception as e:
                log.exception("WORKER %s error: %s", idx, e)
            finally:
                q.task_done()

    for i in range(1, n + 1):
        asyncio.create_task(worker(i))
    log.info("queue: started %s workers", n)
