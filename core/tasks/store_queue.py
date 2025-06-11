"""
Очередь задач для каждого магазина.

• автоматический бесконечный цикл unit_day_5 → p_campain_fin_1 → sleep(30 мин)
• ручные задания («Обновить отчёт») просто добавляются в очередь
• кнопка ⏹ ставит флаг cancel: текущий шаг заканчивает работу,
  очередь очищается, автоматический цикл перезапускается
"""
from __future__ import annotations
import asyncio, logging, time
from typing import Callable, Any, Dict, Awaitable

from core.tasks.queue import enqueue
from core.tasks.report_runner import run_report

log = logging.getLogger(__name__)

class StoreWorker:
    def __init__(self, store_id: str, base_cfg: dict):
        self.store_id = store_id
        self.base_cfg = base_cfg
        self.queue: asyncio.Queue[dict] = asyncio.Queue()
        self.cancel = asyncio.Event()
        self._started = False

    async def _worker(self):
        while True:
            cfg = await self.queue.get()
            if self.cancel.is_set():
                self.queue.task_done()
                continue
            await run_report(cfg)
            self.queue.task_done()

    async def _autoloop(self):
        while True:
            await self.enqueue_chain(manual=False)
            await asyncio.sleep(1800)   # 30 мин

    async def enqueue_chain(self, manual: bool = True):
        step_event = asyncio.Event()
        chain = [
            ("unit_day_5",  "unit-day ≈2 мин"),
            ("p_campain_fin_1", "ads до 1 часа"),
        ]
        for idx, (script, human) in enumerate(chain, start=1):
            await self.queue.put({
                **self.base_cfg,
                "script": script,
                "human": human,
                "step": f"{idx}/{len(chain)}",
                "bot":   self.base_cfg["bot"],
                "step_event": step_event,     # сигнал окончанию шага
            })
        if manual:
            await self.base_cfg["bot"].send_message(
                self.base_cfg["chat_id"],
                "ℹ️ Отчёт добавлен в очередь."
            )

    async def start(self):
        if not self._started:
            asyncio.create_task(self._worker())
            asyncio.create_task(self._autoloop())
            self._started = True


_workers: Dict[str, StoreWorker] = {}

async def get_worker(store_id: str, base_cfg: dict) -> StoreWorker:
    if store_id not in _workers:
        _workers[store_id] = StoreWorker(store_id, base_cfg)
        await _workers[store_id].start()
    return _workers[store_id]
