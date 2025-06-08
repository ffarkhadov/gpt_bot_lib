import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config import settings
from telegram.handlers import router as handlers_router
from core.tasks.queue import start_workers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

async def main() -> None:
    bot = Bot(settings.BOT_TOKEN, parse_mode="HTML")
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(handlers_router)

    # запускаем фоновые workers
    worker_pool = asyncio.create_task(start_workers(settings.WORKERS))

    await dp.start_polling(bot)
    await worker_pool  # не даём python завершиться

if __name__ == "__main__":
    asyncio.run(main())
