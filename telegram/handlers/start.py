from aiogram import Router, F
from aiogram.types import Message, CallbackQuery

from telegram.keyboards import kb_main, kb_stores
from core.tasks.queue import enqueue
from core.tasks.report_runner import run_report

router = Router(name="start")


@router.message(F.text.in_({"/start", "🏠 Меню"}))
async def cmd_start(msg: Message):
    await msg.answer(
        "Привет! Я бот-отчётник для Ozon/WB.\nВыберите действие:",
        reply_markup=kb_main(),
    )


@router.callback_query(F.data.startswith("store_"))
async def on_store_click(cb: CallbackQuery):
    store_id = cb.data.removeprefix("store_")
    await cb.answer(f"Запускаю отчёт для {store_id}...")
    # Заглушка: в реальном коде достаём store_cfg из БД/GS
    store_cfg = {"sheet_id": "dummy", "marketplace": "ozon", "creds_json": "{}"}
    await enqueue(run_report, store_cfg)
    await cb.message.answer("🟢 Задача поставлена в очередь.")
