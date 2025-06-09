from aiogram import Router, F
from aiogram.types import Message

from telegram.keyboards import kb_main
from core.services.gs_db import GsDB

router = Router(name="start")


@router.message(F.text.in_({"/start", "🏠 Меню"}))
async def cmd_start(msg: Message):
    db = GsDB()
    await db.ensure_user(msg.from_user.id, msg.from_user.username,
                         msg.from_user.full_name)
    stores = await db.get_stores_by_owner(msg.from_user.id)
    # [(id, name, mp), …]
    stores_tuples = [(s["store_id"], s["name"], s["marketplace"])
                     for s in stores]

    await msg.answer(
        "Привет! Я бот-отчётник для Ozon/WB.\nВыберите действие:",
        reply_markup=kb_main(stores_tuples),
    )
