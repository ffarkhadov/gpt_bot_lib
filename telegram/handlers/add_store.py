from json import dumps

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from telegram.states import AddStore
from telegram.keyboards import kb_step
from core.tasks.queue import enqueue
from core.tasks.report_runner import run_report

router = Router(name="add_store")


@router.callback_query(F.data.in_({"add_ozon", "add_wb"}))
async def add_store_intro(cb: CallbackQuery, state: FSMContext):
    mp = "ozon" if cb.data == "add_ozon" else "wb"
    await state.update_data(mp=mp)
    await cb.message.answer(
        "<b>Подключение магазина</b>\n"
        "Всего 3 шага: ключи → таблица → доступ.\nНажмите «Далее».",
        reply_markup=kb_step("step_start"),
    )
    await state.set_state(AddStore.client_id if mp == "ozon" else AddStore.api_key)
    await cb.answer()


@router.callback_query(F.data == "step_start")
async def ask_first_key(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if data["mp"] == "ozon":
        await cb.message.answer("Введите <b>Client-ID</b> Ozon:")
    else:
        await cb.message.answer("Введите <b>API-Key</b> Wildberries:")
    await cb.answer()


@router.message(AddStore.client_id)
async def save_client_id(msg: Message, state: FSMContext):
    await state.update_data(client_id=msg.text.strip())
    await msg.answer("Теперь введите <b>API-Key</b> Ozon:")
    await state.set_state(AddStore.api_key)


@router.message(AddStore.api_key)
async def save_api_key(msg: Message, state: FSMContext):
    await state.update_data(api_key=msg.text.strip())
    await msg.answer("Укажите <b>ID Google-таблицы</b> (строка в URL после /d/):")
    await state.set_state(AddStore.sheet_id)


@router.message(AddStore.sheet_id)
async def save_sheet_id(msg: Message, state: FSMContext):
    await state.update_data(sheet_id=msg.text.strip())
    data = await state.get_data()
    sa_email = "sa_placeholder@example.iam.gserviceaccount.com"  # позже real pick
    await state.update_data(sa_email=sa_email)

    await msg.answer(
        f"Добавьте сервис-аккаунт <code>{sa_email}</code> "
        "в доступ к таблице → <b>Редактор</b> и нажмите «Готово».",
    )
    await state.set_state(AddStore.confirm_access)


@router.message(AddStore.confirm_access, F.text.lower().in_("готово", "done"))
async def finish_add(msg: Message, state: FSMContext):
    cfg = await state.get_data()
    # сериализуем ключи
    store_cfg = {
        "sheet_id": cfg["sheet_id"],
        "marketplace": cfg["mp"],
        "creds_json": dumps(
            {"client_id": cfg.get("client_id"), "api_key": cfg["api_key"]}
        ),
    }
    # ставим первую задачу
    await enqueue(run_report, store_cfg)
    await msg.answer("✅ Магазин подключён. Первый отчёт ушёл в очередь!")
    await state.clear()
