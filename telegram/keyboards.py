from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder


# ─────────────────────────────────────────────────────────────
# Главное меню: список магазинов (если есть) + кнопки «Добавить …»
# ─────────────────────────────────────────────────────────────
def kb_main(stores: list[tuple[str, str, str]]) -> InlineKeyboardMarkup:
    """
    :param stores: [(store_id, name, marketplace), …]
    """
    kb = InlineKeyboardBuilder()

    # 1. Магазины пользователя
    for store_id, name, mp in stores:
        kb.button(
            text=f"{name} · {mp.upper()}",
            callback_data=f"store_{store_id}",
        )

    # 2. Кнопки добавления
    kb.button(text="➕ Добавить Ozon магазин", callback_data="add_ozon")
    kb.button(text="➕ Добавить Wildberries магазин", callback_data="add_wb")

    # ➜ Обязательно формируем ряды; 1 кнопка на строку
    kb.adjust(1)
    return kb.as_markup()


# ─────────────────────────────────────────────────────────────
# Универсальная «➡️ Далее» (или любая другая одинарная кнопка)
# ─────────────────────────────────────────────────────────────
def kb_step(next_cb: str, text: str = "➡️ Далее") -> InlineKeyboardMarkup:
    """
    :param next_cb: callback_data, которое бот получит по нажатию
    :param text: надпись на кнопке (по умолчанию «Далее» со стрелкой)
    """
    kb = InlineKeyboardBuilder()
    kb.button(text=text, callback_data=next_cb)
    kb.adjust(1)
    return kb.as_markup()
