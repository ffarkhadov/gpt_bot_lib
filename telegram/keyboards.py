from __future__ import annotations

from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)


# ─────────────────────────────────────────────────────────────
# Главное меню
# ─────────────────────────────────────────────────────────────
def kb_main(stores: list[tuple[str, str, str]]) -> InlineKeyboardMarkup:
    """
    :param stores: [(store_id, name, marketplace)]
    """
    rows: list[list[InlineKeyboardButton]] = []

    # 1. Магазины пользователя (по одному в строке)
    for store_id, name, mp in stores:
        rows.append([
            InlineKeyboardButton(
                text=f"{name} · {mp.upper()}",
                callback_data=f"store_{store_id}",
            )
        ])

    # 2. Кнопки добавления
    rows.append([InlineKeyboardButton(
        text="➕ Добавить Ozon магазин",
        callback_data="add_ozon",
    )])
    rows.append([InlineKeyboardButton(
        text="➕ Добавить Wildberries магазин",
        callback_data="add_wb",
    )])

    return InlineKeyboardMarkup(inline_keyboard=rows)


# ─────────────────────────────────────────────────────────────
# «➡️ Далее» / любой одиночный шаг
# ─────────────────────────────────────────────────────────────
def kb_step(next_cb: str, text: str = "➡️ Далее") -> InlineKeyboardMarkup:
    """
    Однокнопочная клавиатура.

    :param next_cb: callback_data, которое бот получит по нажатию
    :param text: надпись на кнопке (по умолчанию «Далее» с стрелкой)
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=text, callback_data=next_cb)]
        ]
    )
