from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder


def kb_main() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить Ozon магазин", callback_data="add_ozon")
    kb.button(text="➕ Добавить Wildberries магазин", callback_data="add_wb")
    kb.adjust(1, repeat=True)
    return kb.as_markup()


def kb_stores(stores: list[tuple[str, str]]) -> InlineKeyboardMarkup:
    """
    stores = [(store_id, title), ...]
    """
    kb = InlineKeyboardBuilder()
    for store_id, title in stores:
        kb.button(text=title, callback_data=f"store_{store_id}")
    kb.adjust(1)
    return kb.as_markup()


def kb_step(next_cb: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Далее", callback_data=next_cb)
    return kb.as_markup()
