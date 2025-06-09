from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardMarkup


def kb_main(stores: list[tuple[str, str, str]]) -> InlineKeyboardMarkup:
    """
    stores = [(store_id, name, marketplace)]
    """
    kb = InlineKeyboardBuilder()

    # Кнопки магазинов (если есть)
    for store_id, name, mp in stores:
        kb.button(
            text=f"{name} · {mp.upper()}",
            callback_data=f"store_{store_id}",
        )

    # Кнопки «Добавить …»
    kb.button(text="➕ Добавить Ozon магазин", callback_data="add_ozon")
    kb.button(text="➕ Добавить Wildberries магазин", callback_data="add_wb")
    kb.adjust(1)
    return kb.as_markup()
