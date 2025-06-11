from aiogram.fsm.state import State, StatesGroup


class AddStore(StatesGroup):
    choose_mp        = State()   # выбор маркетплейса
    client_id        = State()   # Client-ID Seller API (Ozon)
    api_key          = State()   # API-Key Seller API / WB
    perf_id          = State()   # Performance Client-ID (Ozon)
    perf_secret      = State()   # Performance Client-Secret (Ozon)
    sheet_id         = State()   # ID Google-таблицы
    confirm          = State()   # подтверждение перед сохранением
