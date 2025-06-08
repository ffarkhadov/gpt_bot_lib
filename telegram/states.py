from aiogram.fsm.state import State, StatesGroup


class AddStore(StatesGroup):
    choose_mp = State()
    client_id = State()      # Ozon only
    api_key = State()
    sheet_id = State()
    confirm_access = State()
