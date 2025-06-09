from aiogram.fsm.state import State, StatesGroup


class AddStore(StatesGroup):
    choose_mp = State()
    client_id = State()     # только для Ozon
    api_key = State()
    sheet_id = State()
    confirm = State()

