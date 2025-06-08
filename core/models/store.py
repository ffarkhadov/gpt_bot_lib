from datetime import datetime
from pydantic import BaseModel, Field

class Store(BaseModel):
    store_id: str                       # например client_id (Ozon) или supplier_id (WB)
    owner_id: int                       # tg_id главного пользователя
    marketplace: str                    # "ozon" | "wb"
    name: str                           # человекочитаемое
    credentials_json: str               # json-dump ключей
    sheet_id: str
    extra_users_json: str = "[]"        # json-array Tg-id
    created: datetime = Field(default_factory=datetime.utcnow)
