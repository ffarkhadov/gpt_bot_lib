from datetime import datetime
from pydantic import BaseModel, Field

class User(BaseModel):
    tg_id: int
    username: str | None = None
    full_name: str | None = None
    sa_email: str
    created: datetime = Field(default_factory=datetime.utcnow)
