from pydantic import BaseModel

class ServiceAccount(BaseModel):
    email: str
    json_path: str
    users_count: int
    status: str  # active / disabled
