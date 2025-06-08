from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field

BASE_DIR = Path(__file__).resolve().parent

class Settings(BaseSettings):
    BOT_TOKEN: str = Field(..., env="BOT_TOKEN")          # обязательный
    TECH_SHEET_ID: str = Field(..., env="TECH_SHEET_ID")  # таблица ServiceAccounts/Users/Stores
    WORKERS: int = 2                                      # default worker pool

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

