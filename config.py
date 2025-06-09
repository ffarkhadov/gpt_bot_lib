from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field

BASE_DIR = Path(__file__).resolve().parent

class Settings(BaseSettings):
    BOT_TOKEN: str = Field(..., env="BOT_TOKEN")                  # обязательное поле
    TECH_SHEET_ID: str = Field(..., env="TECH_SHEET_ID")          # обязательное поле
    ADMIN_SA_JSON: str = Field(..., env="ADMIN_SA_JSON")          # путь к JSON сервисного аккаунта
    WORKERS: int = 2                                              # опционально, по умолчанию 2

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
