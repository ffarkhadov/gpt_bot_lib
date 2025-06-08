import importlib
from pathlib import Path

from core.services.sheets import SheetsClient
from core.services.sa_pool import pick_service_account
from config import settings

REPORT_TITLE = "unit-day"

async def run_report(store_cfg: dict):
    """
    store_cfg содержит: {sheet_id, marketplace, creds_json, ...}
    """
    # Шаг 1. Берём SA
    tech_sheet_sa = SheetsClient(json_path=settings.BASE_DIR / "dummy_sa.json")
    sa_email, sa_json = await pick_service_account(tech_sheet_sa)

    # Шаг 2. Авторизуемся под выданным SA
    sheets = SheetsClient(sa_json)
    ws = await sheets.get_worksheet(store_cfg["sheet_id"], REPORT_TITLE)

    # Шаг 3. Импортируем «внешнюю логику» динамически
    module = importlib.import_module("report_scripts.unit_day_5")
    await module.run(store_cfg, ws)  # type: ignore[attr-defined]
