import json
from datetime import datetime
from typing import Tuple

from core.services.sheets import SheetsClient
from config import settings

TECH_SHEET_ID = settings.TECH_SHEET_ID
SA_WORKSHEET = "ServiceAccounts"

async def pick_service_account(sheets: SheetsClient) -> Tuple[str, str]:
    """
    Возвращает (email, json_path) наименее загруженного SA
    и сразу инкрементирует users_count.
    """
    ws = await sheets.get_worksheet(TECH_SHEET_ID, SA_WORKSHEET)
    rows = (await sheets.read_all(ws))[1:]  # без заголовка
    idx_min, min_val = None, 1e9
    for idx, row in enumerate(rows, start=2):  # учёт заголовка
        status = row[4]
        if status != "active":
            continue
        users_count = int(row[2])
        if users_count < min_val:
            min_val = users_count
            idx_min = idx

    if idx_min is None:
        raise RuntimeError("Нет доступных сервис-аккаунтов")

    # читаем строку-победителя
    row = ws.row_values(idx_min)
    email, json_path = row[0], row[1]

    # инкрементируем + метка времени
    ws.update(
        f"C{idx_min}:D{idx_min}",
        [[int(row[2]) + 1, datetime.utcnow().isoformat(timespec='seconds')]],
    )
    return email, json_path
