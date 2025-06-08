import asyncio
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

BACKOFF = (1, 2, 4)  # секунды

def _retry(func):
    async def wrapper(*args, **kwargs):
        for delay in (*BACKOFF, None):
            try:
                return await func(*args, **kwargs)
            except Exception:
                if delay is None:
                    raise
                await asyncio.sleep(delay)
    return wrapper


class SheetsClient:
    def __init__(self, json_path: str):
        creds = Credentials.from_service_account_file(json_path, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        self.gc = gspread.authorize(creds)

    @_retry
    async def get_worksheet(self, sheet_id: str, title: str):
        sh = self.gc.open_by_key(sheet_id)
        try:
            return sh.worksheet(title)
        except gspread.WorksheetNotFound:
            return sh.add_worksheet(title, rows=1000, cols=26)

    @_retry
    async def read_all(self, ws) -> list[list[Any]]:
        return ws.get_all_values()

    @_retry
    async def append_rows(self, ws, rows: list[list[Any]]):
        ws.append_rows(rows, value_input_option="USER_ENTERED")
