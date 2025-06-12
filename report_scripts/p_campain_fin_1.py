"""
p_campain_fin_1 (обновлён)
──────────────────────────
• Сопоставление выполняется ТОЛЬКО по SKU (колонка B листа unit-day);
• Если у одного SKU несколько дат в листе – расход пишется во все строки
  с данным SKU;
• Защита от расхождения заголовков CSV (рус/англ);
• Ошибки чтения CSV больше не роняют отчёт.
"""

from __future__ import annotations
import io, zipfile, time, requests, pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, List


def chunk(lst, n):          # helper
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def run(*, perf_client_id: str, perf_client_secret: str,
        gs_cred: str, spread_id: str,
        days: int = 7, worksheet_main: str = "unit-day", **_) -> None:

    import gspread
    from google.oauth2.service_account import Credentials

    host = "https://api-performance.ozon.ru"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    def refresh() -> datetime:
        body = {"client_id": perf_client_id,
                "client_secret": perf_client_secret,
                "grant_type": "client_credentials"}
        token = requests.post(host + "/api/client/token",
                              headers=headers, json=body).json()["access_token"]
        headers["Authorization"] = f"Bearer {token}"
        return datetime.now(timezone.utc)

    token_time = refresh()

    # кампании
    camps = requests.get(f"{host}/api/client/campaign", headers=headers).json()["list"]
    camp_ids = [c["id"] for c in camps if c["state"] in
                {"CAMPAIGN_STATE_RUNNING", "CAMPAIGN_STATE_STOPPED", "CAMPAIGN_STATE_INACTIVE"}]

    # даты интервала
    msk_now = datetime.now(timezone.utc) + timedelta(hours=3)
    msk_from = msk_now - timedelta(days=days)

    uuids: List[str] = []

    # 1. Запрашиваем отчёты
    for part in chunk(camp_ids, 10):
        if (datetime.now(timezone.utc) - token_time).total_seconds() > 1500:
            token_time = refresh()
        body = {"campaigns": part,
                "dateFrom": msk_from.date().isoformat(),
                "dateTo": msk_now.date().isoformat(),
                "groupBy": "DATE"}
        r = requests.post(f"{host}/api/client/statistics",
                          headers=headers, json=body)
        if r.status_code != 200 or "UUID" not in r.json():
            continue
        uid = r.json()["UUID"]; uuids.append(uid)

    if not uuids:
        print("[ads] нет uuid"); return

    # 2. Ждём готовности
    def wait(u: str):
        nonlocal token_time
        while True:
            if (datetime.now(timezone.utc)-token_time).total_seconds() > 1500:
                token_time = refresh()
            st = requests.get(f"{host}/api/client/statistics/{u}",
                              headers=headers).json().get("state")
            if st == "OK":
                return
            if st == "FAILED":
                raise RuntimeError(f"report {u} failed")
            time.sleep(120)

    for u in uuids:
        wait(u)

    # 3. Читаем ZIP и агрегируем
    ALIAS = {"Day": "date", "День": "date",
             "SKU": "sku", "Расход, ₽, с НДС": "cost",
             "Spend, RUB with VAT": "cost"}
    frames = []
    for u in uuids:
        z = requests.get(f"{host}/api/client/statistics/report",
                         headers=headers, params={"UUID": u})
        if z.status_code != 200 or "application/zip" not in z.headers.get("Content-Type", ""):
            continue
        with zipfile.ZipFile(io.BytesIO(z.content)) as zf:
            for fn in zf.namelist():
                if not fn.endswith((".csv", ".txt")):
                    continue
                df = pd.read_csv(io.TextIOWrapper(zf.open(fn), "utf-8"), sep=";", skiprows=1)
                df = df.rename(columns=ALIAS)
                if not {"date", "sku", "cost"}.issubset(df.columns):
                    continue
                df = df[df["date"] != "Всего"]
                df["sku"] = pd.to_numeric(df["sku"], errors="coerce").dropna().astype(int)
                df["cost"] = df["cost"].astype(str).str.replace(",", ".").astype(float)
                frames.append(df[["sku", "cost"]])

    if not frames:
        print("[ads] нет строк csv"); return

    grouped = (pd.concat(frames)          # агрегируем по SKU
               .groupby("sku", as_index=False)["cost"].sum())

    # 4. Google Sheets
    creds = Credentials.from_service_account_file(
        gs_cred,
        scopes=["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"])
    ws = gspread.authorize(creds).open_by_key(spread_id).worksheet(worksheet_main)

    hdr = ws.row_values(1)
    col_adv = hdr.index("Расходы на рекламу")         # F
    data = ws.get_all_values()[1:]

    # карта sku → список rowIdx
    sku_rows: Dict[int, List[int]] = {}
    for i, row in enumerate(data, start=2):
        try:
            sku_val = int(row[1])
            sku_rows.setdefault(sku_val, []).append(i)
        except Exception:
            continue

    updates = []
    for _, row in grouped.iterrows():
        sku, rub = int(row["sku"]), row["cost"]
        for r in sku_rows.get(sku, []):
            updates.append({"range": f"{chr(65+col_adv)}{r}", "values": [[rub]]})

    if updates:
        ws.batch_update(updates)
    print("[ads] ✅ колонка F обновлена")
