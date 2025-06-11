"""
p_campain_fin_1 (динамический)
──────────────────────────────
• Получает отчёты Performance API
• Суммирует расход (RUB with VAT) по SKU × день
• Записывает значения в колонку «Расходы на рекламу» (F) листа unit-day
"""

from __future__ import annotations
import io, zipfile, time, requests, pandas as pd
from datetime import datetime, timezone, timedelta
from typing import List, Dict


def chunk_list(lst, n):            # helper
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ───────────────────────── main ─────────────────────────
def run(*, perf_client_id: str, perf_client_secret: str,
        gs_cred: str, spread_id: str,
        days: int = 7, worksheet_main: str = "unit-day",
        token_oz: str = "", **_) -> None:

    import gspread
    from google.oauth2.service_account import Credentials

    host = "https://api-performance.ozon.ru"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    def refresh_token() -> datetime:
        body = {"client_id": perf_client_id,
                "client_secret": perf_client_secret,
                "grant_type": "client_credentials"}
        tok = requests.post(host + "/api/client/token",
                            headers=headers, json=body).json()["access_token"]
        headers["Authorization"] = f"Bearer {tok}"
        return datetime.now(timezone.utc)

    token_time = refresh_token()

    # ---------- кампании ----------
    camps = requests.get(f"{host}/api/client/campaign", headers=headers).json()["list"]
    camp_ids = [c["id"] for c in camps if c["state"] in
                {"CAMPAIGN_STATE_RUNNING", "CAMPAIGN_STATE_STOPPED", "CAMPAIGN_STATE_INACTIVE"}]

    # ---------- ожидание ----------
    def wait(uuid: str):
        nonlocal token_time
        url = f"{host}/api/client/statistics/{uuid}"
        while True:
            if (datetime.now(timezone.utc)-token_time).total_seconds() > 1500:
                token_time = refresh_token()
            st = requests.get(url, headers=headers).json().get("state")
            if st == "OK":
                return
            if st == "FAILED":
                raise RuntimeError(f"report {uuid} failed")
            time.sleep(120)

    # ---------- запрашиваем UUID ----------
    uuids: List[str] = []
    d_to   = datetime.now(timezone.utc) + timedelta(hours=3)
    d_from = d_to - timedelta(days=days)
    for chunk in chunk_list(camp_ids, 10):
        body = {"campaigns": chunk,
                "dateFrom": d_from.date().isoformat(),
                "dateTo":   d_to.date().isoformat(),
                "groupBy": "DATE"}
        r = requests.post(f"{host}/api/client/statistics",
                          headers=headers, json=body)
        if r.status_code != 200 or "UUID" not in r.json():
            continue
        uuid = r.json()["UUID"]; uuids.append(uuid); wait(uuid)

    if not uuids:
        print("[ads] нет uuid"); return

    # ---------- читаем ZIP ----------
    ALIASES = {
        "Day": "День",
        "SKU": "sku",
        "Расход, ₽, с НДС": "Расход, ₽, с НДС",
        "Spend, RUB with VAT": "Расход, ₽, с НДС",
    }
    frames = []
    for uuid in uuids:
        r = requests.get(f"{host}/api/client/statistics/report",
                         headers=headers, params={"UUID": uuid})
        if r.status_code != 200 or "application/zip" not in r.headers.get("Content-Type", ""):
            continue
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            for fn in zf.namelist():
                if not fn.endswith((".csv", ".txt")):
                    continue
                df = pd.read_csv(io.TextIOWrapper(zf.open(fn), "utf-8"), sep=";", skiprows=1)
                df = df.rename(columns=ALIASES)
                if not {"День", "sku", "Расход, ₽, с НДС"}.issubset(df.columns):
                    continue
                df = df[df["День"] != "Всего"]
                df["sku"] = pd.to_numeric(df["sku"], errors="coerce").dropna().astype(int)
                df["Расход, ₽, с НДС"] = df["Расход, ₽, с НДС"].astype(str) \
                                                  .str.replace(",", ".").astype(float)
                frames.append(df[["День", "sku", "Расход, ₽, с НДС"]])

    if not frames:
        print("[ads] нет строк csv"); return

    grp = (pd.concat(frames)
           .groupby(["День", "sku"], as_index=False)["Расход, ₽, с НДС"]
           .sum().rename(columns={"Расход, ₽, с НДС": "rub"}))

    # ---------- Google Sheets ----------
    creds = Credentials.from_service_account_file(gs_cred,
        scopes=["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"])
    ws = gspread.authorize(creds).open_by_key(spread_id).worksheet(worksheet_main)

    hdr       = ws.row_values(1)
    col_adv   = hdr.index("Расходы на рекламу")
    sheet_val = ws.get_all_values()[1:]

    row_map: Dict[tuple[str, int], int] = {
        (r[0].split(" ")[0], int(r[1])): idx+2
        for idx, r in enumerate(sheet_val)
        if r and r[0] not in ("", "Итого")
    }

    updates = []
    for _, row in grp.iterrows():
        key = (row["День"], int(row["sku"]))
        if key in row_map:
            updates.append({"range": f"{chr(65+col_adv)}{row_map[key]}",
                            "values": [[row["rub"]]]})

    if updates:
        ws.batch_update(updates)
    print("[ads] ✅ колонка F обновлена")
