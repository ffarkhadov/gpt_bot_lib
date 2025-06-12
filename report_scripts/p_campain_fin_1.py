"""
p_campain_fin_1.py  (фикс: расход = дата+SKU)
"""

from __future__ import annotations
import io, zipfile, time, requests, pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, List

# ───────── helpers ─────────
def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def norm_date(dt: str) -> str:                 # '2025-06-10' → '10.06.2025'
    try:
        return datetime.strptime(dt.split()[0], "%Y-%m-%d").strftime("%d.%m.%Y")
    except Exception:
        return dt


# ───────── MAIN ─────────
def run(*, perf_client_id: str, perf_client_secret: str,
        gs_cred: str, spread_id: str,
        days: int = 7, worksheet_main: str = "unit-day", **_) -> None:

    import gspread
    from google.oauth2.service_account import Credentials

    host = "https://api-performance.ozon.ru"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    # ---- auth ----
    def refresh() -> datetime:
        body = {"client_id": perf_client_id,
                "client_secret": perf_client_secret,
                "grant_type": "client_credentials"}
        token = requests.post(f"{host}/api/client/token",
                              headers=headers, json=body).json()["access_token"]
        headers["Authorization"] = f"Bearer {token}"
        return datetime.now(timezone.utc)

    token_time = refresh()

    # ---- campaigns ----
    camps = requests.get(f"{host}/api/client/campaign", headers=headers).json()["list"]
    camp_ids = [c["id"] for c in camps if c["state"] in
                {"CAMPAIGN_STATE_RUNNING", "CAMPAIGN_STATE_STOPPED", "CAMPAIGN_STATE_INACTIVE"}]

    # ---- interval ----
    msk_now = datetime.now(timezone.utc) + timedelta(hours=3)
    msk_from = msk_now - timedelta(days=days)

    uuids: List[str] = []

    # 1️⃣ запрашиваем отчёты
    for part in chunk(camp_ids, 10):
        if (datetime.now(timezone.utc) - token_time).total_seconds() > 1500:
            token_time = refresh()
        body = {"campaigns": part,
                "dateFrom": msk_from.date().isoformat(),
                "dateTo": msk_now.date().isoformat(),
                "groupBy": "DATE"}
        r = requests.post(f"{host}/api/client/statistics",
                          headers=headers, json=body)
        if r.status_code == 200 and "UUID" in r.json():
            uuids.append(r.json()["UUID"])

    if not uuids:
        print("[ads] нет uuid"); return

    # 2️⃣ ждём готовности
    def wait(u: str):
        nonlocal token_time
        while True:
            if (datetime.now(timezone.utc) - token_time).total_seconds() > 1500:
                token_time = refresh()
            st = requests.get(f"{host}/api/client/statistics/{u}",
                              headers=headers).json().get("state")
            if st == "OK":
                return
            if st == "FAILED":
                raise RuntimeError(f"report {u} failed")
            time.sleep(120)

    for u in uuids: wait(u)

    # 3️⃣ читаем ZIP-ы
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
                df = pd.read_csv(io.TextIOWrapper(zf.open(fn), "utf-8"),
                                 sep=";", skiprows=1).rename(columns=ALIAS)
                if not {"date", "sku", "cost"}.issubset(df.columns):
                    continue
                df = df[df["date"] != "Всего"]
                df["date"] = df["date"].apply(norm_date)
                df["sku"] = pd.to_numeric(df["sku"], errors="coerce").dropna().astype(int)
                df["cost"] = (df["cost"].astype(str)
                              .str.replace(",", ".").astype(float))
                frames.append(df[["date", "sku", "cost"]])

    if not frames:
        print("[ads] csv пусты"); return

    grouped = (pd.concat(frames)
               .groupby(["date", "sku"], as_index=False)["cost"].sum())

    # 4️⃣ Google Sheets
    creds = Credentials.from_service_account_file(
        gs_cred,
        scopes=["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"])
    ws = (gspread.authorize(creds)
          .open_by_key(spread_id).worksheet(worksheet_main))

    hdr = ws.row_values(1)
    col_adv = hdr.index("Расходы на рекламу")   # F
    data = ws.get_all_values()[1:]

    # карта (date, sku) → list[rowIdx]
    pairs: Dict[Tuple[str, int], List[int]] = {}
    for i, row in enumerate(data, start=2):
        dt = (row[0].split()[0] if row[0] else "")
        try:
            sku_val = int(row[1])
            pairs.setdefault((dt, sku_val), []).append(i)
        except Exception:
            continue

    updates = []
    for _, r in grouped.iterrows():
        key = (r["date"], int(r["sku"]))
        for row_idx in pairs.get(key, []):
            updates.append({
                "range": f"{chr(65+col_adv)}{row_idx}",
                "values": [[round(r["cost"], 2)]]
            })

    if updates:
        ws.batch_update(updates)
    print("[ads] ✅ F-колонка обновлена по дате + SKU")
