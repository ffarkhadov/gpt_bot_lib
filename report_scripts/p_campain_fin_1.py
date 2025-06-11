"""
p_campain_fin_1  — обновляет колонку F («Расходы на рекламу») в листе unit-day.

run(
    perf_client_id     = "...",
    perf_client_secret = "...",
    gs_cred            = "/path/sa.json",
    spread_id          = "1AbC...",
    token_oz           = "",            # игнорируется, но нужен для унификации kwargs
    days               = 7,
    worksheet_main     = "unit-day",
    **_
)
"""
from __future__ import annotations
import io, zipfile, time, requests, pandas as pd
from datetime import datetime, timedelta, timezone
from collections import defaultdict


# ───────────────────────── helpers ─────────────────────────
def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ───────────────────────── main ────────────────────────────
def run(*, perf_client_id: str, perf_client_secret: str,
        gs_cred: str, spread_id: str,
        token_oz: str = "",       # лишний, но для совместимости
        days: int = 7,
        worksheet_main: str = "unit-day",
        **_) -> None:

    import gspread
    from google.oauth2.service_account import Credentials

    host = "https://api-performance.ozon.ru"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    # ---------- auth ----------
    def refresh_token() -> datetime:
        payload = {"client_id": perf_client_id,
                   "client_secret": perf_client_secret,
                   "grant_type": "client_credentials"}
        r = requests.post(f"{host}/api/client/token", headers=headers, json=payload)
        r.raise_for_status()
        tok = r.json()["access_token"]
        headers["Authorization"] = f"Bearer {tok}"
        print("[ads] 🔄 token refreshed")
        return datetime.now(timezone.utc)

    token_time = refresh_token()

    # ---------- campaigns ----------
    camps = requests.get(f"{host}/api/client/campaign", headers=headers).json()["list"]
    camp_ids = [c["id"] for c in camps if c["state"] in
                {"CAMPAIGN_STATE_RUNNING", "CAMPAIGN_STATE_STOPPED", "CAMPAIGN_STATE_INACTIVE"}]

    # ---------- wait UUID ----------
    def wait_uuid(uuid: str, interval=120):
        nonlocal token_time                      # <-- ключевая строка
        url = f"{host}/api/client/statistics/{uuid}"
        while True:
            if (datetime.now(timezone.utc) - token_time).total_seconds() > 1500:
                token_time = refresh_token()
            resp = requests.get(url, headers=headers)
            state = resp.json().get("state")
            if state == "OK":
                return
            if state == "FAILED":
                raise RuntimeError(f"Report {uuid} failed")
            time.sleep(interval)

    # ---------- запросы статистики ----------
    uuids = []
    date_to   = datetime.now(timezone.utc) + timedelta(hours=3)
    date_from = date_to - timedelta(days=days)
    for chunk_ids in chunk(camp_ids, 10):
        body = {"campaigns": chunk_ids,
                "dateFrom": date_from.date().isoformat(),
                "dateTo":   date_to.date().isoformat(),
                "groupBy": "DATE"}
        r = requests.post(f"{host}/api/client/statistics", headers=headers, json=body)
        uuid = r.json()["UUID"]
        uuids.append(uuid)
        wait_uuid(uuid)

    # ---------- скачиваем ZIP ----------
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
                df = pd.read_csv(io.TextIOWrapper(zf.open(fn), "utf-8"),
                                 sep=";", skiprows=1,
                                 usecols=["День", "sku", "Расход, ₽, с НДС"])
                df = (df[df["День"] != "Всего"]
                      .assign(sku=lambda d: pd.to_numeric(d["sku"], errors="coerce"))
                      .dropna(subset=["sku"]))
                df["sku"] = df["sku"].astype(int)
                df["Расход, ₽, с НДС"] = df["Расход, ₽, с НДС"].str.replace(",", ".").astype(float)
                frames.append(df)

    if not frames:
        print("[ads] нет данных"); return

    grp = (pd.concat(frames)
             .groupby(["День", "sku"], as_index=False)["Расход, ₽, с НДС"]
             .sum().rename(columns={"Расход, ₽, с НДС": "rub"}))

    # ---------- Google-Sheets ----------
    creds = Credentials.from_service_account_file(gs_cred,
        scopes=["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"])
    ws = gspread.authorize(creds).open_by_key(spread_id).worksheet(worksheet_main)

    sheet = ws.get_all_values()
    hdr   = sheet[0]
    idx_adv = hdr.index("Расходы на рекламу")   # колонка F по ТЗ

    map_rows = {(r[0].split(" ")[0], int(r[1])): i+2
                for i, r in enumerate(sheet[1:], start=1)
                if r and r[0] not in ("", "Итого")}

    updates = []
    for _, r in grp.iterrows():
        key = (r["День"], int(r["sku"]))
        if key in map_rows:
            updates.append({"range": f"{chr(65+idx_adv)}{map_rows[key]}",
                            "values": [[r["rub"]]]})

    if updates:
        ws.batch_update(updates)
    print("[ads] ✅ колонка F обновлена")
