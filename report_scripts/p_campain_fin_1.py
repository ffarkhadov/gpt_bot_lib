"""
p_campain_fin_1 (динамический)
──────────────────────────────
Запрашивает статистику Performance API, суммирует расход
по SKU × дне и записывает значения в колонку F («Расходы на рекламу»)
листа unit-day.

run(
    perf_client_id     = "...@advertising.performance.ozon.ru",
    perf_client_secret = "...",
    gs_cred            = "/path/sa.json",
    spread_id          = "1AbC…",
    days               = 7,
    worksheet_main     = "unit-day",
    token_oz           = "",  # не используется, для унификации kwargs
    **_
)
"""
from __future__ import annotations
import io, zipfile, time, requests, pandas as pd
from datetime import datetime, timezone, timedelta
from typing import List, Dict


# ───────────────────────── helpers ─────────────────────────
def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ───────────────────────── main ────────────────────────────
def run(*, perf_client_id: str, perf_client_secret: str,
        gs_cred: str, spread_id: str,
        days: int = 7, worksheet_main: str = "unit-day",
        token_oz: str = "", **_) -> None:

    import gspread
    from google.oauth2.service_account import Credentials

    host = "https://api-performance.ozon.ru"
    endpoint_token = "/api/client/token"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    # ---------- auth ----------
    def refresh_access_token() -> datetime:
        payload = {"client_id": perf_client_id,
                   "client_secret": perf_client_secret,
                   "grant_type": "client_credentials"}
        r = requests.post(host + endpoint_token, headers=headers, json=payload)
        r.raise_for_status()
        tok = r.json()["access_token"]
        headers["Authorization"] = f"Bearer {tok}"
        print("[ads] 🔄 новый токен Performance API")
        return datetime.now(timezone.utc)

    token_time = refresh_access_token()

    # ---------- список кампаний ----------
    resp = requests.get(f"{host}/api/client/campaign", headers=headers)
    resp.raise_for_status()
    camps = resp.json().get("list", [])
    camp_ids = [c["id"] for c in camps
                if c["state"] in {"CAMPAIGN_STATE_RUNNING",
                                  "CAMPAIGN_STATE_STOPPED",
                                  "CAMPAIGN_STATE_INACTIVE"}]
    print(f"[ads] кампаний: {len(camp_ids)}")

    # ---------- ожидание отчёта ----------
    def wait_uuid(uuid: str, interval=120):
        nonlocal token_time
        url = f"{host}/api/client/statistics/{uuid}"
        while True:
            if (datetime.now(timezone.utc) - token_time).total_seconds() > 1500:
                token_time = refresh_access_token()
            r = requests.get(url, headers=headers)
            if r.status_code == 403:
                token_time = refresh_access_token(); continue
            js = r.json(); st = js.get("state")
            print(f"[ads]  UUID {uuid} → {st}")
            if st == "OK":
                return
            if st == "FAILED":
                raise RuntimeError(f"report {uuid} failed")
            time.sleep(interval)

    # ---------- запрашиваем UUID-ы ----------
    date_to   = datetime.now(timezone.utc) + timedelta(hours=3)
    date_from = date_to - timedelta(days=days)
    uuids: List[str] = []

    for chunk in chunk_list(camp_ids, 10):
        payload = {"campaigns": chunk,
                   "dateFrom": date_from.date().isoformat(),
                   "dateTo":   date_to.date().isoformat(),
                   "groupBy": "DATE"}
        r = requests.post(f"{host}/api/client/statistics",
                          headers=headers, json=payload)
        if r.status_code != 200 or "UUID" not in r.json():
            print(f"[ads] ⚠️ ошибка запроса статистики {r.status_code}: {r.text}")
            continue
        uuid = r.json()["UUID"]; uuids.append(uuid)
        wait_uuid(uuid)

    if not uuids:
        print("[ads] нет отчётов"); return

    # ---------- скачиваем zip ----------
    frames = []
    for uuid in uuids:
        r = requests.get(f"{host}/api/client/statistics/report",
                         headers=headers, params={"UUID": uuid})
        if r.status_code != 200 or "application/zip" not in r.headers.get("Content-Type", ""):
            print(f"[ads] ⚠️ не zip для {uuid} [{r.status_code}]"); continue
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            for fn in zf.namelist():
                if not fn.endswith((".csv", ".txt")):
                    continue
                df = pd.read_csv(io.TextIOWrapper(zf.open(fn), "utf-8"),
                                 sep=";", skiprows=1,
                                 usecols=["День", "sku", "Расход, ₽, с НДС"])
                df = df[df["День"] != "Всего"]
                df = df[pd.to_numeric(df["sku"], errors="coerce").notna()]
                df["sku"] = df["sku"].astype(int)
                df["Расход, ₽, с НДС"] = df["Расход, ₽, с НДС"] \
                                              .str.replace(",", ".").astype(float)
                frames.append(df)

    if not frames:
        print("[ads] нет строк"); return

    grp = (pd.concat(frames)
             .groupby(["День", "sku"], as_index=False)["Расход, ₽, с НДС"]
             .sum().rename(columns={"Расход, ₽, с НДС": "rub"}))

    # ---------- Google Sheets ----------
    creds = Credentials.from_service_account_file(
        gs_cred,
        scopes=["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"])
    ws = gspread.authorize(creds).open_by_key(spread_id).worksheet(worksheet_main)

    all_vals = ws.get_all_values()
    hdr      = all_vals[0]
    col_adv  = hdr.index("Расходы на рекламу")   # колонка F
    rows_map: Dict[tuple[str, int], int] = {
        (r[0].split(" ")[0], int(r[1])): idx+2
        for idx, r in enumerate(all_vals[1:])
        if r and r[0] not in ("", "Итого")
    }

    updates = []
    for _, row in grp.iterrows():
        key = (row["День"], int(row["sku"]))
        if key in rows_map:
            updates.append({"range": f"{chr(65+col_adv)}{rows_map[key]}",
                            "values": [[row["rub"]]]})

    if updates:
        ws.batch_update(updates)
    print("[ads] ✅ колонка F обновлена")
