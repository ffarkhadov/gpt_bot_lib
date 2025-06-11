"""
p_campain_fin_1 (динамический)
Заполняет столбец F («Расходы на рекламу») в листе unit-day.
run(
    perf_client_id='...',
    perf_client_secret='...',
    gs_cred='/path/sa.json',
    spread_id='1AbC…',
    days=7
)
"""
from __future__ import annotations
import io, time, zipfile, requests, pandas as pd
from datetime import datetime, timezone, timedelta
from collections import defaultdict

def run(*, perf_client_id: str, perf_client_secret: str,
        gs_cred: str, spread_id: str,
        token_oz: str = "",    # лишний, но чтоб не ругался
        days: int = 7,
        worksheet_main: str = "unit-day",
        **_) -> None:          # ← игнорируем будущие параметры

    import gspread
    from google.oauth2.service_account import Credentials

    host = "https://api-performance.ozon.ru"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    # auth ---------------------------------------------------
    def refresh_token():
        payload = {"client_id": perf_client_id,
                   "client_secret": perf_client_secret,
                   "grant_type": "client_credentials"}
        r = requests.post(host + "/api/client/token", headers=headers, json=payload)
        r.raise_for_status()
        tok = r.json()["access_token"]
        headers["Authorization"] = f"Bearer {tok}"
        return datetime.now(timezone.utc)

    token_time = refresh_token()

    # campaigns ---------------------------------------------
    ids = requests.get(f"{host}/api/client/campaign", headers=headers).json()["list"]
    camp_ids = [c["id"] for c in ids if c["state"] in
                {"CAMPAIGN_STATE_RUNNING","CAMPAIGN_STATE_STOPPED","CAMPAIGN_STATE_INACTIVE"}]

    # helpers
    def chunk(lst, n):   # 10-штук пачки
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    def wait_uuid(uuid):
        url = f"{host}/api/client/statistics/{uuid}"
        while True:
            if (datetime.now(timezone.utc)-token_time).total_seconds()>1500:
                token_time = refresh_token()
            r = requests.get(url, headers=headers); js = r.json()
            if js["state"] == "OK": return
            if js["state"] == "FAILED": raise RuntimeError(f"Report {uuid} failed")
            time.sleep(120)

    # 1) запрашиваем uuid
    uuids = []
    date_to = datetime.now(timezone.utc)+timedelta(hours=3)
    date_from = date_to - timedelta(days=days)
    for ch in chunk(camp_ids, 10):
        body = {"campaigns": ch,
                "dateFrom": date_from.date().isoformat(),
                "dateTo":   date_to.date().isoformat(),
                "groupBy": "DATE"}
        r = requests.post(f"{host}/api/client/statistics", headers=headers, json=body)
        uuid = r.json()["UUID"]; uuids.append(uuid); wait_uuid(uuid)

    # 2) получаем zip-отчёты
    frames = []
    for u in uuids:
        r = requests.get(f"{host}/api/client/statistics/report", headers=headers,
                         params={"UUID": u})
        if r.status_code!=200 or "application/zip" not in r.headers.get("Content-Type",""):
            continue
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            for fn in zf.namelist():
                if fn.endswith(".csv") or fn.endswith(".txt"):
                    df = pd.read_csv(io.TextIOWrapper(zf.open(fn), "utf-8"),
                                     sep=";", skiprows=1,
                                     usecols=["День","sku","Расход, ₽, с НДС"])
                    df = df[df["День"]!="Всего"]
                    df["sku"]  = pd.to_numeric(df["sku"], errors="coerce").astype("Int64")
                    df["Расход, ₽, с НДС"] = df["Расход, ₽, с НДС"].str.replace(",",".").astype(float)
                    frames.append(df)

    if not frames:
        print("[ads] нет данных"); return

    comb = pd.concat(frames)
    grp  = comb.groupby(["День","sku"],as_index=False)["Расход, ₽, с НДС"].sum()
    grp.columns = ["date","sku","rub"]

    # 3) Google-Sheets
    creds = Credentials.from_service_account_file(gs_cred,
        scopes=["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"])
    sh = gspread.authorize(creds).open_by_key(spread_id)
    ws = sh.worksheet(worksheet_main)

    sheet = ws.get_all_values()
    hdr   = sheet[0]
    idx_adv = hdr.index("Расходы на рекламу")   # столбец F

    # строим map (date, sku) → row_index
    map_rows = {(r[0].split(" ")[0], int(r[1])): i+2
                for i,r in enumerate(sheet[1:], start=1)
                if r and r[0] not in ("","Итого")}

    updates = []
    for _, r in grp.iterrows():
        key = (r["date"], int(r["sku"]))
        if key in map_rows:
            row = map_rows[key]
            updates.append({"range": f"{chr(65+idx_adv)}{row}",
                            "values": [[r["rub"]]]})

    # пачка
    if updates:
        ws.batch_update(updates)
    print("[ads] ✅ колонка F обновлена")
