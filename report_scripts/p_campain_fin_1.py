"""
report_scripts/p_campain_fin_1.py
─────────────────────────────────
Заполняет колонку «Расходы на рекламу» (F) листа *unit-day* по данным
Ozon Performance API.

run(
    gs_cred="/path/to/sa.json",
    spread_id="1AbCdE…",
    perf_client_id="…@advertising.performance.ozon.ru",
    perf_client_secret="XXXX",
    sheet_main="unit-day",          # можно переопределить
    days=7                          # диапазон отчёта (МСК)
)
"""

from __future__ import annotations

# — built-ins
import io, zipfile, time, json, logging
from datetime import datetime, timezone, timedelta
from functools   import partial
from pathlib     import Path
from typing      import List

# — 3-rd party
import requests, pandas as pd, gspread
from google.oauth2.service_account import Credentials

log = logging.getLogger(__name__)
HOST = "https://api-performance.ozon.ru"
TOKEN_EP = "/api/client/token"
CAMPAIGN_EP = "/api/client/campaign"
STAT_EP = "/api/client/statistics"
REPORT_EP = "/api/client/statistics/report"

# ---------------------------------------------------------------------------
#                               ── helpers ──
# ---------------------------------------------------------------------------
def auth_headers(token: str | None = None) -> dict[str, str]:
    hdr = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        hdr["Authorization"] = f"Bearer {token}"
    return hdr


def get_token(pid: str, psecret: str) -> tuple[str, datetime]:
    """client_credentials → (token, timeUTC)"""
    r = requests.post(
        f"{HOST}{TOKEN_EP}",
        headers=auth_headers(),
        json={"client_id": pid, "client_secret": psecret,
              "grant_type": "client_credentials"},
        timeout=60,
    )
    r.raise_for_status()
    tok = r.json()["access_token"]
    log.info("PerformanceAPI token OK (%s…)", tok[:8])
    return tok, datetime.now(timezone.utc)


def refresh_if_needed(token: str, t0: datetime,
                      pid: str, psecret: str) -> tuple[str, datetime]:
    """Через 25 мин обновляем токен"""
    if (datetime.now(timezone.utc) - t0).total_seconds() < 1_500:
        return token, t0
    log.info("PerformanceAPI token refresh…")
    return get_token(pid, psecret)


def moscow_dates(days: int = 7) -> tuple[str, str]:
    now_utc = datetime.now(timezone.utc)
    msk = now_utc + timedelta(hours=3)
    return (msk - timedelta(days=days)).strftime("%Y-%m-%d"), msk.strftime("%Y-%m-%d")


def chunked(seq: list, n: int):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


# ---------------------------------------------------------------------------
#                   ── fetch UUID-pages for a chunk of campaigns ──
# ---------------------------------------------------------------------------
def fetch_all_pages(cids: List[int], date_from: str, date_to: str,
                    token: str, pid: str, psecret: str) -> List[str]:
    uuids: List[str] = []
    offset = 0
    while True:
        token, _ = refresh_if_needed(token, fetch_all_pages.t0, pid, psecret)
        payload = {
            "campaigns": cids, "dateFrom": date_from, "dateTo": date_to,
            "groupBy": "DATE", "offset": offset, "limit": 1_000,
        }
        r = requests.post(f"{HOST}{STAT_EP}", headers=auth_headers(token),
                          json=payload, timeout=120)
        r.raise_for_status()
        data = r.json()
        uuid = data.get("UUID")
        if not uuid:
            break
        uuids.append(uuid)
        log.debug("chunk %s → UUID %s (offset %s)", cids[:3], uuid, offset)
        if not data.get("has_more"):
            break
        offset += payload["limit"]
    return uuids
# инициализируем «статическое» поле с моментом последнего токена
fetch_all_pages.t0 = datetime.now(timezone.utc)


def wait_ready(uuid: str, token: str, pid: str, psecret: str) -> None:
    url = f"{HOST}{STAT_EP}/{uuid}"
    while True:
        token, _ = refresh_if_needed(token, wait_ready.t0, pid, psecret)
        r = requests.get(url, headers=auth_headers(token), timeout=60)
        if r.status_code == 403:                      # просроченный токен
            token, _ = get_token(pid, psecret)
            wait_ready.t0 = datetime.now(timezone.utc)
            continue
        r.raise_for_status()
        st = r.json().get("state")
        if st == "OK":
            return
        if st == "FAILED":
            raise RuntimeError(f"UUID {uuid} state=FAILED")
        time.sleep(120)
# статическое поле
wait_ready.t0 = datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
#                   ── main entry point used by the bot ──
# ---------------------------------------------------------------------------
def run(*,
        gs_cred: str,
        spread_id: str,
        perf_client_id: str,
        perf_client_secret: str,
        sheet_main: str = "unit-day",
        days: int = 7) -> None:
    """
    Обновляет F-столбец «Расходы на рекламу» в листe *sheet_main*.
    остальные аргументы игнорируются; передаются ботом «на всякий».
    """

    # 1) токен + список кампаний ------------------------------------------------
    token, t0 = get_token(perf_client_id, perf_client_secret)
    fetch_all_pages.t0 = wait_ready.t0 = t0                      # синхронизируем

    r = requests.get(f"{HOST}{CAMPAIGN_EP}", headers=auth_headers(token), timeout=60)
    r.raise_for_status()
    campaigns = [it["id"] for it in r.json()["list"]
                 if it.get("state") in {"CAMPAIGN_STATE_RUNNING",
                                        "CAMPAIGN_STATE_STOPPED",
                                        "CAMPAIGN_STATE_INACTIVE"}]
    log.info("campaigns: %s", campaigns)

    date_from, date_to = moscow_dates(days)

    # 2) собираем ВСЕ uuid-страницы ---------------------------------------------
    all_uuids: list[str] = []
    for chunk in chunked(campaigns, 10):
        all_uuids += fetch_all_pages(chunk, date_from, date_to,
                                     token, perf_client_id, perf_client_secret)

    if not all_uuids:
        log.warning("no data pages -> exit")
        return
    log.info("total UUID pages: %d", len(all_uuids))

    # 3) грузим zip-отчёты -------------------------------------------------------
    frames = []
    for uid in all_uuids:
        wait_ready(uid, token, perf_client_id, perf_client_secret)
        r = requests.get(REPORT_EP, headers=auth_headers(token),
                         params={"UUID": uid}, timeout=300)
        r.raise_for_status()
        if "application/zip" not in r.headers.get("Content-Type", ""):
            log.warning("UUID %s returned not-zip, skip", uid)
            continue
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        for fn in zf.namelist():
            with zf.open(fn) as fd:
                try:
                    df = pd.read_csv(io.TextIOWrapper(fd, "utf-8"),
                                     sep=";", skiprows=0,
                                     usecols=["День", "sku", "Расход, ₽, с НДС"])
                    frames.append(df)
                except Exception as e:
                    log.debug("file %s ignored (%s)", fn, e)

    if not frames:
        log.warning("no csv rows parsed")
        return

    # 4) агрегируем --------------------------------------------------------------
    df = pd.concat(frames, ignore_index=True)
    df = df[df["День"] != "Всего"]
    df = df[df["sku"].notna()]

    df["date"] = pd.to_datetime(df["День"], dayfirst=True).dt.strftime("%d.%m.%Y")
    df["sku"]  = pd.to_numeric(df["sku"], errors="coerce").astype("Int64")
    df["rub"]  = pd.to_numeric(df["Расход, ₽, с НДС"].str.replace(",", "."),
                               errors="coerce").fillna(0.0)

    grouped = (df.groupby(["date", "sku"], as_index=False)["rub"].sum())
    log.info("rows after groupby: %d", len(grouped))

    # 5) Sheets -----------------------------------------------------------------
    creds = Credentials.from_service_account_file(
        gs_cred,
        scopes=["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"]
    )
    sh = gspread.authorize(creds).open_by_key(spread_id)
    ws = sh.worksheet(sheet_main)

    sheet_vals = ws.get_all_values()
    if not sheet_vals:
        log.warning("sheet '%s' empty – nothing to update", sheet_main)
        return

    head = sheet_vals[0]
    try:
        col_date = head.index("Дата обновления")
        col_sku  = head.index("SKU")
        col_adv  = head.index("Расходы на рекламу")
    except ValueError as e:
        raise RuntimeError(f"sheet '{sheet_main}': {e}")

    # строим map (date,sku) → row_idx
    positions = {}
    for ridx, row in enumerate(sheet_vals[1:], start=2):
        if len(row) <= max(col_date, col_sku):
            continue
        d = (row[col_date].split(" ")[0]).strip()
        try:
            s = int(float(row[col_sku]))
        except Exception:
            continue
        positions[(d, s)] = ridx

    batch = []
    for _, r in grouped.iterrows():
        key = (r["date"], int(r["sku"]))
        ridx = positions.get(key)
        if not ridx:
            continue            # продаж ещё нет → строка появится позже
        batch.append({"range": f"{gspread.utils.rowcol_to_a1(ridx, col_adv+1)}",
                      "values": [[round(r["rub"], 2)]]})

    if batch:
        ws.batch_update(batch, value_input_option="USER_ENTERED")
        log.info("updated %d cells in column F", len(batch))
    else:
        log.info("nothing to update in column F")
