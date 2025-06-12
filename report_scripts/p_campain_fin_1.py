"""
report_scripts/p_campain_fin_1.py
─────────────────────────────────
Заполняет столбец F «Расходы на рекламу» в unit-day
по данным Performance API Ozon.

Параметры run() пробрасывает core/tasks/report_runner:

run(
    gs_cred            = '/path/sa.json',
    spread_id          = '1AbCdE…',
    sheet_main         = 'unit-day',
    perf_client_id     = '…@advertising.performance.ozon.ru',
    perf_client_secret = 'xxxxxxxx',
    days               = 7         # глубина, по-умолчанию
)
"""
from __future__ import annotations

import io
import time
import zipfile
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Callable

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

API = "https://api-performance.ozon.ru"
UTC = timezone.utc


# ──────────────────────────── helpers ────────────────────────────
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def sleep_progress(sec: int):
    for _ in range(sec):
        time.sleep(1)
        print(".", end="", flush=True)
    print()


def get_token(session: requests.Session, cid: str, secret: str) -> tuple[str, datetime]:
    r = session.post(
        f"{API}/api/client/token",
        json={"client_id": cid, "client_secret": secret, "grant_type": "client_credentials"},
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        timeout=None,
    )
    r.raise_for_status()
    token = r.json()["access_token"]
    log(f"token OK ({token[:10]}…)")
    return token, datetime.now(UTC)


def ensure_token(session: requests.Session,
                 token_time: datetime,
                 cid: str,
                 secret: str,
                 headers_cb: Callable[[dict], None]) -> datetime:
    """Обновляет токен, если прошло >25 мин."""
    if (datetime.now(UTC) - token_time).total_seconds() <= 1500:
        return token_time
    new, t = get_token(session, cid, secret)
    headers_cb({"Authorization": f"Bearer {new}"})
    return t


def chunk(lst: list, n: int = 10):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ─────────────────── работа с Performance API ────────────────────
def fetch_campaigns(session: requests.Session, headers: dict) -> list[str]:
    r = session.get(f"{API}/api/client/campaign", headers=headers, timeout=None)
    r.raise_for_status()
    good = {"CAMPAIGN_STATE_RUNNING", "CAMPAIGN_STATE_STOPPED", "CAMPAIGN_STATE_INACTIVE"}
    ids = [str(c["id"]) for c in r.json()["list"] if c.get("state") in good]
    log(f"campaigns: {ids}")
    return ids


def post_statistics(session: requests.Session,
                    headers: dict,
                    camp_ids: list[str],
                    date_from: str,
                    date_to: str) -> str:
    while True:
        r = session.post(
            f"{API}/api/client/statistics",
            headers=headers,
            json={"campaigns": camp_ids,
                  "dateFrom": date_from,
                  "dateTo":   date_to,
                  "groupBy":  "DATE"},
            timeout=None)
        if r.status_code == 429:
            log("⚠️ 429 – ждём 70 с")
            sleep_progress(70)
            continue
        r.raise_for_status()
        return r.json()["UUID"]


def wait_uuid(session: requests.Session,
              uuid: str,
              headers_fn: Callable[[], dict],
              refresh_token: Callable[[], None]):
    url = f"{API}/api/client/statistics/{uuid}"
    while True:
        refresh_token()
        r = session.get(url, headers=headers_fn(), timeout=None)
        if r.status_code == 429:
            log("⚠️ 429 UUID-poll – ждём 70 с")
            sleep_progress(70)
            continue
        r.raise_for_status()
        state = r.json().get("state")
        log(f"uuid {uuid} → {state}")
        if state == "OK":
            return
        if state == "FAILED":
            raise RuntimeError(f"uuid {uuid} FAILED")
        time.sleep(60)


def download_zip(session: requests.Session, headers: dict, uuid: str) -> bytes:
    while True:
        r = session.get(f"{API}/api/client/statistics/report",
                        headers=headers, params={"UUID": uuid}, timeout=None)
        if r.status_code == 429:
            log("⚠️ 429 ZIP – ждём 70 с")
            sleep_progress(70)
            continue
        r.raise_for_status()
        if "application/zip" not in r.headers.get("Content-Type", ""):
            raise RuntimeError("не ZIP")
        return r.content


# ─────────────────────── CSV → DataFrame ────────────────────────
# локальный скрипт жёстко ожидал русские заголовки;
# добавляем fallback-наборы «Day / Ad spend, ₽ incl. VAT» и др.
USECOL_SETS = [
    ["День", "sku", "Расход, ₽, с НДС"],
    ["Day", "sku", "Spend, ₽ incl. VAT"],
    ["Day", "sku", "Расход, ₽, с НДС"],
    ["День", "sku", "Spend, ₽ incl. VAT"],
]


def read_csv_flexible(buf) -> pd.DataFrame | None:
    for cols in USECOL_SETS:
        try:
            df = pd.read_csv(buf, sep=";", skiprows=1, usecols=cols)
            df.columns = ["date", "sku", "rub"]
            return df
        except ValueError:
            continue
    return None


def parse_zip(data: bytes) -> pd.DataFrame:
    dfs = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for fn in zf.namelist():
            if not fn.lower().endswith((".csv", ".txt")):
                continue
            df = read_csv_flexible(io.TextIOWrapper(zf.open(fn), "utf-8"))
            if df is None:
                continue
            df = df[df["date"] != "Всего"]
            df["sku"] = pd.to_numeric(df["sku"], errors="coerce").dropna().astype(int)
            df["rub"] = pd.to_numeric(df["rub"].astype(str).str.replace(",", "."),
                                      errors="coerce").fillna(0.0)
            dfs.append(df)

    if not dfs:
        return pd.DataFrame(columns=["date", "sku", "rub"])

    out = pd.concat(dfs, ignore_index=True)
    out = out.groupby(["date", "sku"], as_index=False)["rub"].sum()
    return out


# ─────────────────────── запись в Google Sheets ──────────────────────
def write_sheet(gs_cred: str, spread_id: str, sheet: str, df: pd.DataFrame):
    creds = Credentials.from_service_account_file(
        gs_cred,
        scopes=["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"]
    )
    ws = gspread.authorize(creds).open_by_key(spread_id).worksheet(sheet)
    rows = ws.get_all_values()
    if not rows:
        log("⚠️ Лист пуст, ничего записывать")
        return

    head = rows[0]
    try:
        col_date = head.index("Дата обновления")
        col_sku  = head.index("SKU")
    except ValueError as e:
        raise RuntimeError("unit-day без нужных колонок") from e

    # index → key
    sheet_map = defaultdict(list)
    for i, r in enumerate(rows[1:], start=2):
        key = (r[col_date].split(" ")[0], str(r[col_sku]))
        sheet_map[key].append(i)

    updates = []
    for _, r in df.iterrows():
        for row_idx in sheet_map.get((r["date"], str(r["sku"])), []):
            updates.append({"range": f"F{row_idx}", "values": [[round(r["rub"], 2)]]})

    if updates:
        ws.batch_update(updates)
        log(f"🟢 Записано {len(updates)} ячеек")
    else:
        log("ℹ️ Совпадений (date+sku) не найдено")


# ──────────────────────────── run() ────────────────────────────
def run(
    *,
    gs_cred: str,
    spread_id: str,
    sheet_main: str = "unit-day",
    perf_client_id: str,
    perf_client_secret: str,
    days: int = 7,
):
    session = requests.Session()

    token, token_time = get_token(session, perf_client_id, perf_client_secret)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    # функция-замыкание для «прокидывания» новых header’ов
    def set_headers(d: dict):
        headers.update(d)

    def hdr() -> dict:
        return headers

    # 1. кампании
    cids = fetch_campaigns(session, hdr())

    # 2. даты интервала
    msk_now = datetime.now(UTC) + timedelta(hours=3)
    date_to = msk_now.date()
    date_from = (msk_now - timedelta(days=days)).date()
    df_str, dt_str = date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d")

    # 3. собираем UUID
    uuids: list[str] = []
    for ch in chunk(cids, 10):  # 10 как в локальном
        uuid = post_statistics(session, hdr(), ch, df_str, dt_str)
        log(f"uuid {uuid} OK → wait")
        wait_uuid(
            session,
            uuid,
            hdr,
            lambda: set_headers(
                {"Authorization": f"Bearer {get_token(session, perf_client_id, perf_client_secret)[0]}"}
            ),
        )
        uuids.append(uuid)
        time.sleep(1)  # мягкий RPS-limit

    # 4. скачиваем ZIP-ы, агрегируем
    df_total = pd.DataFrame(columns=["date", "sku", "rub"])
    for u in uuids:
        df_total = pd.concat(
            [df_total, parse_zip(download_zip(session, hdr(), u))],
            ignore_index=True,
        )

    if df_total.empty:
        log("ℹ️ Нет строк для записи → завершено")
        return

    # 5. Google Sheets
    write_sheet(gs_cred, spread_id, sheet_main, df_total)
    log("✅ p_campain_fin_1 DONE")
