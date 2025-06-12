"""
report_scripts/p_campain_fin_1.py
─────────────────────────────────
Обновляет столбец «F = Расходы на рекламу» в листе unit-day
по данным Performance API Ozon.

run(
    gs_cred            ='/path/sa.json',
    spread_id          ='1AbCdE…',
    sheet_main         ='unit-day',      # где лежит отчёт
    perf_client_id     ='…@advertising.performance.ozon.ru',
    perf_client_secret ='xxxxxxxx',
    days               = 7               # глубина выборки
)
"""
from __future__ import annotations

import io
import time
import zipfile
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

# ───────────────────────── helpers ──────────────────────────
API_HOST = "https://api-performance.ozon.ru"
TZ_UTC   = timezone.utc


def log(msg: str):
    """Мини-логгер (stdout попадает в journalctl)."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def backoff_sleep(sec: int):
    """Спит с выводом секундомера (чтобы в Telegram было видно «живой» лог)."""
    for _ in range(sec):
        time.sleep(1)
        print(".", end="", flush=True)
    print()


def get_token(client_id: str, client_secret: str) -> tuple[str, datetime]:
    r = requests.post(
        f"{API_HOST}/api/client/token",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        },
        timeout=None,                 # как в локальном скрипте
    )
    r.raise_for_status()
    token = r.json()["access_token"]
    log(f"PerformanceAPI token OK ({token[:10]}…)")

    return token, datetime.now(TZ_UTC)


def chunk(lst: list, size: int = 10):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def fetch_campaign_ids(session: requests.Session, headers: dict) -> list[str]:
    r = session.get(f"{API_HOST}/api/client/campaign", headers=headers, timeout=None)
    r.raise_for_status()
    target_states = {
        "CAMPAIGN_STATE_RUNNING",
        "CAMPAIGN_STATE_STOPPED",
        "CAMPAIGN_STATE_INACTIVE",
    }
    ids = [str(c["id"]) for c in r.json()["list"] if c.get("state") in target_states]
    log(f"campaigns: {ids}")
    return ids


def stats_request(
    session: requests.Session,
    headers: dict,
    camp_ids: list[str],
    date_from: str,
    date_to: str,
) -> str:
    """Отправляем запрос → получаем UUID. Обработка 429."""
    while True:
        r = session.post(
            f"{API_HOST}/api/client/statistics",
            headers=headers,
            json={
                "campaigns": camp_ids,
                "dateFrom": date_from,
                "dateTo": date_to,
                "groupBy": "DATE",
            },
            timeout=None,
        )
        if r.status_code == 429:
            log("⚠️ 429 Too Many Requests – ждём 65 с…")
            backoff_sleep(65)
            continue
        r.raise_for_status()
        uuid = r.json()["UUID"]
        return uuid


def wait_uuid(
    session: requests.Session,
    headers_fn,
    uuid: str,
    token_expiry: datetime,
    client_id: str,
    client_secret: str,
    poll_sec: int = 60,
):
    """Ожидаем готовности UUID. Перезапрашиваем токен при необходимости."""
    url = f"{API_HOST}/api/client/statistics/{uuid}"
    while True:
        # истёк ли токен (±25 минут)?
        if (datetime.now(TZ_UTC) - token_expiry).total_seconds() > 1500:
            new_token, token_expiry = get_token(client_id, client_secret)
            headers_fn({"Authorization": f"Bearer {new_token}"})

        r = session.get(url, headers=headers_fn(), timeout=None)
        if r.status_code == 429:
            log("⚠️ 429 на UUID-check – ждём 65 с…")
            backoff_sleep(65)
            continue
        r.raise_for_status()
        state = r.json()["state"]
        log(f"uuid {uuid} → {state}")

        if state == "OK":
            return
        if state == "FAILED":
            raise RuntimeError(f"uuid {uuid} FAILED")

        time.sleep(poll_sec)


def fetch_report_file(session: requests.Session, headers: dict, uuid: str) -> bytes:
    """Скачиваем ZIP-отчёт."""
    r = session.get(
        f"{API_HOST}/api/client/statistics/report",
        headers=headers,
        params={"UUID": uuid},
        timeout=None,
    )
    if r.status_code == 429:
        log("⚠️ 429 при скачивании ZIP – ждём 65 с…")
        backoff_sleep(65)
        return fetch_report_file(session, headers, uuid)

    r.raise_for_status()
    if "application/zip" not in r.headers.get("Content-Type", ""):
        raise RuntimeError(f"uuid {uuid}: не ZIP")
    return r.content


def parse_zip(data: bytes) -> pd.DataFrame:
    """Читаем все CSV внутри ZIP, возвращаем df с колонками date, sku, rub."""
    rows = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for fn in zf.namelist():
            if not (fn.endswith(".csv") or fn.endswith(".txt")):
                continue
            df = pd.read_csv(
                io.TextIOWrapper(zf.open(fn), "utf-8"),
                sep=";",
                skiprows=1,
                usecols=["День", "sku", "Расход, ₽, с НДС"],
            )
            df = df[df["День"] != "Всего"]
            df["sku"] = pd.to_numeric(df["sku"], errors="coerce").dropna().astype(int)
            df["Расход, ₽, с НДС"] = (
                df["Расход, ₽, с НДС"].astype(str).str.replace(",", ".").astype(float)
            )
            rows.append(df)

    if not rows:
        return pd.DataFrame(columns=["date", "sku", "rub"])

    df_all = pd.concat(rows, ignore_index=True)
    df_all = (
        df_all.groupby(["День", "sku"], as_index=False)["Расход, ₽, с НДС"]
        .sum()
        .rename(columns={"День": "date", "Расход, ₽, с НДС": "rub"})
    )
    return df_all


def write_to_sheet(
    gs_cred: str,
    spread_id: str,
    sheet_name: str,
    df_rub: pd.DataFrame,
):
    """Записываем суммы в столбец F, сопоставляя (Дата, SKU)."""
    creds = Credentials.from_service_account_file(
        gs_cred,
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    ws = (
        gspread.authorize(creds)
        .open_by_key(spread_id)
        .worksheet(sheet_name)
    )

    values = ws.get_all_values()
    if not values:
        log("⚠️ Лист пуст – ничего записывать")
        return

    head = values[0]
    try:
        col_date = head.index("Дата обновления")
        col_sku  = head.index("SKU")
        col_rub  = 5  # столбец F
    except ValueError as e:
        raise RuntimeError("В unit-day нет нужных столбцов") from e

    # строим мапу (date, sku) → index row_in_sheet
    pos = defaultdict(list)
    for i, row in enumerate(values[1:], start=2):  # строки в GS – с 2
        d = (row[col_date].split(" ")[0] if row[col_date] else "", row[col_sku])
        pos[d].append(i)

    updates = []
    for _, r in df_rub.iterrows():
        key = (r["date"], str(r["sku"]))
        for row_idx in pos.get(key, []):
            updates.append(
                {
                    "range": f"F{row_idx}",
                    "values": [[round(r["rub"], 2)]],
                }
            )

    if updates:
        ws.batch_update(updates)
        log(f"🟢 Записано {len(updates)} ячеек в unit-day (столбец F)")
    else:
        log("ℹ️ Совпадений (дата+sku) не найдено – ничего не записано")


# ──────────────────────────── run() ─────────────────────────
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

    # 1. токен
    token, token_time = get_token(perf_client_id, perf_client_secret)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    def headers_proxy(new: dict | None = None):
        """callback для подмены Authorization при обновлении токена"""
        if new:
            headers.update(new)
        return headers

    # 2. кампании
    camp_ids = fetch_campaign_ids(session, headers)

    # 3. даты
    now_msk = datetime.now(TZ_UTC) + timedelta(hours=3)
    date_to = now_msk.date()
    date_from = (now_msk - timedelta(days=days)).date()
    df_str, dt_str = date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d")

    # 4. собираем UUID-ы (с учётом лимитов)
    all_uuids: list[str] = []
    for ch in chunk(camp_ids, 10):  # как в локальном скрипте
        uuid = stats_request(session, headers, ch, df_str, dt_str)
        log(f"uuid {uuid} OK → wait")
        wait_uuid(
            session,
            headers_proxy,
            uuid,
            token_time,
            perf_client_id,
            perf_client_secret,
            poll_sec=60,
        )
        all_uuids.append(uuid)
        time.sleep(1)  # мягкий глобальный RPS-лимит

    # 5. скачиваем ZIP-ы и консолидируем
    df_total = pd.DataFrame(columns=["date", "sku", "rub"])
    for u in all_uuids:
        zip_bytes = fetch_report_file(session, headers, u)
        df = parse_zip(zip_bytes)
        df_total = pd.concat([df_total, df], ignore_index=True)

    if df_total.empty:
        log("⚠️ Performance-данных нет – выход")
        return

    # 6. пишем в Sheets
    write_to_sheet(gs_cred, spread_id, sheet_main, df_total)
    log("✅ p_campain_fin_1 DONE")
