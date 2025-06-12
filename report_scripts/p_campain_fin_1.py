"""
report_scripts/p_campain_fin_1.py
---------------------------------
Обновляет столбец F («Расходы на рекламу») в листе unit-day.

run(
    gs_cred             = '/path/sa.json',
    spread_id           = '1AbCdE...',
    perf_client_id      = '…@advertising.performance.ozon.ru',
    perf_client_secret  = '******',
    sheet_main          = 'unit-day',   # опционально
    days                = 7            # глубина отчёта
)
"""
from __future__ import annotations

import os, io, time, json, zipfile, requests, pandas as pd
from typing     import Iterable
from datetime   import datetime, timezone, timedelta
from itertools  import islice
from pathlib    import Path
from google.oauth2.service_account import Credentials
import gspread
import logging

log = logging.getLogger(__name__)

# ──────────────────── настройки/константы ────────────────────
API_HOST       = "https://api-performance.ozon.ru"
TOKEN_ENDPOINT = "/api/client/token"
STATS_ENDP     = "/api/client/statistics"
REPORT_ENDP    = "/api/client/statistics/report"

CHUNK_SIZE     = 8          # кампаний в одном запросе
WAIT_SEC       = 60         # пауза между polls UUID
MAX_PAGES      = 10         # подстраховка от бесконечного цикла
TIMEOUT        = 40         # requests timeout

USECOL_SETS = [
    ["День", "sku", "Расход, ₽, с НДС"],
    ["Day", "sku", "Spend, ₽ incl. VAT"],
    ["Day", "sku", "Spend, ₽"],
]

# ──────────────────── helpers ────────────────────
def grouper(it: Iterable, n: int):
    """([1,2,3,4,5], 2) → (1,2) (3,4) (5,)"""
    it = iter(it)
    while (chunk := tuple(islice(it, n))):
        yield chunk


def best_usecols(cols: list[str]) -> list[str]:
    for st in USECOL_SETS:
        if set(st).issubset(cols):
            return st
    raise ValueError("Не найден подходящий набор колонок для {}".format(cols))


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def parse_zip(b: bytes, debug_sku: str | None = None) -> pd.DataFrame:
    """ZIP → DataFrame(date, sku, rub)"""
    dfs = []
    with zipfile.ZipFile(io.BytesIO(b)) as zf:
        for fn in zf.namelist():
            with zf.open(fn) as f:
                df = pd.read_csv(
                    io.TextIOWrapper(f, "utf-8"), sep=";", skiprows=1
                )
            cols = best_usecols(list(df.columns))
            df = df[cols]
            df = df.query("sku.notna() & sku != 'inf' & sku != '-inf'")
            df["sku"] = pd.to_numeric(df["sku"], errors="coerce").astype("Int64")
            df["rub"] = (
                df[cols[2]].astype(str).str.replace(",", ".").astype(float)
            )
            df.rename(columns={cols[0]: "date"}, inplace=True)
            dfs.append(df[["date", "sku", "rub"]])

            # отладочный дамп
            if debug_sku and not df[df["sku"] == int(debug_sku)].empty:
                df[df["sku"] == int(debug_sku)].to_csv(
                    f"/tmp/ads_raw_{debug_sku}.csv", index=False
                )
                log.info("[DEBUG] dump for SKU %s saved to /tmp/ads_raw_%s.csv",
                         debug_sku, debug_sku)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ──────────────────── Performance-API ────────────────────
def get_token(client_id: str, client_secret: str) -> tuple[str, datetime]:
    r = requests.post(
        API_HOST + TOKEN_ENDPOINT,
        json={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        },
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    token = r.json()["access_token"]
    log.info("token OK (%.10s…)", token)
    return token, datetime.now(timezone.utc)


def refresh_if_needed(token: str, t0: datetime,
                      client_id: str, client_secret: str) -> tuple[str, datetime]:
    if (datetime.now(timezone.utc) - t0).total_seconds() < 1500:
        return token, t0
    return get_token(client_id, client_secret)


def wait_uuid(uuid: str, headers: dict[str, str]) -> None:
    url = f"{API_HOST}{STATS_ENDP}/{uuid}"
    for _ in range(MAX_PAGES * 3):               # max ~30 мин
        r = requests.get(url, headers=headers, timeout=TIMEOUT)
        if r.status_code == 404:
            time.sleep(WAIT_SEC)
            continue
        r.raise_for_status()
        state = r.json().get("state")
        log.info("uuid %s → %s", uuid, state)
        if state == "OK":
            return
        if state == "FAILED":
            raise RuntimeError(f"UUID {uuid} FAILED")
        time.sleep(WAIT_SEC)
    raise TimeoutError(f"UUID {uuid} wait timeout")


def fetch_all_pages(
    camp_chunk: tuple[str, ...],
    date_from: str,
    date_to: str,
    token: str,
    client_id: str,
    client_secret: str,
    debug_sku: str | None,
) -> pd.DataFrame:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    # 1) create request
    r = requests.post(
        API_HOST + STATS_ENDP,
        headers=headers,
        json={
            "campaigns": camp_chunk,
            "dateFrom": date_from,
            "dateTo": date_to,
            "groupBy": "DATE",
        },
        timeout=TIMEOUT,
    )
    if r.status_code == 429:
        raise RuntimeError("429 Too Many Requests (create stats)")
    r.raise_for_status()
    uuid = r.json()["UUID"]
    log.info("uuid %s OK → wait", uuid)

    # 2) wait
    wait_uuid(uuid, headers)

    # 3) download archive
    rep = requests.get(
        API_HOST + REPORT_ENDP,
        headers=headers,
        params={"UUID": uuid},
        timeout=TIMEOUT,
    )
    rep.raise_for_status()
    if "application/zip" not in rep.headers.get("Content-Type", ""):
        raise RuntimeError(f"Report for {uuid} is not zip")
    return parse_zip(rep.content, debug_sku)


# ──────────────────── Google Sheets ────────────────────
def write_sheet(df_total: pd.DataFrame,
                gs_cred: str,
                spread_id: str,
                sheet_main: str):
    if df_total.empty:
        log.warning("No data to write")
        return

    creds = Credentials.from_service_account_file(
        gs_cred,
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    ss = gspread.authorize(creds).open_by_key(spread_id)
    ws = ss.worksheet(sheet_main)

    # карта (дата, sku) → индекс строки
    vals = ws.get_all_values()
    header = vals[0]
    idx_date = header.index("Дата обновления")
    idx_sku  = header.index("SKU")
    row_map  = {
        (r[idx_date].split()[0], int(r[idx_sku])): i + 1
        for i, r in enumerate(vals[1:], start=2)  # Google-строки начинаются с 1
    }

    updates = []
    for date, sku, rub in df_total.itertuples(index=False):
        key = (date.split(" ")[0], int(sku))
        row = row_map.get(key)
        if row:
            updates.append([row, rub])

    if not updates:
        log.info("Nothing to update in sheet")
        return

    # формируем пачечный запрос к столбцу F
    body = [
        {"range": f"F{r}:F{r}", "values": [[v]]}
        for r, v in updates
    ]
    # Google API ограничивает batchUpdate ~100 операций ⇒ режем
    for chunk in grouper(body, 90):
        ws.batch_update(chunk)
    log.info("🟢 Записано %d ячеек", len(updates))


# ──────────────────── ENTRYPOINT ────────────────────
def run(*,
        gs_cred: str,
        spread_id: str,
        perf_client_id: str,
        perf_client_secret: str,
        sheet_main: str = "unit-day",
        days: int = 7):

    debug_sku = os.getenv("DEBUG_SKU")

    date_to   = (datetime.now(timezone.utc) + timedelta(hours=3)).date()
    date_from = (date_to - timedelta(days=days-1))

    date_from_s = date_from.strftime("%Y-%m-%d")
    date_to_s   = date_to.strftime("%Y-%m-%d")

    token, ts = get_token(perf_client_id, perf_client_secret)

    # 1) список кампаний
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    r = requests.get(f"{API_HOST}{STATS_ENDP.replace('/statistics','/campaign')}",
                     headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    camps = [
        str(it["id"]) for it in r.json().get("list", [])
        if it.get("state") in {
            "CAMPAIGN_STATE_RUNNING",
            "CAMPAIGN_STATE_STOPPED",
            "CAMPAIGN_STATE_INACTIVE",
        }
    ]
    log.info("campaigns: %s", camps)

    df_total = pd.DataFrame(columns=["date", "sku", "rub"])

    # 2) цикл по чанкам кампаний
    for chunk in grouper(camps, CHUNK_SIZE):
        try:
            token, ts = refresh_if_needed(token, ts,
                                          perf_client_id, perf_client_secret)
            part = fetch_all_pages(chunk, date_from_s, date_to_s,
                                   token, perf_client_id, perf_client_secret,
                                   debug_sku)
            df_total = pd.concat([df_total, part], ignore_index=True)
        except requests.HTTPError as e:
            if e.response.status_code == 429:
                log.warning("429 on chunk %s; sleep %s sec", chunk, WAIT_SEC*2)
                time.sleep(WAIT_SEC * 2)
                continue
            raise

    # 3) агрегация и запись
    if not df_total.empty:
        df_total = (
            df_total.groupby(["date", "sku"], as_index=False)["rub"]
            .sum()
            .round(2)
        )
    write_sheet(df_total, gs_cred, spread_id, sheet_main)
    log.info("✅ p_campain_fin_1 DONE")


# тесты локально:
# if __name__ == "__main__":
#     run(gs_cred="cred.json",
#         spread_id="1xxx",
#         perf_client_id="xxx",
#         perf_client_secret="xxx")
