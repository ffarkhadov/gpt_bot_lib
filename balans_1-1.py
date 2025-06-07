"""
Google-таблица «balans_1»

"""

# ---------------------------------------------------------------------------
#                               imports
# ---------------------------------------------------------------------------
import requests, pandas as pd, pytz, gspread
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from collections import defaultdict
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------------
#                               credentials
# ---------------------------------------------------------------------------
TOKEN_OZ  = "102efb35-db8d-4552-b6fa-75c0a66ce11d"
CLIENT_ID = "2567268"

CREDS_FILE      = r"E:/Ozon_API/teak-digit-438912-s0-cc0207cd6ee3.json"
SPREADSHEET_ID  = "1pkR_vV-g0cI8AUdQEAw2VTS4tBe78t5gD-ExqUnrpfo"
WORKSHEET_NAME  = "balans_1"

HEADERS = {"Client-Id": CLIENT_ID, "Api-Key": TOKEN_OZ}

# ---------------------------------------------------------------------------
#                               utils
# ---------------------------------------------------------------------------
def col_letter(n: int) -> str:
    """1 → A  • 27 → AA …"""
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def iso_utc(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ---------------------------------------------------------------------------
#                      1. Свободный остаток  (stock_on_warehouses)
# ---------------------------------------------------------------------------
def fetch_free_stock() -> dict[str, int]:
    url  = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    pay  = {"limit": 1000, "offset": 0, "warehouse_type": "ALL"}
    res  = requests.post(url, headers=HEADERS, json=pay).json()["result"]["rows"]

    if not res:
        return {}

    df   = pd.DataFrame(res)
    free = (df.groupby("sku", as_index=False)["free_to_sell_amount"]
              .sum().rename(columns={"free_to_sell_amount": "free"}))
    return dict(zip(free["sku"].astype(str), free["free"]))


# ---------------------------------------------------------------------------
#                2. Возвраты  (returns/list → две агрегированные колонки)
# ---------------------------------------------------------------------------
RETURNS_STATUS_MAP = {
    "Утилизирован"     : "Утиль/Возврат",
    "Уже у вас"        : "Утиль/Возврат",
    "Едет к вам"       : "Утиль/Возврат",
    "Едет на склад Ozon": "Едет на склад Ozon",
    "Ожидает отправки" : "Едет на склад Ozon",
}

def fetch_returns() -> dict[str, dict[str, int]]:
    url   = "https://api-seller.ozon.ru/v1/returns/list"
    limit = 500
    last  = 0
    totals = defaultdict(lambda: defaultdict(int))

    while True:
        payload = {"filter": {}, "limit": limit, "last_id": last}
        resp    = requests.post(url, headers=HEADERS, json=payload).json()
        rets    = resp.get("returns", [])
        if not rets:
            break

        for r in rets:
            sku       = str(r.get("product", {}).get("sku", "UNKNOWN"))
            qty       = int(r.get("product", {}).get("quantity", 0))
            display   = r.get("visual", {}).get("status", {}).get("display_name")
            group     = RETURNS_STATUS_MAP.get(display)
            if group:
                totals[sku][group] += qty

        last = rets[-1].get("id", 0)
        if len(rets) < limit:
            break

    return totals      # {sku: {"Утиль/Возврат": x, "Едет на склад Ozon": y}}


# ---------------------------------------------------------------------------
#               3. Постинги FBO (объединяем «В пути», убираем cancelled)
# ---------------------------------------------------------------------------
STATUS_ALIAS = {
    "awaiting_deliver"  : "В пути",
    "awaiting_packaging": "В пути",
    "delivering"        : "В пути",
}

def fetch_fbo():
    url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
    tz  = pytz.timezone("Europe/Moscow")
    now, since = datetime.now(tz), datetime.now(tz) - relativedelta(years=1)

    base = {
        "dir": "ASC",
        "filter": {"since": iso_utc(since), "to": iso_utc(now), "status": ""},
        "limit": 1000,
        "translit": True,
        "with": {"analytics_data": True, "financial_data": True},
    }

    total, offset = [], 0
    while True:
        chunk = requests.post(url, headers=HEADERS,
                              json={**base, "offset": offset}).json()["result"]
        total += chunk
        if len(chunk) < base["limit"]:
            return total
        offset += base["limit"]


def pivot_statuses(postings):
    rows, statuses = defaultdict(lambda: defaultdict(int)), set()
    for p in postings:
        raw = p["status"]
        if raw == "cancelled":
            continue
        st = STATUS_ALIAS.get(raw, raw)

        for pr in p.get("products", []):
            sku, qty = str(pr["sku"]), int(pr.get("quantity", 0))
            rows[sku][st] += qty
            statuses.add(st)
    return rows, statuses


# ---------------------------------------------------------------------------
#                       4. Supply → название + поставка
# ---------------------------------------------------------------------------
def supply_lookup():
    ids = requests.post(
        "https://api-seller.ozon.ru/v2/supply-order/list",
        headers=HEADERS,
        json={"filter": {"states": ["ORDER_STATE_COMPLETED"]},
              "paging": {"from_supply_order_id": 0, "limit": 100}}
    ).json().get("supply_order_id", [])
    if not ids:
        return {}

    orders = requests.post(
        "https://api-seller.ozon.ru/v2/supply-order/get",
        headers=HEADERS, json={"order_ids": ids}
    ).json().get("orders", [])

    bundle_ids = [s["bundle_id"] for o in orders for s in o["supplies"]
                  if s.get("supply_state") == "SUPPLY_STATE_COMPLETED" and s.get("bundle_id")]

    if not bundle_ids:
        return {}

    items = requests.post(
        "https://api-seller.ozon.ru/v1/supply-order/bundle",
        headers=HEADERS,
        json={"bundle_ids": bundle_ids, "is_asc": True,
              "limit": 100, "query": "", "sort_field": "UNSPECIFIED"}
    ).json().get("items", [])

    return {str(it["sku"]): (it["offer_id"], it["quantity"]) for it in items}


# ---------------------------------------------------------------------------
#                       5. Google-Sheets writer
# ---------------------------------------------------------------------------
def save_sheet(date_disp, rows_dict, statuses,
               free_map, returns_map, supply_map):
    creds = Credentials.from_service_account_file(
        CREDS_FILE,
        scopes=["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"]
    )
    ws = gspread.authorize(creds).open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)

    base_hdr = ["Дата обновления", "SKU", "Наименование", "Поставка",
                "Свободный остаток", "Утиль/Возврат", "Едет на склад Ozon"]
    hdr = ws.row_values(1) or []
    for col in base_hdr:
        if col not in hdr:
            hdr.append(col)
    for st in sorted(statuses):
        if st not in hdr:
            hdr.append(st)

    if hdr != ws.row_values(1):
        ws.update("A1", [hdr])

    sheet   = ws.get_all_values()
    idx     = {row[1]: i for i, row in enumerate(sheet[1:], start=2)}  # key=SKU
    lastcol = col_letter(len(hdr))

    base_len = len(base_hdr)
    updates, appends = [], []

    for sku, st_map in rows_dict.items():
        # базовые данные
        name, supp = supply_map.get(sku, ("", ""))
        free_qty   = free_map.get(sku, "")
        util_qty   = returns_map.get(sku, {}).get("Утиль/Возврат", "")
        towh_qty   = returns_map.get(sku, {}).get("Едет на склад Ozon", "")

        row = [""] * len(hdr)
        row[:7] = [date_disp, sku, name, supp,
                   free_qty, util_qty, towh_qty]

        # статусы заказов
        for st, qty in st_map.items():
            row[hdr.index(st)] = qty

        if sku in idx:          # UPDATE
            r = idx[sku]
            updates.append({"range": f"A{r}:{lastcol}{r}", "values": [row]})
        else:                   # APPEND
            appends.append(row)

    if updates:
        ws.batch_update(updates)
    if appends:
        ws.append_rows(appends, value_input_option="USER_ENTERED")


# ---------------------------------------------------------------------------
#                                   MAIN
# ---------------------------------------------------------------------------
def main():
    free_map    = fetch_free_stock()
    returns_map = fetch_returns()
    supply_map  = supply_lookup()

    postings   = fetch_fbo()
    rows_dict, statuses = pivot_statuses(postings)

    date_disp = datetime.now(
        pytz.timezone("Europe/Moscow")).strftime("%d.%m.%Y (%H:%M)")

    save_sheet(date_disp, rows_dict, statuses,
               free_map, returns_map, supply_map)

    print("✅ Таблица обновлена: добавлены «В пути», возвраты и свободный остаток.")


if __name__ == "__main__":
    main()
