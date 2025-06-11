"""
balans_1.py  ─ динамический
Обновляет лист «balans_1»:
  • свободный остаток   (stock_on_warehouses)
  • возвраты            (returns/list)
  • статусы FBO-постингов (в пути, доставлен и т.д.)
  • последняя поставка + свободный остаток

Вызов:
   run(token_oz, client_id, gs_cred, spread_id, worksheet="balans_1")
"""

from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict

# ───────── helpers ─────────
def col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def iso_utc(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ───────── main entry ─────────
def run(*, token_oz: str, client_id: str,
        gs_cred: str, spread_id: str,
        worksheet: str = "balans_1") -> None:

    # локальные импорты (имён достаточно внутри функции)
    import requests, gspread, pytz
    import pandas as pd
    from dateutil.relativedelta import relativedelta
    from google.oauth2.service_account import Credentials

    headers = {"Client-Id": client_id, "Api-Key": token_oz}
    tz_msk = pytz.timezone("Europe/Moscow")
    now_msk = datetime.now(tz_msk)
    date_disp = now_msk.strftime("%d.%m.%Y (%H:%M)")

    # ------------------------------------------------------------------
    # 1. Свободный остаток
    # ------------------------------------------------------------------
    def fetch_free_stock() -> dict[str, int]:
        url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
        pay = {"limit": 1000, "offset": 0, "warehouse_type": "ALL"}
        rows = requests.post(url, headers=headers, json=pay, timeout=60) \
                       .json()["result"]["rows"]
        if not rows:
            return {}
        df = pd.DataFrame(rows)
        free = (df.groupby("sku", as_index=False)["free_to_sell_amount"]
                  .sum().rename(columns={"free_to_sell_amount": "free"}))
        return dict(zip(free["sku"].astype(str), free["free"]))

    # ------------------------------------------------------------------
    # 2. Возвраты
    # ------------------------------------------------------------------
    RETURNS_MAP = {
        "Утилизирован": "Утиль/Возврат",
        "Уже у вас": "Утиль/Возврат",
        "Едет к вам": "Утиль/Возврат",
        "Едет на склад Ozon": "Едет на склад Ozon",
        "Ожидает отправки": "Едет на склад Ozon",
    }

    def fetch_returns() -> dict[str, dict[str, int]]:
        url, limit, last = "https://api-seller.ozon.ru/v1/returns/list", 500, 0
        totals: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

        while True:
            payload = {"filter": {}, "limit": limit, "last_id": last}
            rets = requests.post(url, headers=headers, json=payload, timeout=60) \
                           .json().get("returns", [])
            if not rets:
                break

            for r in rets:
                sku = str(r.get("product", {}).get("sku", "UNKNOWN"))
                qty = int(r.get("product", {}).get("quantity", 0))
                disp = r.get("visual", {}).get("status", {}).get("display_name")
                group = RETURNS_MAP.get(disp)
                if group:
                    totals[sku][group] += qty
            last = rets[-1].get("id", 0)
            if len(rets) < limit:
                break
        return totals

    # ------------------------------------------------------------------
    # 3. Постинги FBO
    # ------------------------------------------------------------------
    STATUS_ALIAS = {
        "awaiting_deliver": "В пути",
        "awaiting_packaging": "В пути",
        "delivering": "В пути",
    }

    def fetch_fbo():
        url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
        since = now_msk - relativedelta(years=1)
        base = {
            "dir": "ASC",
            "filter": {"since": iso_utc(since), "to": iso_utc(now_msk), "status": ""},
            "limit": 1000,
            "translit": True,
            "with": {"analytics_data": True, "financial_data": True},
        }
        total, offset = [], 0
        while True:
            chunk = requests.post(url, headers=headers,
                                  json={**base, "offset": offset}, timeout=60).json()["result"]
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

    # ------------------------------------------------------------------
    # 4. Supply lookup
    # ------------------------------------------------------------------
    def supply_lookup():
        ids = requests.post(
            "https://api-seller.ozon.ru/v2/supply-order/list",
            headers=headers,
            json={"filter": {"states": ["ORDER_STATE_COMPLETED"]},
                  "paging": {"from_supply_order_id": 0, "limit": 100}},
            timeout=60,
        ).json().get("supply_order_id", [])
        if not ids:
            return {}

        orders = requests.post(
            "https://api-seller.ozon.ru/v2/supply-order/get",
            headers=headers,
            json={"order_ids": ids}, timeout=60).json().get("orders", [])

        bundle_ids = [s["bundle_id"] for o in orders for s in o["supplies"]
                      if s.get("supply_state") == "SUPPLY_STATE_COMPLETED" and s.get("bundle_id")]

        if not bundle_ids:
            return {}

        items = requests.post(
            "https://api-seller.ozon.ru/v1/supply-order/bundle",
            headers=headers,
            json={"bundle_ids": bundle_ids, "is_asc": True,
                  "limit": 100, "query": "", "sort_field": "UNSPECIFIED"},
            timeout=60,
        ).json().get("items", [])

        return {str(it["sku"]): (it["offer_id"], it["quantity"]) for it in items}

    # ------------------------------------------------------------------
    # 5. Google Sheets writer
    # ------------------------------------------------------------------
    def save_sheet(rows_dict, statuses,
                   free_map, returns_map, supply_map):

        creds = Credentials.from_service_account_file(
            gs_cred,
            scopes=["https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive"])
        sh = gspread.authorize(creds).open_by_key(spread_id)
        ws = (sh.worksheet(worksheet)
              if worksheet in [w.title for w in sh.worksheets()]
              else sh.add_worksheet(worksheet, rows=1000, cols=30))

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

        sheet = ws.get_all_values()
        idx = {row[1]: i for i, row in enumerate(sheet[1:], start=2)}
        lastcol = col_letter(len(hdr))

        updates, appends = [], []

        for sku, st_map in rows_dict.items():
            name, supp = supply_map.get(sku, ("", ""))
            free_qty = free_map.get(sku, "")
            util_qty = returns_map.get(sku, {}).get("Утиль/Возврат", "")
            towh_qty = returns_map.get(sku, {}).get("Едет на склад Ozon", "")

            row = [""] * len(hdr)
            row[:7] = [date_disp, sku, name, supp,
                       free_qty, util_qty, towh_qty]

            for st, qty in st_map.items():
                row[hdr.index(st)] = qty

            if sku in idx:
                r = idx[sku]
                updates.append({"range": f"A{r}:{lastcol}{r}", "values": [row]})
            else:
                appends.append(row)

        if updates:
            ws.batch_update(updates)
        if appends:
            ws.append_rows(appends, value_input_option="USER_ENTERED")

    # ------------------------------------------------------------------
    # main pipeline
    # ------------------------------------------------------------------
    free_map = fetch_free_stock()
    returns_map = fetch_returns()
    supply_map = supply_lookup()
    fbo = fetch_fbo()
    rows_dict, statuses = pivot_statuses(fbo)

    save_sheet(rows_dict, statuses, free_map, returns_map, supply_map)
    print("[balans_1] ✅ таблица обновлена")
