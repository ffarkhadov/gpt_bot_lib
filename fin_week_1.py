from __future__ import annotations
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import requests
from dateutil import tz
import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
from gspread_formatting import CellFormat, Color, TextFormat, format_cell_range

# ────────────────────  НАСТРОЙКИ  ─────────────────────────────────────────
TOKEN_OZ  = '102efb35-db8d-4552-b6fa-75c0a66ce11d'
CLIENT_ID = '2567268'
URL_FIN   = 'https://api-seller.ozon.ru/v3/finance/transaction/list'
PAGE_SIZE = 1000
BOTTOM_TS = datetime(2022, 1, 1, tzinfo=tz.tzutc())

SPREADSHEET_ID = '1pkR_vV-g0cI8AUdQEAw2VTS4tBe78t5gD-ExqUnrpfo'
OUTPUT_SHEET   = 'week_fin'
INPUT_SHEET    = 'input'
CREDS_FILE     = r'E:/Ozon_API/teak-digit-438912-s0-cc0207cd6ee3.json'

HEADERS = {"Client-Id": CLIENT_ID,
           "Api-Key": TOKEN_OZ,
           "Content-Type": "application/json"}

OTHER_OP_NAME_COL   = "Доп. расходы"
OTHER_OP_AMOUNT_COL = "Сумма доп. расходов"
COST_COL            = "Себестоимость партии"
TAX_COL             = "Налоговые расходы"

# ────────────────────  G-Sheets helpers  ──────────────────────────────────
def open_sheet_retry(creds, key, name, retries=5):
    client = gspread.authorize(creds)
    delay = 5
    for i in range(retries):
        try:
            return client.open_by_key(key).worksheet(name)
        except APIError as e:
            if e.response.status_code == 503 and i < retries-1:
                print(f"⚠️  Google вернул 503, повтор через {delay} с"); time.sleep(delay); delay *= 2
            else:
                raise

def col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

HEAD_FMT  = CellFormat(backgroundColor=Color(1, 0.96, 0.62),
                       textFormat=TextFormat(bold=True))
TOTAL_FMT = CellFormat(backgroundColor=Color(0.88, 0.88, 0.88),
                       textFormat=TextFormat(bold=True))
BLANK_FMT = CellFormat(backgroundColor=Color(1, 1, 1),
                       textFormat=TextFormat(bold=False))

def apply_styles(sheet, values: List[List]):
    """Раскраска: жёлтая шапка, серые строки 'Итого', белый фон для пустых строк"""
    last = col_letter(len(values[0]))
    format_cell_range(sheet, f"A1:{last}1", HEAD_FMT)
    for i, row in enumerate(values, 1):
        if i == 1:
            continue
        if row and row[0] == "Итого":
            format_cell_range(sheet, f"A{i}:{last}{i}", TOTAL_FMT)
        elif not any(row):                          # полностью пустая строка
            format_cell_range(sheet, f"A{i}:{last}{i}", BLANK_FMT)

def split_by_first_block(rows: List[List]) -> Tuple[List[List], List[List]]:
    """
    Разбивает список строк таблицы на два массива:
    1) всё до (и включая) подряд двух пустых строк;
    2) всё, что после.
    """
    top, rest, blanks = [], [], 0
    for i, line in enumerate(rows):
        top.append(line)
        if i == 0:
            continue
        if any(line):
            blanks = 0
        else:
            blanks += 1
            if blanks == 2:
                rest = rows[i+1:]
                break
    return top, rest

# ────────────────────  Загрузка операций Ozon  ────────────────────────────
def month_by_month_operations() -> List[Dict]:
    ops: List[Dict] = []
    cur = datetime.now(tz=tz.tzutc()).replace(microsecond=0)
    while cur.replace(day=1) >= BOTTOM_TS:
        m_start = cur.replace(day=1, hour=0, minute=0, second=0)
        next_m  = m_start.replace(year=m_start.year + (m_start.month // 12),
                                  month=(m_start.month % 12) + 1)
        frm, to = m_start, next_m - timedelta(seconds=1)
        page = 1
        while True:
            payload = {"filter": {"date": {"from": frm.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                                           "to"  : to.strftime('%Y-%m-%dT%H:%M:%S.000Z')}},
                       "page": page, "page_size": PAGE_SIZE}
            r = requests.post(URL_FIN, headers=HEADERS, json=payload)
            r.raise_for_status()
            chunk = r.json().get("result", {}).get("operations", [])
            if not chunk:
                break
            ops.extend(chunk)
            if len(chunk) < PAGE_SIZE:
                break
            page += 1
        cur = m_start - timedelta(seconds=1)
    return ops

def week_start_tue(dt: datetime) -> datetime:
    """Начало отчётной недели с вторника."""
    return (dt - timedelta(days=(dt.weekday()-1)%7)).replace(hour=0, minute=0,
                                                             second=0, microsecond=0)

# ────────────────────  Читаем лист input  ────────────────────────────────
def load_input_mapping(creds) -> Dict[int, Tuple[float, float]]:
    ws   = open_sheet_retry(creds, SPREADSHEET_ID, INPUT_SHEET)
    rows = ws.get_all_values()
    hdr  = {h: i for i, h in enumerate(rows[0])}
    mp: Dict[int, Tuple[float, float]] = {}
    for r in rows[1:]:
        try:
            sku = int(r[hdr['SKU']])
        except:
            continue
        cost = float(r[hdr.get('Себестоимость', 2)] or 0)
        tax  = float(r[hdr.get('% Налога', 3)] or 0) / 100
        mp[sku] = (cost, tax)
    return mp

# ────────────────────  Строим недельный отчёт  ───────────────────────────
def build_weekly_report(ops: List[Dict],
                        lookup: Dict[int, Tuple[float, float]]) -> pd.DataFrame:

    sku_data: Dict[Tuple[datetime, int], Dict] = {}
    period_misc: Dict[datetime, Dict[str, float]] = {}
    extra_cols: set[str] = set()

    base_cols = ["Кол-во заказов", "Сумма заказов", "% Озон",
                 "Логистика", "Обратная логистика", "Кол-во обратных",
                 "Последняя миля", "Эквайринг", COST_COL, TAX_COL]
    STATIC_SET = set(base_cols)

    for op in ops:
        period = week_start_tue(datetime.strptime(op["operation_date"],
                         "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz.tzutc()))
        name, amount = op["operation_type_name"], op.get("amount", 0.0)
        accr, items  = op.get("accruals_for_sale", 0.0), op.get("items", [])
        servs        = op.get("services", [])

        # --- общие операции
        if not items:
            period_misc.setdefault(period, {}).setdefault(name, 0.0)
            period_misc[period][name] += amount
            continue

        # --- операции по SKU
        qtys  = [it.get("quantity", 1) for it in items]
        tot_q = sum(qtys) or 1
        for it, q in zip(items, qtys):
            sku, ratio = it["sku"], q / tot_q
            row = sku_data.setdefault((period, sku),
                   dict.fromkeys(base_cols, 0.0))

            if op["operation_type"] == "OperationAgentDeliveredToCustomer":
                if accr > 0:
                    row["Кол-во заказов"] += q
                    row["Сумма заказов"]  += accr * ratio
                    row["% Озон"]         += op.get("sale_commission", 0) * ratio
                    for s in servs:
                        if s["name"] == "MarketplaceServiceItemDirectFlowLogistic":
                            row["Логистика"] += s["price"] * ratio
                        elif s["name"] == "MarketplaceServiceItemDelivToCustomer":
                            row["Последняя миля"] += s["price"] * ratio
                    cost, tax = lookup.get(sku, (0, 0))
                    row[COST_COL] -= cost * q
                    row[TAX_COL]  -= accr * tax * ratio

            elif op["operation_type"] == "OperationItemReturn":
                row["Кол-во обратных"] += q
                for s in servs:
                    row["Обратная логистика"] += s["price"] * ratio

            elif op["operation_type"] == "MarketplaceRedistributionOfAcquiringOperation":
                row["Эквайринг"] += amount * ratio

            elif op["operation_type"].startswith("DisposalReason"):
                if name not in STATIC_SET:
                    extra_cols.add(name)
                    row[name] = row.get(name, 0.0) + amount * ratio

            else:   # любые другие динамические
                if name not in STATIC_SET:
                    extra_cols.add(name)
                    row[name] = row.get(name, 0.0) + amount * ratio

    rows: List[Dict] = []
    all_periods = sorted({p for p, _ in sku_data} | period_misc.keys(),
                         reverse=True)

    for period in all_periods:
        ptxt = f"{period:%d.%m.%Y} – {(period+timedelta(days=6)):%d.%m.%Y}"
        block: List[Dict] = []

        # строки по SKU
        for (pr, sku), m in sku_data.items():
            if pr != period:
                continue
            rec = {"Период": ptxt, "SKU": sku}
            rec.update({c: round(m[c]) for c in base_cols})
            for c in extra_cols:
                rec[c] = round(m.get(c, 0))
            rec.update({OTHER_OP_NAME_COL: "", OTHER_OP_AMOUNT_COL: ""})
            block.append(rec)

        # строки «доп. расходы»
        for idx, (nm, amt) in enumerate(period_misc.get(period, {}).items()):
            if idx < len(block):
                block[idx][OTHER_OP_NAME_COL]   = nm
                block[idx][OTHER_OP_AMOUNT_COL] = round(amt)
            else:
                pad = dict.fromkeys(block[0].keys(), "")
                pad[OTHER_OP_NAME_COL], pad[OTHER_OP_AMOUNT_COL] = nm, round(amt)
                block.append(pad)

        # строка «Итого»
        total = dict.fromkeys(block[0].keys(), "")
        total["Период"] = "Итого"
        for col in block[0]:
            if col in ("Период", "SKU", OTHER_OP_NAME_COL):
                continue
            total[col] = round(sum(float(r[col]) if r[col] else 0 for r in block))
        block.append(total)
        rows.extend(block)

    all_cols = ["Период", "SKU"] + base_cols + sorted(extra_cols) + \
               [OTHER_OP_NAME_COL, OTHER_OP_AMOUNT_COL]
    return pd.DataFrame(rows).reindex(columns=all_cols)

# ────────────────────  Upload: только верхний период  ────────────────────
def upload_to_gs(df: pd.DataFrame, creds):
    df = df.where(pd.notnull(df), "")
    sheet = open_sheet_retry(creds, SPREADSHEET_ID, OUTPUT_SHEET)
    last_col = col_letter(len(df.columns))

    # ---------- values из DataFrame (весь отчёт, но формулы вставим)
    vals = [df.columns.tolist()]
    cur  = None
    for _, r in df.iterrows():
        if r["Период"] not in (cur, "Итого"):
            if cur is not None:
                vals.extend([[""] * len(df.columns)] * 2)
            cur = r["Период"]
        row = r.tolist()
        if r["Период"] == "Итого":
            idx  = len(vals) + 1
            row[1] = f"=SUM(D{idx}:{last_col}{idx})"
        vals.append(row)

    # ---------- если лист пустой, загружаем целиком
    old_vals = sheet.get_all_values()
    if len(old_vals) <= 1:
        sheet.update(vals, 'A1', value_input_option='USER_ENTERED')
        sheet.freeze(rows=1)
        apply_styles(sheet, vals)
        print("✅ Лист был пуст — отчёт загружен")
        return

    # ---------- split both sheets by first double-blank separator
    new_top, _       = split_by_first_block(vals)
    old_top, old_tail = split_by_first_block(old_vals)

    # ---------- merge: header + new top (без header) + old tail
    merged = [vals[0]]            # шапка из нового отчёта
    merged.extend(new_top[1:])    # новый верхний период
    merged.extend(old_tail)       # старые периоды

    sheet.clear()
    sheet.update(merged, 'A1', value_input_option='USER_ENTERED')
    sheet.freeze(rows=1)
    apply_styles(sheet, merged)
    print("✅ Обновлён только верхний период, старые данные сохранены")

# ────────────────────  MAIN  ─────────────────────────────────────────────
if __name__ == "__main__":
    print("⏬ Скачиваю операции Ozon …")
    ops = month_by_month_operations()
    print("✔ Загружено операций:", len(ops))

    creds = Credentials.from_service_account_file(
        CREDS_FILE, scopes=["https://www.googleapis.com/auth/drive"]
    )

    print("📥 Читаю лист input …")
    sku_map = load_input_mapping(creds)
    print("✔ Позиций в справочнике:", len(sku_map))

    print("📊 Формирую недельный отчёт …")
    df = build_weekly_report(ops, sku_map)

    print("📤 Обновляю Google Sheets …")
    upload_to_gs(df, creds)
