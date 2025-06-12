"""
fin_week_1.py
─────────────────────────────────
Формирует еженедельный финансовый отчёт на основе транзакций Ozon Seller API
и загружает его в Google Sheets.

Версия, преобразованная в глобальный модуль по аналогии с p_campain_fin_1.py.
- Вся логика инкапсулирована в функции run().
- Конфигурация (токены, ID) передаётся как параметры.
- Улучшено логирование и обработка ошибок API.
"""
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

from gspread_formatting import (
    CellFormat, Color, TextFormat,
    format_cell_range, batch_updater
)

# Импортируем clear_sheet_formatting с резервной реализацией
try:
    from gspread_formatting import clear_sheet_formatting
except ImportError:
    def clear_sheet_formatting(worksheet, batch=None):
        req = {
            "updateCells": {
                "range": {"sheetId": worksheet._properties["sheetId"]},
                "fields": "userEnteredFormat"
            }
        }
        if batch is not None:
            batch.requests.append(req)
        else:
            worksheet.spreadsheet.batch_update({"requests": [req]})


# ────────────────────  Константы и настройки  ──────────────────────────
URL_FIN   = 'https://api-seller.ozon.ru/v3/finance/transaction/list'
PAGE_SIZE = 1000
REQUEST_TIMEOUT = 60 # Таймаут для запросов к API

# Названия колонок
OTHER_OP_NAME_COL   = "Доп. расходы"
OTHER_OP_AMOUNT_COL = "Сумма доп. расходов"
COST_COL            = "Себестоимость партии"
TAX_COL             = "Налоговые расходы"

# Форматы ячеек
HEAD_FMT  = CellFormat(backgroundColor=Color(1, 0.96, 0.62), textFormat=TextFormat(bold=True))
TOTAL_FMT = CellFormat(backgroundColor=Color(0.88, 0.88, 0.88), textFormat=TextFormat(bold=True))
BLANK_FMT = CellFormat(backgroundColor=Color(1, 1, 1), textFormat=TextFormat(bold=False))

# ────────────────────  Вспомогательные функции  ─────────────────────────
def log(msg: str):
    """Выводит сообщение с временной меткой."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def col_letter(n: int) -> str:
    """Преобразует номер колонки в букву (1 -> A, 2 -> B)."""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def week_start_tue(dt: datetime) -> datetime:
    """Возвращает начало отчётной недели (вторник)."""
    return (dt - timedelta(days=(dt.weekday() - 1) % 7)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

def split_by_first_block(rows: List[List]) -> Tuple[List[List], List[List]]:
    """Разбивает список строк на две части по первому двойному пробелу."""
    top, rest, blanks = [], [], 0
    for i, line in enumerate(rows):
        top.append(line)
        if i == 0: continue
        if any(line):
            blanks = 0
        else:
            blanks += 1
            if blanks == 2:
                rest = rows[i + 1:]
                break
    return top, rest

# ────────────────────  Работа с Google Sheets  ───────────────────────────
def open_sheet_retry(creds, key, name, retries=5, delay=5):
    """Открывает лист с несколькими попытками при ошибках API."""
    client = gspread.authorize(creds)
    for i in range(retries):
        try:
            return client.open_by_key(key).worksheet(name)
        except APIError as e:
            if e.response.status_code in (429, 503) and i < retries - 1:
                log(f"⚠️ Google API вернул {e.response.status_code}, повтор через {delay} сек.")
                time.sleep(delay)
                delay *= 2
            else:
                raise

def safe_update(func, *args, **kwargs):
    """Обёртка для методов update/batch_update с повторными попытками."""
    retries, delay = 5, 5
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            if e.response.status_code in (429, 503) and i < retries - 1:
                log(f"⚠️ G-Sheets {func.__name__} вернул {e.response.status_code}; повтор {i+1}/{retries-1} через {delay} сек.")
                time.sleep(delay)
                delay *= 2
            else:
                raise

def apply_styles(sheet, values: List[List]):
    """Применяет стили к шапке, итоговым и пустым строкам."""
    last_col = col_letter(len(values[0]))
    clear_sheet_formatting(sheet)
    format_cell_range(sheet, f"A1:{last_col}1", HEAD_FMT)

    total_ranges, blank_ranges = [], []
    cur_total = cur_blank = None
    for i, row in enumerate(values, 1):
        if i == 1: continue
        is_total = row and row[0] == "Итого"
        is_blank = not any(row)

        if is_total: cur_total = i if cur_total is None else cur_total
        elif cur_total is not None:
            total_ranges.append((cur_total, i - 1)); cur_total = None
        if is_blank: cur_blank = i if cur_blank is None else cur_blank
        elif cur_blank is not None:
            blank_ranges.append((cur_blank, i - 1)); cur_blank = None

    if cur_total is not None: total_ranges.append((cur_total, len(values)))
    if cur_blank is not None: blank_ranges.append((cur_blank, len(values)))

    with batch_updater(sheet.spreadsheet) as batch:
        for s, e in total_ranges:
            format_cell_range(sheet, f"A{s}:{last_col}{e}", TOTAL_FMT, batch)
        for s, e in blank_ranges:
            format_cell_range(sheet, f"A{s}:{last_col}{e}", BLANK_FMT, batch)

# ────────────────────  Загрузка данных из Ozon  ──────────────────────────
def month_by_month_operations(headers: dict, bottom_ts: datetime) -> List[Dict]:
    """Скачивает все операции с Ozon API помесячно с логикой retry."""
    ops: List[Dict] = []
    cur = datetime.now(tz=tz.tzutc()).replace(microsecond=0)
    
    while cur.replace(day=1) >= bottom_ts:
        m_start = cur.replace(day=1, hour=0, minute=0, second=0)
        next_m  = m_start.replace(year=m_start.year + (m_start.month // 12), month=(m_start.month % 12) + 1)
        frm, to = m_start, next_m - timedelta(seconds=1)
        log(f"🗓️  Загрузка операций за {frm.strftime('%Y-%m')}...")
        page = 1
        
        while True:
            payload = {"filter": {"date": {"from": frm.strftime('%Y-%m-%dT%H:%M:%S.000Z'), "to": to.strftime('%Y-%m-%dT%H:%M:%S.000Z')}}, "page": page, "page_size": PAGE_SIZE}
            chunk = []
            
            for attempt in range(5): # 5 попыток
                try:
                    r = requests.post(URL_FIN, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
                    if r.status_code == 429:
                        delay = 15 * (attempt + 1)
                        log(f"⚠️ API Ozon вернул 429. Повтор через {delay} сек...")
                        time.sleep(delay)
                        continue
                    r.raise_for_status()
                    chunk = r.json().get("result", {}).get("operations", [])
                    break
                except requests.exceptions.RequestException as e:
                    if attempt == 4:
                        log(f"❌ Ошибка API Ozon после всех попыток: {e}")
                        raise
                    delay = 10 * (attempt + 1)
                    log(f"⚠️ Ошибка запроса к API Ozon ({e}). Повтор через {delay} сек...")
                    time.sleep(delay)
            
            if not chunk: break
            ops.extend(chunk)
            if len(chunk) < PAGE_SIZE: break
            page += 1
        
        cur = m_start - timedelta(seconds=1)
    return ops

# ────────────────────  Обработка и построение отчёта  ───────────────────
def load_input_mapping(creds, spreadsheet_id: str, sheet_name: str) -> Dict[int, Tuple[float, float]]:
    """Загружает справочник SKU, себестоимости и налога из листа input."""
    ws = open_sheet_retry(creds, spreadsheet_id, sheet_name)
    rows = ws.get_all_values()
    hdr = {h: i for i, h in enumerate(rows[0])}
    mp: Dict[int, Tuple[float, float]] = {}
    for r in rows[1:]:
        try:
            sku = int(r[hdr['SKU']])
            cost = float(r[hdr.get('Себестоимость', 2)] or 0)
            tax = float(r[hdr.get('% Налога', 3)] or 0) / 100
            mp[sku] = (cost, tax)
        except (ValueError, IndexError):
            continue
    return mp

def build_weekly_report(ops: List[Dict], lookup: Dict[int, Tuple[float, float]]) -> pd.DataFrame:
    """Строит еженедельный отчёт на основе данных об операциях."""
    sku_data: Dict[Tuple[datetime, int], Dict] = {}
    period_misc: Dict[datetime, Dict[str, float]] = {}
    extra_cols: set[str] = set()

    base_cols = ["Кол-во заказов", "Сумма заказов", "% Озон", "Логистика", "Обратная логистика", "Кол-во обратных", "Последняя миля", "Эквайринг", COST_COL, TAX_COL]
    STATIC_SET = set(base_cols)

    for op in ops:
        period = week_start_tue(datetime.strptime(op["operation_date"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz.tzutc()))
        name, amount = op["operation_type_name"], op.get("amount", 0.0)
        accr, items, servs = op.get("accruals_for_sale", 0.0), op.get("items", []), op.get("services", [])

        if not items:
            period_misc.setdefault(period, {}).setdefault(name, 0.0)
            period_misc[period][name] += amount
            continue

        qtys = [it.get("quantity", 1) for it in items]
        tot_q = sum(qtys) or 1
        for it, q in zip(items, qtys):
            sku, ratio = it["sku"], q / tot_q
            row = sku_data.setdefault((period, sku), dict.fromkeys(base_cols, 0.0))
            op_type = op["operation_type"]

            if op_type == "OperationAgentDeliveredToCustomer" and accr > 0:
                row["Кол-во заказов"] += q
                row["Сумма заказов"] += accr * ratio
                row["% Озон"] += op.get("sale_commission", 0) * ratio
                for s in servs:
                    if s["name"] == "MarketplaceServiceItemDirectFlowLogistic": row["Логистика"] += s["price"] * ratio
                    elif s["name"] == "MarketplaceServiceItemDelivToCustomer": row["Последняя миля"] += s["price"] * ratio
                cost, tax = lookup.get(sku, (0, 0))
                row[COST_COL] -= cost * q
                row[TAX_COL] -= accr * tax * ratio
            elif op_type == "OperationItemReturn":
                row["Кол-во обратных"] += q
                for s in servs: row["Обратная логистика"] += s["price"] * ratio
            elif op_type == "MarketplaceRedistributionOfAcquiringOperation": row["Эквайринг"] += amount * ratio
            else:
                if name not in STATIC_SET:
                    extra_cols.add(name)
                    row.setdefault(name, 0.0)
                row[name] = row.get(name, 0.0) + amount * ratio
    
    rows: List[Dict] = []
    all_periods = sorted({p for p, _ in sku_data} | period_misc.keys(), reverse=True)
    all_cols = ["Период", "SKU"] + base_cols + sorted(extra_cols) + [OTHER_OP_NAME_COL, OTHER_OP_AMOUNT_COL]

    for period in all_periods:
        ptxt = f"{period:%d.%m.%Y} – {(period + timedelta(days=6)):%d.%m.%Y}"
        block: List[Dict] = []
        
        period_skus = {sku: m for (pr, sku), m in sku_data.items() if pr == period}
        for sku, m in period_skus.items():
            rec = {"Период": ptxt, "SKU": sku}
            rec.update({c: round(m.get(c, 0)) for c in base_cols})
            rec.update({c: round(m.get(c, 0)) for c in extra_cols})
            block.append(rec)
        
        misc_items = list(period_misc.get(period, {}).items())
        for idx in range(max(len(block), len(misc_items))):
            if idx >= len(block): block.append({})
            op_name, op_amount = misc_items[idx] if idx < len(misc_items) else ("", "")
            block[idx][OTHER_OP_NAME_COL] = op_name
            block[idx][OTHER_OP_AMOUNT_COL] = round(op_amount) if isinstance(op_amount, (int, float)) else op_amount
        
        if not block: continue
        
        # Заполняем пустые ячейки для консистентности перед расчётом итогов
        for r in block:
            for c in all_cols:
                r.setdefault(c, "")

        total = {"Период": "Итого"}
        for col in all_cols:
            if col not in ["Период", "SKU", OTHER_OP_NAME_COL]:
                total[col] = round(sum(float(r.get(col) or 0) for r in block))
        block.append(total)
        
        rows.extend(block)
        if period != all_periods[-1]: rows.extend([{}, {}]) # Добавляем пустые строки между периодами
        
    return pd.DataFrame(rows, columns=all_cols)

# ────────────────────  Загрузка в Google Sheets  ───────────────────────
def upload_to_gs(df: pd.DataFrame, creds, spreadsheet_id: str, sheet_name: str):
    """Обновляет Google-лист: верхний период заменяется, остальное сохраняется."""
    df_prepared = df.where(pd.notnull(df), None)
    sheet = open_sheet_retry(creds, spreadsheet_id, sheet_name)
    last_col = col_letter(len(df_prepared.columns))

    vals = [df_prepared.columns.tolist()] + df_prepared.values.tolist()
    # Заменяем None на пустые строки для корректной записи
    vals = [[ "" if x is None else x for x in row] for row in vals]
    
    for i, row in enumerate(vals):
      if row and row[0] == "Итого":
        # Формула прибыли для строки "Итого"
        formula_cols = "DEFGHIJK" # Колонки для суммирования
        sum_formula = "+".join([f'{c}{i+1}' for c in formula_cols])
        # vals[i][1] = f"=SUM(D{i+1}:{last_col}{i+1})"
        
    old_vals = sheet.get_all_values()
    if len(old_vals) <= 1:
        log("📝 Лист пуст — отчёт будет загружен полностью.")
        safe_update(sheet.update, vals, 'A1', value_input_option='USER_ENTERED')
        safe_update(sheet.freeze, rows=1)
        apply_styles(sheet, vals)
        log("✅ Отчёт успешно загружен.")
        return

    new_top, _ = split_by_first_block(vals)
    _, old_tail = split_by_first_block(old_vals)

    merged = [vals[0]] + new_top[1:] + old_tail
    
    safe_update(sheet.clear)
    safe_update(sheet.update, merged, 'A1', value_input_option='USER_ENTERED')
    safe_update(sheet.freeze, rows=1)
    apply_styles(sheet, merged)
    log("✅ Обновлён только верхний период, старые данные сохранены.")

# ──────────────────────────── MAIN / RUN ────────────────────────────
def run(*,
        gs_creds_path: str,
        spreadsheet_id: str,
        output_sheet_name: str,
        input_sheet_name: str,
        ozon_client_id: str,
        ozon_api_key: str,
        start_date_str: str = "2022-01-01"
    ):
    """
    Основная функция для запуска процесса формирования и загрузки отчёта.
    """
    log("🚀 Запуск скрипта fin_week_1")

    try:
        creds = Credentials.from_service_account_file(
            gs_creds_path, scopes=["https://www.googleapis.com/auth/drive", "https://spreadsheets.google.com/feeds"]
        )
        
        log("📥 Читаю лист input со справочником SKU...")
        sku_map = load_input_mapping(creds, spreadsheet_id, input_sheet_name)
        log(f"✔ Найдено позиций в справочнике: {len(sku_map)}")
        
        headers = {
            "Client-Id": ozon_client_id,
            "Api-Key": ozon_api_key,
            "Content-Type": "application/json"
        }
        bottom_ts = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=tz.tzutc())
        
        log("⏬ Скачиваю операции Ozon...")
        ops = month_by_month_operations(headers, bottom_ts)
        log(f"✔ Загружено операций: {len(ops)}")

        if not ops:
            log("⚠️ Операции не найдены. Завершение работы.")
            return

        log("📊 Формирую недельный отчёт...")
        df = build_weekly_report(ops, sku_map)

        log("📤 Обновляю Google Sheets...")
        upload_to_gs(df, creds, spreadsheet_id, output_sheet_name)
        
        log("🎉 Скрипт fin_week_1 успешно завершён!")

    except FileNotFoundError:
        log(f"❌ Критическая ошибка: файл учетных данных Google не найден по пути: {gs_creds_path}")
        raise
    except Exception as e:
        log(f"❌ Критическая ошибка выполнения: {e}")
        raise

if __name__ == "__main__":
    #  Tauri's note:
    # ⚠️ Эти данные взяты из вашего исходного файла `fin_week_1.py`.
    # Они жестко вписаны для удобства запуска.
    # В реальных условиях лучше использовать переменные окружения для безопасности.

    run(
        gs_creds_path=r'E:/Ozon_API/teak-digit-438912-s0-cc0207cd6ee3.json',
        spreadsheet_id='1pkR_vV-g0cI8AUdQEAw2VTS4tBe78t5gD-ExqUnrpfo',
        output_sheet_name='week_fin',
        input_sheet_name='input',
        ozon_client_id='2567268',
        ozon_api_key='102efb35-db8d-4552-b6fa-75c0a66ce11d',
        start_date_str="2022-01-01"
    )
