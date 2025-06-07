#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Еженедельный отчёт Ozon (вторник → вторник) → Google Sheets

• диапазон каждого запроса строго ≤ 30 дней (требование API);
• page_size = 1000 (максимум, поддерживаемый /v3/finance/transaction/list);
• transaction_type убран — метод сам вернёт все типы операций;
• в случае HTTP 400 выводит тело ошибки и аварийно завершает скрипт;
• остальная бизнес-логика (Кол-во заказов, Трафареты, Вывод в топ,
  Услуги FBO и т. д.) не изменялась.
"""

from __future__ import annotations
import requests
from datetime import datetime, timedelta
from dateutil import tz
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import sys
import json

# ---------- Ozon API ----------
TOKEN_OZ  = '102efb35-db8d-4552-b6fa-75c0a66ce11d'
CLIENT_ID = '2567268'
URL_FIN   = 'https://api-seller.ozon.ru/v3/finance/transaction/list'
PAGE_SIZE = 1000                  # максимум разрешённый API
MAX_DAYS  = 30                    # диапазон ≤ 30 дней
BOTTOM_TS = datetime(2022, 1, 1, tzinfo=tz.tzutc())

HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": TOKEN_OZ,
    "Content-Type": "application/json"
}

# ---------- Google Sheets ----------
SCOPES         = ['https://spreadsheets.google.com/feeds',
                  'https://www.googleapis.com/auth/drive']
CREDS_FILE     = 'E:/Ozon_API/teak-digit-438912-s0-cc0207cd6ee3.json'
SPREADSHEET_ID = '1pkR_vV-g0cI8AUdQEAw2VTS4tBe78t5gD-ExqUnrpfo'
SHEET_NAME     = 'week_fin'

# ---------- Типы для «Услуг FBO» ----------
FBO_TYPES = {
    "MarketplaceServiceItemCrossdocking",
    "OperationMarketplaceServiceSupplyInboundCargoShortage",
    "OperationMarketplaceSupplyExpirationDateProcessing"
}

# --------------------------------------------------------------------------- #
#                              DATA COLLECTION                                #
# --------------------------------------------------------------------------- #
def get_finance_operations() -> list[dict]:
    """
    Скачивает все операции, делая запрос **по каждому календарному месяцу**.
    Диапазон каждого запроса лежит строго в пределах одного месяца, что
    удовлетворяет требование Ozon API «only one month allowed».
    """
    all_ops: list[dict] = []
    tz_utc = tz.tzutc()

    # берём текущий день (UTC) и обрезаем микросекунды
    cursor = datetime.now(tz=tz_utc).replace(microsecond=0)

    while cursor.replace(day=1) >= BOTTOM_TS:
        # начало месяца
        month_start = cursor.replace(day=1, hour=0, minute=0, second=0)
        # конец месяца = первое число следующего месяца минус 1 секунда
        if month_start.month == 12:
            next_month_start = month_start.replace(year=month_start.year + 1,
                                                   month=1)
        else:
            next_month_start = month_start.replace(month=month_start.month + 1)
        month_end = next_month_start - timedelta(seconds=1)

        # если работаем с «текущим» месяцем — обрезаем верхнюю границу now
        to_date = min(cursor, month_end)
        frm = month_start
        if frm < BOTTOM_TS:
            frm = BOTTOM_TS

        page = 1
        while True:
            payload = {
                "filter": {
                    "date": {
                        "from": frm.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                        "to"  : to_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    }
                },
                "page": page,
                "page_size": PAGE_SIZE
            }

            resp = requests.post(URL_FIN, headers=HEADERS, json=payload)
            if resp.status_code == 400:
                print("🔴 400 Bad Request:", resp.text, file=sys.stderr)
                sys.exit(1)
            resp.raise_for_status()

            chunk = resp.json().get("result", {}).get("operations", [])
            if not chunk:
                break

            all_ops.extend(chunk)
            if len(chunk) < PAGE_SIZE:
                break
            page += 1

        # переходим к предыдущему месяцу
        cursor = month_start - timedelta(seconds=1)

    return all_ops

# --------------------------------------------------------------------------- #
#                    ВСПОМОГАТЕЛЬНЫЕ И АГРЕГАЦИОННЫЕ ФУНКЦИИ                   #
# --------------------------------------------------------------------------- #
def week_start_tue(dt: datetime) -> datetime:
    """UTC 00:00 последнего вторника перед dt (или самого dt, если вторник)."""
    delta = (dt.weekday() - 1) % 7  # Tue=1
    return (dt - timedelta(days=delta)).replace(hour=0, minute=0,
                                                second=0, microsecond=0)

def build_weekly_report(ops: list[dict]) -> pd.DataFrame:
    """Формирует DataFrame с полным отчётом (см. ТЗ)."""
    sku_cache: dict[tuple[datetime, int], dict] = {}
    period_extra: dict[datetime, dict] = {}

    for op in ops:
        # --- базовые поля ---
        op_dt  = datetime.strptime(op["operation_date"],
                                   "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz.tzutc())
        period = week_start_tue(op_dt)
        otype  = op["operation_type"]
        amount = op.get("amount", 0)
        accr   = op.get("accruals_for_sale", 0)
        items  = op.get("items", [])
        servs  = op.get("services", [])

        # --- агрегаты без SKU (Трафареты, Топ, FBO) ---
        pe = period_extra.setdefault(period, {"stencil": 0.0,
                                              "top": 0.0,
                                              "fbo": 0.0})
        if otype == "OperationElectronicServiceStencil":
            pe["stencil"] += amount
        elif otype == "OperationGettingToTheTop":
            pe["top"] += amount
        elif otype in FBO_TYPES:
            pe["fbo"] += amount

        for s in servs:
            if s["name"] in FBO_TYPES:
                pe["fbo"] += s["price"]

        # --- пропускаем, если у операции нет товаров ---
        if not items:
            continue

        qtys = [it.get("quantity", 1) for it in items]
        total_q = sum(qtys) or 1

        for it, q in zip(items, qtys):
            sku   = it["sku"]
            key   = (period, sku)
            ratio = q / total_q

            if key not in sku_cache:
                sku_cache[key] = {
                    "orders_qty"        : 0,
                    "orders_sum"        : 0.0,
                    "sale_commission"   : 0.0,
                    "logistics"         : 0.0,
                    "last_mile"         : 0.0,
                    "reverse_logistics" : 0.0,
                    "agent_services"    : 0.0,
                    "acquiring"         : 0.0
                }
            row = sku_cache[key]

            if otype == "OperationAgentDeliveredToCustomer":
                if accr > 0:                       # только реальная продажа
                    row["orders_qty"] += q
                row["orders_sum"]      += accr * ratio
                row["sale_commission"] += op.get("sale_commission", 0) * ratio
                for s in servs:
                    if s["name"] == "MarketplaceServiceItemDirectFlowLogistic":
                        row["logistics"] += s["price"] * ratio
                    elif s["name"] == "MarketplaceServiceItemDelivToCustomer":
                        row["last_mile"] += s["price"] * ratio

            elif otype == "OperationItemReturn":
                for s in servs:
                    if s["name"] == "MarketplaceServiceItemReturnFlowLogistic":
                        row["reverse_logistics"] += s["price"] * ratio
                    else:
                        row["agent_services"] += s["price"] * ratio

            elif otype == "MarketplaceRedistributionOfAcquiringOperation":
                row["acquiring"] += amount * ratio

    # ---------------- DataFrame ----------------
    rows = []
    for period, ex in period_extra.items():
        pers = f"{period:%d.%m.%Y} - {(period+timedelta(days=6)):%d.%m.%Y}"
        rows.append({
            "Период": pers, "SKU": "",
            "Кол-во заказов": "", "Сумма заказов": "", "% Озон": "",
            "Логистика": "", "Обратная логистика": "", "Услуги агентов": "",
            "Последняя миля": "", "Эквайринг": "",
            "Трафареты": int(round(ex["stencil"])),
            "Вывод в топ": int(round(ex["top"])),
            "Услуги FBO": int(round(ex["fbo"])),
            "_period": period, "_rank": 0
        })

    for (period, sku), m in sku_cache.items():
        pers = f"{period:%d.%m.%Y} - {(period+timedelta(days=6)):%d.%m.%Y}"
        rows.append({
            "Период": pers, "SKU": sku,
            "Кол-во заказов": m["orders_qty"],
            "Сумма заказов": int(round(m["orders_sum"])),
            "% Озон": int(round(m["sale_commission"])),
            "Логистика": int(round(m["logistics"])),
            "Обратная логистика": int(round(m["reverse_logistics"])),
            "Услуги агентов": int(round(m["agent_services"])),
            "Последняя миля": int(round(m["last_mile"])),
            "Эквайринг": int(round(m["acquiring"])),
            "Трафареты": "", "Вывод в топ": "", "Услуги FBO": "",
            "_period": period, "_rank": 1
        })

    df = pd.DataFrame(rows)
    df.sort_values(["_period", "_rank", "SKU"],
                   ascending=[False, True, True], inplace=True)
    df.drop(columns=["_period", "_rank"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    columns = ["Период", "SKU", "Кол-во заказов", "Сумма заказов", "% Озон",
               "Логистика", "Обратная логистика", "Услуги агентов",
               "Последняя миля", "Эквайринг",
               "Трафареты", "Вывод в топ", "Услуги FBO"]
    return df[columns]

# --------------------------------------------------------------------------- #
#                          EXPORT TO GOOGLE SHEETS                            #
# --------------------------------------------------------------------------- #
def upload_to_gs(df: pd.DataFrame) -> None:
    try:
        creds  = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        sheet  = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
    except Exception as e:
        print("❌ Авторизация / открытие листа:", e, file=sys.stderr)
        sys.exit(1)

    values = [df.columns.tolist()]
    cur = None
    for _, r in df.iterrows():
        if cur is None:
            cur = r["Период"]
        elif r["Период"] != cur:
            values.extend([[""] * len(df.columns), [""] * len(df.columns)])
            cur = r["Период"]
        values.append([str(v) if v != "" else "" for v in r.tolist()])

    try:
        sheet.clear()
        sheet.update(values=values, range_name='A1',
                     value_input_option='USER_ENTERED')
        sheet.freeze(rows=1)
        print("✅ Данные загружены в Google Sheets")
    except Exception as e:
        print("❌ Ошибка записи в лист:", e, file=sys.stderr)
        sys.exit(1)

# --------------------------------------------------------------------------- #
#                                     MAIN                                    #
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    print("⏬ Загружаем операции Ozon …")
    ops = get_finance_operations()
    print(f"✔ Получено операций: {len(ops)}")

    print("📊 Формируем отчёт …")
    df_report = build_weekly_report(ops)

    print("📤 Отправляем в Google Sheets …")
    upload_to_gs(df_report)
