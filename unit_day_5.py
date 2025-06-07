

import requests, gspread, pytz
from datetime import datetime, timedelta, timezone
from dateutil import tz
from google.oauth2.service_account import Credentials
from collections import defaultdict

# ───────────── Настройки ─────────────
TOKEN_OZ  = '102efb35-db8d-4552-b6fa-75c0a66ce11d'
CLIENT_ID = '2567268'
HEADERS   = {'Client-Id': CLIENT_ID, 'Api-Key': TOKEN_OZ}

GS_CRED   = r'E:/Ozon_API/teak-digit-438912-s0-cc0207cd6ee3.json'
SPREAD_ID = '1pkR_vV-g0cI8AUdQEAw2VTS4tBe78t5gD-ExqUnrpfo'
SHEET_MAIN, SHEET_SRC = 'unit-day', 'input'

DEFAULT_TAX = 7.0
tz_msk      = pytz.timezone('Europe/Moscow')
now_msk     = datetime.now(timezone.utc).astimezone(tz_msk)
today_key   = now_msk.strftime('%d.%m.%Y')
today_disp  = now_msk.strftime('%d.%m.%Y (%H:%M МСК)')

# ───── helpers ─────
def num(x):
    try: return round(float(str(x).replace(',', '.')), 2)
    except: return 0.0

def col_letter(n):                       # 1-based → 'A', 'B'…
    s = ''
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# ───────── 1. Продажи (7 дней) ─────────
url_sales = 'https://api-seller.ozon.ru/v1/analytics/data'
sales_req = {
    "date_from": (now_msk - timedelta(days=7)).strftime('%Y-%m-%d'),
    "date_to"  :  now_msk.strftime('%Y-%m-%d'),
    "metrics"  : ["ordered_units", "revenue"],
    "dimension": ["sku", "day"],
    "limit"    : 1000
}
sales_raw = requests.post(url_sales, headers=HEADERS,
                          json=sales_req, timeout=60).json()

sales = defaultdict(lambda: {"name": None, "units": 0, "rev": 0.0})
for r in sales_raw['result']['data']:
    sku  = r['dimensions'][0]['id']
    name = r['dimensions'][0]['name']
    day  = datetime.strptime(r['dimensions'][1]['id'],
                             '%Y-%m-%d').strftime('%d.%m.%Y')
    sales[(day, sku)]['name']  = name
    sales[(day, sku)]['units'] += r['metrics'][0]
    sales[(day, sku)]['rev']   += r['metrics'][1]

# ───────── 2. Финансы (30 дней) ─────────
url_fin = 'https://api-seller.ozon.ru/v3/finance/transaction/list'
fin_req = {
    "filter": {
      "date": {
        "from": (datetime.now(tz.tzutc()) - timedelta(days=30)
                ).strftime('%Y-%m-%dT%H:%M:%S.000Z'),
        "to"  :  datetime.now(tz.tzutc()
                ).strftime('%Y-%m-%dT%H:%M:%S.000Z')},
      "transaction_type": "all"},
    "page": 1, "page_size": 1000
}
fin_ops = requests.post(url_fin, headers=HEADERS,
                        json=fin_req, timeout=60).json()\
                        ['result']['operations']

tmp = defaultdict(lambda: {"log": [], "acq": [], "last": [],
                           "accr": 0.0, "comm": 0.0})
for op in fin_ops:
    accr = op.get('accruals_for_sale', 0)
    comm = abs(op.get('sale_commission', 0))
    for it in op.get('items', []):
        sku = it['sku']
        for s in op.get('services', []):
            n, p = s['name'], abs(s['price'])
            if n == 'MarketplaceServiceItemDirectFlowLogistic':
                tmp[sku]['log'].append(p)
            elif n == 'MarketplaceRedistributionOfAcquiringOperation':
                tmp[sku]['acq'].append(p)
            elif n == 'MarketplaceServiceItemDelivToCustomer':
                tmp[sku]['last'].append(p)
        tmp[sku]['accr'] += accr
        tmp[sku]['comm'] += comm

svc = {}
for sku, d in tmp.items():
    pct = round(d['comm'] / d['accr'] * 100, 2) if d['accr'] else 0
    avg = lambda lst: round(sum(lst)/len(lst), 2) if lst else 0
    svc[sku] = {"log": avg(d['log']), "acq": avg(d['acq']),
                "last": avg(d['last']), "pct": pct}

# ───────── 3. Sheets ─────────
creds  = Credentials.from_service_account_file(
    GS_CRED, scopes=['https://spreadsheets.google.com/feeds',
                     'https://www.googleapis.com/auth/drive'])
gc     = gspread.authorize(creds)
ws     = gc.open_by_key(SPREAD_ID).worksheet(SHEET_MAIN)
sheet_id = ws.id

# Adv старые
adv_map = {(r[0][:10], r[1]): num(r[5])
           for r in ws.get_all_values()[1:]
           if r and r[0] not in ("", "Итого")}

# input-лист
inp_vals = gc.open_by_key(SPREAD_ID).worksheet(SHEET_SRC).get_all_values()
inp_map  = {}
for row in inp_vals[1:]:
    if not row or not row[0]: continue
    sku = str(row[0]).strip()
    inp_map[sku] = {"name":  row[1].strip() if len(row)>1 else "",
                    "sebes": num(row[2])    if len(row)>2 else 0.0,
                    "tax%": num(str(row[3]).replace('%',''))
                            if len(row)>3 else DEFAULT_TAX}

# ─── Заголовок ───
HEAD = [
 "Дата обновления","SKU","Название товара","Количество продаж","Сумма продаж",
 "Расходы на рекламу","Логистика","Комиссия Озон","Эквайринг","Последняя миля",
 "Налог (руб)","Себес. Продаж","Себес. Юнит","Прибыль","Маржа %"
]

# индексы
IDX_ADV, IDX_LOG, IDX_COMM, IDX_ACQ, IDX_LAST  = 5,6,7,8,9
IDX_TAX, IDX_SEB_PR, IDX_SEB_UNIT = 10,11,12
IDX_PROF, IDX_MAR                 = 13,14

# ─── Формируем строки ───
rows_by_day = defaultdict(list)

for (day, sku), s in sales.items():
    sku_str = str(sku)
    units   = int(s['units'])
    revenue = round(float(s['rev']), 2)

    sv      = svc.get(int(sku), {})
    log_r   = round(abs(sv.get('log', 0))*units, 2)
    acq_r   = round(abs(sv.get('acq', 0))*units, 2)
    last_r  = round(abs(sv.get('last', 0))*units, 2)
    comm_r  = round(revenue*sv.get('pct',0)/100, 2)

    tax_pct = inp_map.get(sku_str, {}).get("tax%", DEFAULT_TAX)
    tax_r   = round(revenue*tax_pct/100, 2)

    adv     = adv_map.get((day, sku_str), 0.0)
    sebes_u = inp_map.get(sku_str, {}).get("sebes", 0.0)

    row = [
      today_disp if day==today_key else day, sku,
      inp_map.get(sku_str, {}).get("name") or s['name'],
      units, revenue, adv, log_r, comm_r, acq_r, last_r,
      tax_r, "", sebes_u, "", ""     # формулы позднее
    ]
    rows_by_day[day].append(row)

# ─── Таблица + строки «Итого» ───
table, total_idx = [HEAD], [] ; r_idx = 2

for day in sorted(rows_by_day,
                  key=lambda d: datetime.strptime(d,'%d.%m.%Y'),
                  reverse=True):
    start = r_idx
    table.extend(rows_by_day[day]); r_idx += len(rows_by_day[day])

    # строка «Итого»
    tot = ["Итого"] + [""]*(len(HEAD)-1)
    cols_sum = [3,4,5,6,7,8,9,10,11,13]
    for ci in cols_sum:
        ltr = col_letter(ci+1)
        tot[ci] = f"=SUM({ltr}{start}:{ltr}{r_idx-1})"
    table.append(tot); total_idx.append(r_idx); r_idx += 1
    table.append([""]*len(HEAD)); r_idx += 1

# ─── Формулы строк данных ───
for i,row in enumerate(table[1:], start=2):
    if not row or row[0] in ("", "Итого"): continue
    row[IDX_SEB_PR] = f'=IF({col_letter(IDX_SEB_UNIT+1)}{i}="";"";' \
                      f'ROUND({col_letter(IDX_SEB_UNIT+1)}{i}*D{i};2))'
    row[IDX_PROF]   = f'=IF(E{i}=0;"";ROUND(E{i}-F{i}-G{i}-H{i}-I{i}-' \
                      f'J{i}-K{i}-L{i};2))'
    row[IDX_MAR]    = f'=IF(E{i}=0;"";ROUND(N{i}/E{i}*100;2))'

# ─── Запись ───
ws.clear()
ws.update(table,'A1',value_input_option='USER_ENTERED')

# ─── Формат ───
req=[
 {"repeatCell":{
  "range":{"sheetId":sheet_id,"startRowIndex":0,"endRowIndex":len(table)},
  "cell":{"userEnteredFormat":{"backgroundColor":{"red":1,"green":1,"blue":1},
                               "textFormat":{"bold":False}}},
  "fields":"userEnteredFormat(backgroundColor,textFormat.bold)"}},
 {"repeatCell":{
  "range":{"sheetId":sheet_id,"startRowIndex":0,"endRowIndex":1},
  "cell":{"userEnteredFormat":{"backgroundColor":{"red":1,"green":1,"blue":0.6},
                               "textFormat":{"bold":True}}},
  "fields":"userEnteredFormat(backgroundColor,textFormat.bold)"}}]

for r in total_idx:
    req.append({
     "repeatCell":{
      "range":{"sheetId":sheet_id,
               "startRowIndex":r-1,"endRowIndex":r},
      "cell":{"userEnteredFormat":{
          "backgroundColor":{"red":0.93,"green":0.93,"blue":0.93},
          "textFormat":{"bold":True}}},
      "fields":"userEnteredFormat(backgroundColor,textFormat.bold)"}})

ws.spreadsheet.batch_update({"requests":req})
print("Обновление завершено.")
