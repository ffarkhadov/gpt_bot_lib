import requests
import json
import pandas as pd
import os
from datetime import datetime, timezone, timedelta
import zipfile
import io
import time
import gspread
from google.oauth2.service_account import Credentials

# Токен и Client ID
client_secret = 'M6KXzabQefsFBJSQskwiCcIdEug0FfpY8JkceOGLcRaeGRk8_K4pi9VUUi4GEpF-WKfBYsaham1RQW4GyQ'
client_id = '67428955-1747226962895@advertising.performance.ozon.ru'

endpoint = '/api/client/token'
host = 'https://api-performance.ozon.ru'

headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

# Функция для получения/обновления access_token
def refresh_access_token():
    global headers, access_token, token_time
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    response = requests.post(host + endpoint, headers=headers, json=payload)
    if response.status_code == 200:
        data = response.json()
        access_token = data['access_token']
        token_time = datetime.now(timezone.utc)  # Сохраняем время получения токена
        headers['Authorization'] = f'Bearer {access_token}'  # Обновляем заголовки
        print(f"🔄 Токен успешно обновлён: {access_token[:30]}...")
        return True
    else:
        print(f"❌ Ошибка при обновлении токена: {response.status_code} — {response.text}")
        return False

# Определяем payload для начального запроса токена
payload = {
    "client_id": client_id,
    "client_secret": client_secret,
    "grant_type": "client_credentials"
}

# Получаем начальный токен
response = requests.post(host + endpoint, headers=headers, json=payload)
if response.status_code == 200:
    data = response.json()
    access_token = data['access_token']
    token_time = datetime.now(timezone.utc)  # Время получения токена
    headers['Authorization'] = f'Bearer {access_token}'
    print(f"✅ Начальный токен получен: {access_token[:30]}...")
else:
    print(f"❌ Ошибка получения начального токена: {response.status_code} — {response.text}")
    exit()

# Получаем список кампаний
url = 'https://api-performance.ozon.ru:443/api/client/campaign'
response = requests.get(url, headers=headers)
data = response.json()
target_states = {'CAMPAIGN_STATE_RUNNING', 'CAMPAIGN_STATE_STOPPED', 'CAMPAIGN_STATE_INACTIVE'}
campaign_ids = [item['id'] for item in data['list'] if item.get('state') in target_states]
print(f"📋 ID кампаний: {campaign_ids}")

# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===
def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def wait_for_ready(uuid, check_interval=120):
    global headers
    url = f'https://api-performance.ozon.ru:443/api/client/statistics/{uuid}'
    print(f"⏳ Ждём готовности UUID: {uuid}")
    while True:
        # Проверяем, не истёк ли токен (25 минут)
        if (datetime.now(timezone.utc) - token_time).total_seconds() > 1500:  # 25 минут
            print("⚠️ Токен истёк по времени, обновляем...")
            if not refresh_access_token():
                print(f"❌ Не удалось обновить токен для UUID {uuid}")
                return None

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 403:  # Токен недействителен
                print(f"⚠️ Ошибка 403 для UUID {uuid}, обновляем токен...")
                if not refresh_access_token():
                    print(f"❌ Не удалось обновить токен для UUID {uuid}")
                    return None
                continue  # Повторяем запрос с новым токеном

            if response.status_code != 200:
                print(f"❌ Ошибка запроса UUID {uuid}: {response.status_code}")
                time.sleep(check_interval)
                continue

            data = response.json()
            state = data.get("state")
            print(f"🔍 UUID {uuid} → state: {state}")

            if state == "OK":
                print(f"✅ Отчёт готов: {uuid}")
                return data

            if state == "FAILED":
                print(f"❌ Ошибка формирования отчёта UUID {uuid}")
                return None

        except Exception as e:
            print(f"⚠️ Исключение при проверке UUID {uuid}: {e}")

        time.sleep(check_interval)

# === ОСНОВНОЙ ЦИКЛ ===
uuids = []

def get_moscow_dates(days=7):
    now_utc = datetime.now(timezone.utc)
    msk_offset = timedelta(hours=3)
    now_msk = now_utc + msk_offset
    date_to = now_msk.date()
    date_from = (now_msk - timedelta(days=days)).date()
    return date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d")

date_from, date_to = get_moscow_dates(7)

for chunk in chunk_list(campaign_ids, 10):
    # Проверяем, не истёк ли токен перед отправкой запроса
    if (datetime.now(timezone.utc) - token_time).total_seconds() > 1500:
        print("⚠️ Токен истёк по времени перед запросом статистики, обновляем...")
        if not refresh_access_token():
            print("❌ Не удалось обновить токен, пропускаем чанк")
            continue

    payload = {
        "campaigns": chunk,
        "dateFrom": date_from,
        "dateTo": date_to,
        "groupBy": "DATE"
    }

    url = 'https://api-performance.ozon.ru:443/api/client/statistics'
    print(f"\n📤 Отправка запроса на статистику для кампаний: {chunk}")
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        uuid = response.json().get("UUID")
        print(f"📥 Получен UUID: {uuid}")
        result = wait_for_ready(uuid)
        if result:
            uuids.append(uuid)
    else:
        print(f"❌ Ошибка при отправке запроса: {response.status_code} — {response.text}")

# === ИТОГ ===
print("\n📊 Все собранные UUID:")
print(uuids)

# --- Настройка Google Sheets API ---
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials_file = 'E:/Ozon_API/teak-digit-438912-s0-cc0207cd6ee3.json'

try:
    creds = Credentials.from_service_account_file(credentials_file, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key('1pkR_vV-g0cI8AUdQEAw2VTS4tBe78t5gD-ExqUnrpfo').worksheet('unit-day')
except Exception as e:
    print(f"❌ Ошибка при аутентификации или открытии Google Таблицы: {e}")
    exit()

all_data = []

for uuid in uuids:
    # Проверяем токен перед запросом отчёта
    if (datetime.now(timezone.utc) - token_time).total_seconds() > 1500:
        print("⚠️ Токен истёк по времени перед запросом отчёта, обновляем...")
        if not refresh_access_token():
            print(f"❌ Не удалось обновить токен для UUID {uuid}")
            continue

    url = 'https://api-performance.ozon.ru/api/client/statistics/report'
    params = {'UUID': uuid}

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 403:  # Пробуем обновить токен при 403
        print(f"⚠️ Ошибка 403 для UUID {uuid} при получении отчёта, обновляем токен...")
        if not refresh_access_token():
            print(f"❌ Не удалось обновить токен для UUID {uuid}")
            continue
        response = requests.get(url, headers=headers, params=params)  # Повторяем запрос

    if response.status_code == 200 and 'application/zip' in response.headers.get('Content-Type', ''):
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                for file_name in zip_ref.namelist():
                    if file_name.endswith('.csv') or file_name.endswith('.txt'):
                        with zip_ref.open(file_name) as f:
                            decoded = io.TextIOWrapper(f, encoding='utf-8')
                            df = pd.read_csv(decoded, sep=';', skiprows=1)

                            required_columns = ['День', 'sku', 'Расход, ₽, с НДС']
                            if not all(col in df.columns for col in required_columns):
                                continue

                            df_selected = df[required_columns].copy()
                            df_selected = df_selected[df_selected['День'] != 'Всего']
                            df_selected = df_selected[
                                df_selected['sku'].notna() & ~df_selected['sku'].isin([float('inf'), -float('inf')])]

                            try:
                                df_selected['sku'] = df_selected['sku'].astype(int)
                            except Exception:
                                continue

                            all_data.append(df_selected)
        except zipfile.BadZipFile:
            pass
    else:
        print(f"❌ Ошибка получения отчёта для UUID {uuid}: {response.status_code}")

# Объединяем данные
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)

    try:
        combined_df['Расход, ₽, с НДС'] = combined_df['Расход, ₽, с НДС'].replace({',': '.'}, regex=True).astype(float)
    except Exception as e:
        print(f"❌ Ошибка преобразования столбца 'Расход, ₽, с НДС': {e}")
        exit()

    grouped_df = combined_df.groupby(['День', 'sku'], as_index=False)['Расход, ₽, с НДС'].sum()
    grouped_df.columns = ['date', 'sku', 'rub']

    # --- Работа с Google Таблицей ---
    existing_values = sheet.get_all_values()
    sheet_df = pd.DataFrame(existing_values[1:], columns=existing_values[0])

    required_sheet_columns = ['Дата обновления', 'SKU']
    if not all(col in sheet_df.columns for col in required_sheet_columns):
        print(f"❌ Ошибка: Не найдены столбцы {required_sheet_columns} в Google Таблице")
        exit()

    # Преобразуем даты и SKU
    sheet_df['Дата обновления'] = sheet_df['Дата обновления'].apply(
        lambda x: x.split(' ')[0] if isinstance(x, str) and ' ' in x else x)
    try:
        sheet_df['SKU'] = pd.to_numeric(sheet_df['SKU'], errors='coerce').astype('Int64')
    except Exception as e:
        print(f"❌ Ошибка преобразования столбца 'SKU': {e}")
        exit()

    # Подготовка данных для массовой записи
    updates = []
    sheet_data = sheet_df[['Дата обновления', 'SKU']].copy()
    sheet_data['row_index'] = sheet_data.index + 2  # Индексы строк в Google Таблице
    sheet_data['rub'] = None

    for _, row in grouped_df.iterrows():
        date = row['date']
        sku = row['sku']
        rub = row['rub']
        match = sheet_data[(sheet_data['Дата обновления'] == date) & (sheet_data['SKU'] == sku)]
        if not match.empty:
            sheet_data.loc[match.index, 'rub'] = rub

    # Формируем данные для столбца F
    update_data = [[row['rub'] if row['rub'] is not None else ''] for _, row in sheet_data.iterrows()]
    update_range = f'F2:F{len(sheet_data) + 1}'

    # Массовая запись в столбец F
    try:
        sheet.update(range_name=update_range, values=update_data)
        print("\n✅ Данные записаны в Google Таблицу")
    except Exception as e:
        print(f"❌ Ошибка записи в Google Таблицу: {e}")
        exit()
else:
    print("\n❌ Данные не получены, отчёт не создан")