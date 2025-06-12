"""
report_scripts/p_campain_fin_1.py
─────────────────────────────────
Заполняет столбец F «Расходы на рекламу» в unit-day
по данным Performance API Ozon.

Оптимизированная версия максимально близкая к локальной
с улучшенной обработкой лимитов API и полным сбором данных.
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

# Увеличенные таймауты для стабильной работы
REQUEST_TIMEOUT = 60
RETRY_DELAY = 70  # Задержка при 429 ошибках
UUID_CHECK_INTERVAL = 120  # Интервал проверки UUID (как в локальной версии)


# ──────────────────────────── helpers ────────────────────────────
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def sleep_progress(sec: int, msg: str = ""):
    if msg:
        log(msg)
    for i in range(sec):
        time.sleep(1)
        print(".", end="", flush=True)
        if (i + 1) % 10 == 0:
            print(f" {i + 1}/{sec}")
    print()


def get_token(session: requests.Session, cid: str, secret: str) -> tuple[str, datetime]:
    """Получение токена с retry логикой"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            r = session.post(
                f"{API}/api/client/token",
                json={"client_id": cid, "client_secret": secret, "grant_type": "client_credentials"},
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=REQUEST_TIMEOUT,
            )
            if r.status_code == 429:
                sleep_progress(RETRY_DELAY, f"⚠️ 429 при получении токена (попытка {attempt + 1})")
                continue
            r.raise_for_status()
            token = r.json()["access_token"]
            log(f"✅ Токен получен ({token[:10]}...)")
            return token, datetime.now(UTC)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            log(f"⚠️ Ошибка получения токена (попытка {attempt + 1}): {e}")
            time.sleep(5)


def ensure_token(session: requests.Session,
                 token_time: datetime,
                 cid: str,
                 secret: str,
                 headers_cb: Callable[[dict], None]) -> datetime:
    """Обновляет токен, если прошло >25 мин (как в локальной версии)"""
    if (datetime.now(UTC) - token_time).total_seconds() <= 1500:  # 25 минут
        return token_time
    log("🔄 Обновление токена по времени...")
    new_token, new_time = get_token(session, cid, secret)
    headers_cb({"Authorization": f"Bearer {new_token}"})
    return new_time


def chunk(lst: list, n: int = 10):
    """Разбивка списка на чанки (как в локальной версии)"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ─────────────────── работа с Performance API ────────────────────
def fetch_campaigns(session: requests.Session, headers: dict) -> list[str]:
    """Получение списка кампаний с retry логикой"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            r = session.get(f"{API}/api/client/campaign", headers=headers, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                sleep_progress(RETRY_DELAY, f"⚠️ 429 при получении кампаний (попытка {attempt + 1})")
                continue
            r.raise_for_status()
            
            # Точно такие же состояния как в локальной версии
            target_states = {'CAMPAIGN_STATE_RUNNING', 'CAMPAIGN_STATE_STOPPED', 'CAMPAIGN_STATE_INACTIVE'}
            ids = [str(c["id"]) for c in r.json()["list"] if c.get("state") in target_states]
            log(f"📋 Найдено кампаний: {len(ids)} {ids}")
            return ids
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            log(f"⚠️ Ошибка получения кампаний (попытка {attempt + 1}): {e}")
            time.sleep(5)


def post_statistics(session: requests.Session,
                    headers: dict,
                    camp_ids: list[str],
                    date_from: str,
                    date_to: str) -> str:
    """Отправка запроса на статистику с полной retry логикой"""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            payload = {
                "campaigns": camp_ids,
                "dateFrom": date_from,
                "dateTo": date_to,
                "groupBy": "DATE"  # Точно как в локальной версии
            }
            
            r = session.post(
                f"{API}/api/client/statistics",
                headers=headers,
                json=payload,
                timeout=REQUEST_TIMEOUT
            )
            
            if r.status_code == 429:
                sleep_progress(RETRY_DELAY, f"⚠️ 429 при запросе статистики (попытка {attempt + 1})")
                continue
                
            r.raise_for_status()
            uuid = r.json()["UUID"]
            log(f"📥 UUID получен: {uuid}")
            return uuid
            
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            log(f"⚠️ Ошибка запроса статистики (попытка {attempt + 1}): {e}")
            time.sleep(10)


def wait_uuid(session: requests.Session,
              uuid: str,
              headers_fn: Callable[[], dict],
              refresh_token_fn: Callable[[], None]):
    """Ожидание готовности UUID - точная копия логики из локальной версии"""
    url = f"{API}/api/client/statistics/{uuid}"
    log(f"⏳ Ждём готовности UUID: {uuid}")
    
    while True:
        try:
            # Обновляем токен если нужно
            refresh_token_fn()
            
            r = session.get(url, headers=headers_fn(), timeout=REQUEST_TIMEOUT)
            
            if r.status_code == 429:
                sleep_progress(RETRY_DELAY, "⚠️ 429 при проверке UUID")
                continue
                
            if r.status_code == 403:
                log("⚠️ 403 при проверке UUID - принудительное обновление токена")
                refresh_token_fn()
                continue
                
            r.raise_for_status()
            
            data = r.json()
            state = data.get("state")
            log(f"🔍 UUID {uuid} → state: {state}")
            
            if state == "OK":
                log(f"✅ Отчёт готов: {uuid}")
                return
                
            if state == "FAILED":
                raise RuntimeError(f"❌ UUID {uuid} завершился с ошибкой")
                
        except Exception as e:
            log(f"⚠️ Исключение при проверке UUID {uuid}: {e}")
        
        # Интервал как в локальной версии
        time.sleep(UUID_CHECK_INTERVAL)


def download_zip(session: requests.Session, headers: dict, uuid: str) -> bytes:
    """Скачивание ZIP отчёта с retry логикой"""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            r = session.get(
                f"{API}/api/client/statistics/report",
                headers=headers, 
                params={"UUID": uuid}, 
                timeout=REQUEST_TIMEOUT
            )
            
            if r.status_code == 429:
                sleep_progress(RETRY_DELAY, f"⚠️ 429 при скачивании ZIP (попытка {attempt + 1})")
                continue
                
            if r.status_code == 403:
                log(f"⚠️ 403 при скачивании ZIP {uuid} - возможно токен устарел")
                raise requests.exceptions.HTTPError("403 Forbidden")
                
            r.raise_for_status()
            
            if "application/zip" not in r.headers.get("Content-Type", ""):
                raise RuntimeError(f"Неожиданный тип контента для UUID {uuid}")
                
            log(f"📦 ZIP скачан для UUID {uuid}")
            return r.content
            
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            log(f"⚠️ Ошибка скачивания ZIP (попытка {attempt + 1}): {e}")
            time.sleep(10)


# ─────────────────────── CSV → DataFrame ────────────────────────
def parse_zip(data: bytes) -> pd.DataFrame:
    """Парсинг ZIP - максимально близко к локальной версии"""
    all_data = []
    
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zip_ref:
            for file_name in zip_ref.namelist():
                if not (file_name.endswith('.csv') or file_name.endswith('.txt')):
                    continue
                    
                try:
                    with zip_ref.open(file_name) as f:
                        decoded = io.TextIOWrapper(f, encoding='utf-8')
                        df = pd.read_csv(decoded, sep=';', skiprows=1)
                        
                        # Точные названия колонок как в локальной версии
                        required_columns = ['День', 'sku', 'Расход, ₽, с НДС']
                        if not all(col in df.columns for col in required_columns):
                            log(f"⚠️ Файл {file_name} не содержит необходимых колонок")
                            continue
                            
                        df_selected = df[required_columns].copy()
                        
                        # Фильтрация точно как в локальной версии
                        df_selected = df_selected[df_selected['День'] != 'Всего']
                        df_selected = df_selected[
                            df_selected['sku'].notna() & 
                            ~df_selected['sku'].isin([float('inf'), -float('inf')])
                        ]
                        
                        if df_selected.empty:
                            continue
                            
                        # Преобразование типов точно как в локальной версии
                        try:
                            df_selected['sku'] = df_selected['sku'].astype(int)
                        except Exception as e:
                            log(f"⚠️ Ошибка преобразования SKU в файле {file_name}: {e}")
                            continue
                            
                        all_data.append(df_selected)
                        
                except Exception as e:
                    log(f"⚠️ Ошибка обработки файла {file_name}: {e}")
                    continue
                    
    except zipfile.BadZipFile as e:
        log(f"❌ Неверный ZIP файл: {e}")
        return pd.DataFrame(columns=['date', 'sku', 'rub'])
    
    if not all_data:
        log("⚠️ Нет данных для обработки")
        return pd.DataFrame(columns=['date', 'sku', 'rub'])
    
    # Объединение и группировка точно как в локальной версии
    combined_df = pd.concat(all_data, ignore_index=True)
    
    try:
        # Преобразование расходов точно как в локальной версии
        combined_df['Расход, ₽, с НДС'] = combined_df['Расход, ₽, с НДС'].replace({',': '.'}, regex=True).astype(float)
    except Exception as e:
        log(f"❌ Ошибка преобразования столбца расходов: {e}")
        return pd.DataFrame(columns=['date', 'sku', 'rub'])
    
    # Группировка точно как в локальной версии
    grouped_df = combined_df.groupby(['День', 'sku'], as_index=False)['Расход, ₽, с НДС'].sum()
    grouped_df.columns = ['date', 'sku', 'rub']
    
    log(f"📊 Обработано строк: {len(grouped_df)}")
    return grouped_df


# ─────────────────────── запись в Google Sheets ──────────────────────
def write_sheet(gs_cred: str, spread_id: str, sheet_name: str, df: pd.DataFrame):
    """Запись в Google Sheets - логика из локальной версии"""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(gs_cred, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(spread_id).worksheet(sheet_name)
        
        # Получаем существующие данные
        existing_values = sheet.get_all_values()
        if not existing_values:
            log("⚠️ Лист пуст")
            return
            
        sheet_df = pd.DataFrame(existing_values[1:], columns=existing_values[0])
        
        # Проверяем необходимые колонки
        required_sheet_columns = ['Дата обновления', 'SKU']
        if not all(col in sheet_df.columns for col in required_sheet_columns):
            raise RuntimeError(f"❌ Не найдены колонки {required_sheet_columns} в Google Таблице")
        
        # Преобразование данных точно как в локальной версии
        sheet_df['Дата обновления'] = sheet_df['Дата обновления'].apply(
            lambda x: x.split(' ')[0] if isinstance(x, str) and ' ' in x else x
        )
        
        try:
            sheet_df['SKU'] = pd.to_numeric(sheet_df['SKU'], errors='coerce').astype('Int64')
        except Exception as e:
            log(f"❌ Ошибка преобразования SKU: {e}")
            return
        
        # Подготовка данных для записи - точно как в локальной версии
        sheet_data = sheet_df[['Дата обновления', 'SKU']].copy()
        sheet_data['row_index'] = sheet_data.index + 2  # Индексы строк в Google Таблице
        sheet_data['rub'] = None
        
        matches_found = 0
        for _, row in df.iterrows():
            date = row['date']
            sku = row['sku']
            rub = row['rub']
            match = sheet_data[
                (sheet_data['Дата обновления'] == date) & 
                (sheet_data['SKU'] == sku)
            ]
            if not match.empty:
                sheet_data.loc[match.index, 'rub'] = rub
                matches_found += 1
        
        log(f"🔍 Найдено совпадений: {matches_found}")
        
        # Массовая запись точно как в локальной версии
        update_data = [[row['rub'] if row['rub'] is not None else ''] for _, row in sheet_data.iterrows()]
        update_range = f'F2:F{len(sheet_data) + 1}'
        
        sheet.update(range_name=update_range, values=update_data)
        log(f"✅ Записано в Google Таблицу: {matches_found} значений")
        
    except Exception as e:
        log(f"❌ Ошибка записи в Google Таблицу: {e}")
        raise


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
    """Основная функция - максимально близко к локальной версии"""
    log("🚀 Запуск p_campain_fin_1")
    
    session = requests.Session()
    
    # Получаем начальный токен
    token, token_time = get_token(session, perf_client_id, perf_client_secret)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    
    # Функции для работы с токеном
    def update_headers(new_headers: dict):
        headers.update(new_headers)
    
    def get_headers() -> dict:
        return headers.copy()
    
    def refresh_token():
        nonlocal token_time
        token_time = ensure_token(session, token_time, perf_client_id, perf_client_secret, update_headers)
    
    try:
        # 1. Получаем кампании
        campaign_ids = fetch_campaigns(session, get_headers())
        if not campaign_ids:
            log("❌ Нет активных кампаний")
            return
        
        # 2. Формируем даты (точно как в локальной версии)
        now_utc = datetime.now(timezone.utc)
        msk_offset = timedelta(hours=3)
        now_msk = now_utc + msk_offset
        date_to = now_msk.date()
        date_from = (now_msk - timedelta(days=days)).date()
        date_from_str = date_from.strftime("%Y-%m-%d")
        date_to_str = date_to.strftime("%Y-%m-%d")
        
        log(f"📅 Период: {date_from_str} - {date_to_str}")
        
        # 3. Собираем UUID для каждого чанка кампаний
        uuids = []
        for chunk_campaigns in chunk(campaign_ids, 10):  # 10 кампаний в чанке как в локальной версии
            try:
                # Проверяем токен перед запросом
                refresh_token()
                
                uuid = post_statistics(session, get_headers(), chunk_campaigns, date_from_str, date_to_str)
                
                # Ждём готовности UUID
                wait_uuid(session, uuid, get_headers, refresh_token)
                
                uuids.append(uuid)
                log(f"✅ UUID {uuid} готов к скачиванию")
                
            except Exception as e:
                log(f"❌ Ошибка обработки чанка кампаний {chunk_campaigns}: {e}")
                continue
        
        if not uuids:
            log("❌ Не получено ни одного UUID")
            return
        
        log(f"📊 Всего UUID для скачивания: {len(uuids)}")
        
        # 4. Скачиваем и обрабатываем все ZIP файлы
        all_dataframes = []
        for uuid in uuids:
            try:
                refresh_token()  # Обновляем токен перед каждым скачиванием
                zip_data = download_zip(session, get_headers(), uuid)
                df = parse_zip(zip_data)
                if not df.empty:
                    all_dataframes.append(df)
                    log(f"✅ UUID {uuid}: получено {len(df)} строк")
                else:
                    log(f"⚠️ UUID {uuid}: нет данных")
            except Exception as e:
                log(f"❌ Ошибка обработки UUID {uuid}: {e}")
                continue
        
        if not all_dataframes:
            log("❌ Нет данных для записи")
            return
        
        # 5. Объединяем все данные
        final_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Финальная группировка по дате и SKU
        final_df = final_df.groupby(['date', 'sku'], as_index=False)['rub'].sum()
        
        log(f"📊 Итого строк для записи: {len(final_df)}")
        log(f"💰 Общая сумма расходов: {final_df['rub'].sum():.2f} ₽")
        
        # 6. Записываем в Google Sheets
        write_sheet(gs_cred, spread_id, sheet_main, final_df)
        
        log("✅ p_campain_fin_1 успешно завершён")
        
    except Exception as e:
        log(f"❌ Критическая ошибка: {e}")
        raise
