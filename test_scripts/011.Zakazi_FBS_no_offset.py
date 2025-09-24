# -*- coding: utf-8 -*-
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, timezone
import logging
from pytz import timezone as tz
from collections import defaultdict
import warnings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=DeprecationWarning, message="Parsing dates involving.*")

def read_api_keys(filename='(1)API.txt'):
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            lines = [line.strip() for line in file if line.strip()]
            if len(lines) >= 4:
                return lines[0], lines[1], lines[2], lines[3]
            raise ValueError("Файл должен содержать минимум 4 строки")
    except Exception as e:
        logger.error(f"Ошибка чтения файла {filename}: {e}")
        raise

def setup_google_sheets(sheet_id, target_sheet_name):
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)

        try:
            worksheet = spreadsheet.worksheet(target_sheet_name)
            logger.info(f"Найден лист '{target_sheet_name}'")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=target_sheet_name, rows="1000", cols="100")
            logger.info(f"Создан новый лист '{target_sheet_name}'")

        worksheet.batch_clear([
            "BT3:CU",
            "CX5:CX", "CZ5:CZ", "DB5:DB", "DD5:DD", "DF5:DF",
            "DG5:DG", "DH5:DH", "DI5:DI", "DJ5:DJ", "DK5:DK",
            "DO5:DO", "DQ5:DQ",
            "EC5:EF", "EC3:EF3",
            "EG5:EG", "EH5:EH", "EI5:EI", "EJ5:EJ"  # Добавлены новые столбцы для очистки
        ])
        logger.info("Очищены все диапазоны перед записью")

        return worksheet
    except Exception as e:
        logger.error(f"Ошибка Google Sheets: {e}")
        raise

def get_ozon_orders(client_id, api_key, since, to):
    url = "https://api-seller.ozon.ru/v3/posting/fbs/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }

    all_orders = []
    offset = 0
    limit = 1000

    while True:
        payload = {
            "dir": "DESC",
            "filter": {"since": since, "to": to},
            "limit": limit,
            "offset": offset,
            "with": {"analytics_data": False, "financial_data": False}
        }

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()

            if not data.get('result', {}).get('postings'):
                break

            all_orders.extend(data['result']['postings'])
            offset += limit

            if not data['result'].get('has_next', False):
                break

        except Exception as e:
            logger.error(f"Ошибка API Ozon: {e}")
            break

    return all_orders

def group_postings_by_datetime(orders, days=28):
    moscow_tz = tz('Europe/Moscow')
    utc_tz = timezone.utc

    today = datetime.now(moscow_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    date_range = [(today - timedelta(days=i)).strftime('%d.%m') for i in reversed(range(days))]

    datetime_postings = defaultdict(list)

    for order in orders:
        order_datetime = order.get('in_process_at', '')
        products = order.get('products', [])

        total_quantity = sum(int(product.get('quantity', 0)) for product in products)

        try:
            utc_time = datetime.fromisoformat(order_datetime.replace('Z', '+00:00')).replace(tzinfo=utc_tz)
            moscow_time = utc_time.astimezone(moscow_tz)
            date_key = moscow_time.strftime('%d.%m')

            if date_key in date_range:
                datetime_postings[date_key].append({
                    'quantity': total_quantity,
                    'full_date': moscow_time
                })

        except Exception as e:
            logger.warning(f"Ошибка обработки даты: {order_datetime} — {e}")
            continue

    result_postings = {}
    for date in date_range:
        result_postings[date] = datetime_postings.get(date, [])

    return date_range, result_postings

def map_offer_id_to_dates(orders):
    moscow_tz = tz('Europe/Moscow')
    utc_tz = timezone.utc
    offer_map = defaultdict(list)

    for order in orders:
        order_datetime = order.get('in_process_at', '')
        products = order.get('products', [])
        try:
            utc_time = datetime.fromisoformat(order_datetime.replace('Z', '+00:00')).replace(tzinfo=utc_tz)
            moscow_time = utc_time.astimezone(moscow_tz)
            date_key = moscow_time.strftime('%d.%m')

            for product in products:
                offer_id = product.get('offer_id')
                quantity = int(product.get('quantity', 0))
                if offer_id:
                    offer_map[offer_id].append({'date': date_key, 'quantity': quantity})
        except Exception as e:
            logger.warning(f"Ошибка обработки offer_id: {e}")
            continue
    return offer_map

def get_period_quantities(orders, days):
    moscow_tz = tz('Europe/Moscow')
    now = datetime.now(moscow_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = now - timedelta(days=days - 1)

    offer_totals = defaultdict(int)

    for order in orders:
        try:
            utc_time = datetime.fromisoformat(order['in_process_at'].replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
            moscow_time = utc_time.astimezone(moscow_tz)

            if start_date <= moscow_time <= now + timedelta(days=1):
                for product in order.get('products', []):
                    offer_id = product.get('offer_id')
                    quantity = int(product.get('quantity', 0))
                    if offer_id:
                        offer_totals[offer_id] += quantity
        except Exception as e:
            logger.warning(f"Ошибка при агрегации за {days} дней: {e}")

    return offer_totals

def get_cancellations_by_period(orders, days):
    moscow_tz = tz('Europe/Moscow')
    now = datetime.now(moscow_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = now - timedelta(days=days - 1)

    cancel_map = defaultdict(int)

    for order in orders:
        if order.get("status") != "cancelled":
            continue
        try:
            utc_time = datetime.fromisoformat(order['in_process_at'].replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
            moscow_time = utc_time.astimezone(moscow_tz)
            if start_date <= moscow_time <= now + timedelta(days=1):
                for product in order.get("products", []):
                    offer_id = product.get("offer_id")
                    quantity = int(product.get("quantity", 0))
                    if offer_id:
                        cancel_map[offer_id] += quantity
        except Exception as e:
            logger.warning(f"Ошибка при обработке отмены: {e}")
            continue

    return cancel_map

def get_delivered_by_period(orders, days):
    moscow_tz = tz('Europe/Moscow')
    now = datetime.now(moscow_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = now - timedelta(days=days - 1)

    delivered_map = defaultdict(int)

    for order in orders:
        if order.get("status") != "delivered":
            continue
        try:
            utc_time = datetime.fromisoformat(order['in_process_at'].replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
            moscow_time = utc_time.astimezone(moscow_tz)
            if start_date <= moscow_time <= now + timedelta(days=1):
                for product in order.get("products", []):
                    offer_id = product.get("offer_id")
                    quantity = int(product.get("quantity", 0))
                    if offer_id:
                        delivered_map[offer_id] += quantity
        except Exception as e:
            logger.warning(f"Ошибка при обработке доставки: {e}")
            continue

    return delivered_map

def write_to_sheet(worksheet, dates, datetime_postings, offer_map, orders):
    try:
        # 1. Записываем основные данные
        sum_row = [sum(item['quantity'] for item in datetime_postings.get(date, [])) for date in dates]
        worksheet.update(values=[sum_row], range_name='BT3')
        worksheet.update(values=[dates], range_name='BT4')

        offer_column = worksheet.col_values(4)[4:]
        data_to_write = []

        for offer_id in offer_column:
            row = []
            for date in dates:
                total = sum(entry['quantity'] for entry in offer_map.get(offer_id, []) if entry['date'] == date)
                row.append(total)
            data_to_write.append(row)

        if data_to_write:
            worksheet.update(values=data_to_write, range_name='BT5')
            logger.info(f"Записано {len(data_to_write)} строк данных по offer_id")

        # 2. Записываем агрегации по дням
        period_columns = {
            7: 'CX',
            14: 'CZ',
            28: 'DB',
            60: 'DD',
            90: 'DF',
        }

        for days, col_letter in period_columns.items():
            totals = get_period_quantities(orders, days)
            column_data = [[totals.get(offer_id, 0)] for offer_id in offer_column]
            worksheet.update(values=column_data, range_name=f'{col_letter}5')
            logger.info(f"Обновлён столбец {col_letter} — заказы за {days} дней")

        # 3. Записываем суммы DG–DK
        def get_column(col_letter):
            try:
                return worksheet.col_values(gspread.utils.a1_to_rowcol(f"{col_letter}1")[1])[4:]
            except:
                return []

        def safe_int(val):
            try:
                return int(val)
            except:
                return 0

        def sum_columns(col1, col2):
            return [[safe_int(a) + safe_int(b)] for a, b in zip(col1, col2)]

        col_pairs = {
            'DG': ('CW', 'CX'),
            'DH': ('CY', 'CZ'),
            'DI': ('DA', 'DB'),
            'DJ': ('DC', 'DD'),
            'DK': ('DE', 'DF')
        }

        for target_col, (left_col, right_col) in col_pairs.items():
            left_vals = get_column(left_col)
            right_vals = get_column(right_col)
            summed = sum_columns(left_vals, right_vals)
            worksheet.update(values=summed, range_name=f'{target_col}5')
            logger.info(f"Обновлён столбец {target_col} = {left_col} + {right_col}")

        # 4. Записываем отмены
        cancel_periods = {
            7: 'DO',
            28: 'DQ'
        }

        for days, col_letter in cancel_periods.items():
            cancellations = get_cancellations_by_period(orders, days)
            cancel_data = [[cancellations.get(offer_id, 0)] for offer_id in offer_column]
            worksheet.update(values=cancel_data, range_name=f"{col_letter}5")
            logger.info(f"Обновлён столбец {col_letter} — отмены за {days} дней")

        # 5. Записываем данные о доставке
        delivered_periods = {
            30: 'EC',
            60: 'ED',
            90: 'EE',
            180: 'EF'
        }

        for days, col_letter in delivered_periods.items():
            delivered = get_delivered_by_period(orders, days)
            delivered_data = [[delivered.get(offer_id, 0)] for offer_id in offer_column]
            worksheet.update(values=delivered_data, range_name=f"{col_letter}5")
            logger.info(f"Обновлён столбец {col_letter} — доставлено за {days} дней")

        # 6. Записываем даты начала периодов
        today = datetime.now(tz('Europe/Moscow'))
        period_dates_row = [[(today - timedelta(days=d-1)).strftime('%d.%m.%y')] for d in delivered_periods]
        worksheet.update(values=list(map(list, zip(*period_dates_row))), range_name="EC3:EF3")

        # 7. ТОЛЬКО ПОСЛЕ ВСЕХ ЗАПИСЕЙ - выполняем суммирование новых столбцов
        sum_pairs = {
            'EG': ('DY', 'EC'),  # DY + EC
            'EH': ('DZ', 'ED'),  # DZ + ED
            'EI': ('EA', 'EE'),  # EA + EE
            'EJ': ('EB', 'EF')   # EB + EF
        }

        for target_col, (left_col, right_col) in sum_pairs.items():
            left_vals = get_column(left_col)
            right_vals = get_column(right_col)
            summed = sum_columns(left_vals, right_vals)
            worksheet.update(values=summed, range_name=f'{target_col}5')
            logger.info(f"Обновлён столбец {target_col} = {left_col} + {right_col}")

        # 8. Форматирование (после всех записей)
        try:
            end_col = chr(ord('T') + len(dates))
            worksheet.format(f'BT3:{end_col}', {"numberFormat": {"type": "NUMBER"}})
            for col in list(period_columns.values()) + list(col_pairs.keys()) + list(cancel_periods.values()) + list(delivered_periods.values()) + list(sum_pairs.keys()):
                worksheet.format(f'{col}5:{col}', {"numberFormat": {"type": "NUMBER"}})
        except Exception as format_error:
            logger.warning(f"Форматирование не применено: {format_error}")

    except Exception as e:
        logger.error(f"Ошибка при записи в Google Sheets: {e}")
        raise

def main():
    try:
        client_id, api_key, sheet_id, target_sheet_name = read_api_keys()
        logger.info(f"Используем лист '{target_sheet_name}' в таблице с ID: {sheet_id}")

        worksheet = setup_google_sheets(sheet_id, target_sheet_name)

        moscow_tz = tz('Europe/Moscow')
        now_moscow = datetime.now(moscow_tz)
        since_date = (now_moscow - timedelta(days=170)).astimezone(timezone.utc).isoformat()
        to_date = now_moscow.astimezone(timezone.utc).isoformat()

        orders = get_ozon_orders(client_id, api_key, since_date, to_date)
        if not orders:
            logger.warning("Не получены заказы от API Ozon")
            return

        logger.info(f"Получено {len(orders)} заказов")

        dates, datetime_postings = group_postings_by_datetime(orders, days=28)
        offer_map = map_offer_id_to_dates(orders)

        write_to_sheet(worksheet, dates, datetime_postings, offer_map, orders)

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        logger.info("Скрипт завершил работу")

if __name__ == "__main__":
    main()