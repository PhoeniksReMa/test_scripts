import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, timezone
import logging
import pytz
import time
from collections import defaultdict
from dateutil.relativedelta import relativedelta

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

def read_api_credentials(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = [line.strip() for line in file.readlines() if line.strip()]
            if len(lines) >= 4:
                return lines[0], lines[1], lines[2], lines[3]
            raise ValueError("Файл должен содержать 4 строки: client_id, API ключ, ID таблицы и название листа")
    except Exception as e:
        logging.error(f"Ошибка при чтении API: {e}")
        raise

class OzonAPI:
    def __init__(self, client_id, api_key):
        self.client_id = client_id
        self.api_key = api_key
        self.base_url = "https://api-seller.ozon.ru"

    def get_fbo_posting_list(self, data):
        url = f"{self.base_url}/v2/posting/fbo/list"
        headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json"
        }
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка API: {e}")
            return None

def convert_to_moscow_date(utc_time_str):
    try:
        utc_time = datetime.strptime(utc_time_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
        return utc_time.astimezone(pytz.timezone('Europe/Moscow'))
    except Exception as e:
        logging.error(f"Конверт времени: {e}")
        return None

def prepare_data_for_sheet(all_postings, days=28):
    today = datetime.now(timezone.utc)
    order_counts = defaultdict(lambda: defaultdict(int))
    date_headers = [(today - timedelta(days=i)).strftime('%d.%m') for i in reversed(range(days))]
    date_strings = [(today - timedelta(days=i)).strftime('%d.%m.%Y') for i in reversed(range(days))]

    for posting in all_postings:
        posting_date_utc = convert_to_moscow_date(posting.get('in_process_at', ''))
        if not posting_date_utc:
            continue
        posting_date_str = posting_date_utc.strftime('%d.%m.%Y')
        for product in posting.get('products', []):
            order_counts[product.get('offer_id', '')][posting_date_str] += product.get('quantity', 0)
    return date_headers, date_strings, order_counts

def get_month_ranges(start_date, end_date):
    ranges = []
    current = start_date
    while current < end_date:
        next_month = current + relativedelta(months=1)
        ranges.append((current, min(next_month, end_date)))
        current = next_month
    return ranges

def main():
    start_time = time.time()
    try:
        client_id, api_key, sheet_id, sheet_name = read_api_credentials('(1)API.txt')
        ozon_api = OzonAPI(client_id, api_key)

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sheet_id).worksheet(sheet_name)

        # Очистка
        sheet.batch_clear(['AQ3:BR', 'CW5:CW', 'CY5:CY', 'DA5:DA', 'DC5:DC', 'DE5:DE',
                           'DN5:DN', 'DP5:DP', 'DY5:DY', 'DZ5:DZ', 'EA5:EA', 'EB5:EB', 'DL5:DL'])

        today = datetime.now(timezone.utc)
        start_date = (today - timedelta(days=170)).replace(tzinfo=timezone.utc)
        end_date = today.replace(tzinfo=timezone.utc)

        all_postings = []
        limit = 1000
        month_ranges = get_month_ranges(start_date, end_date)

        for month_start, month_end in month_ranges:
            offset = 0
            while True:
                data = {
                    "dir": "DESC",
                    "filter": {
                        "since": month_start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        "to": month_end.strftime('%Y-%m-%dT%H:%M:%SZ')
                    },
                    "limit": limit,
                    "offset": offset,
                    "translit": True,
                    "with": {"analytics_data": True, "financial_data": True}
                }
                response = ozon_api.get_fbo_posting_list(data)
                if not response:
                    break
                postings = response['result'] if isinstance(response['result'], list) else response['result'].get('postings', [])
                if not postings:
                    break
                all_postings.extend(postings)
                offset += limit
                if len(postings) < limit:
                    break
                time.sleep(0.5)

        if not all_postings:
            return

        date_headers, date_strings, order_counts = prepare_data_for_sheet(all_postings)
        skus = sheet.col_values(4)[4:]

        data_to_write = [{'range': 'AQ4:BR4', 'values': [date_headers]}]

        all_rows = []
        for idx, sku in enumerate(skus, start=5):
            if not sku:
                continue
            row_data = [order_counts[sku].get(date_str, 0) for date_str in date_strings]
            data_to_write.append({'range': f'AQ{idx}:BR{idx}', 'values': [row_data]})
            all_rows.append(row_data)

        if all_rows:
            column_sums = [sum(col) for col in zip(*all_rows)]
            data_to_write.append({'range': 'AQ3:BR3', 'values': [column_sums]})

        moscow_tz = pytz.timezone('Europe/Moscow')
        today_moscow = datetime.now(moscow_tz).replace(hour=0, minute=0, second=0, microsecond=0)

        periods = {'CW': 7, 'CY': 14, 'DA': 28, 'DC': 60, 'DE': 90}
        delivered_periods = {'DY': 30, 'DZ': 60, 'EA': 90, 'EB': 180}
        cancellation_periods = {'DN': 7, 'DP': 28}

        period_counts = {col: defaultdict(int) for col in periods}
        delivered_counts = {col: defaultdict(int) for col in delivered_periods}
        cancellation_counts = {col: defaultdict(int) for col in cancellation_periods}

        for posting in all_postings:
            posting_date = convert_to_moscow_date(posting.get('in_process_at', ''))
            if not posting_date:
                continue

            for col, days in periods.items():
                if today_moscow - timedelta(days=days - 1) <= posting_date <= today_moscow + timedelta(days=1):
                    for product in posting.get('products', []):
                        period_counts[col][product['offer_id']] += product['quantity']

            if posting.get('status') == 'delivered':
                for col, days in delivered_periods.items():
                    if today_moscow - timedelta(days=days - 1) <= posting_date <= today_moscow + timedelta(days=1):
                        for product in posting.get('products', []):
                            delivered_counts[col][product['offer_id']] += product['quantity']

            if posting.get('status') == 'cancelled':
                for col, days in cancellation_periods.items():
                    if today_moscow - timedelta(days=days - 1) <= posting_date <= today_moscow + timedelta(days=1):
                        for product in posting.get('products', []):
                            cancellation_counts[col][product['offer_id']] += product['quantity']

        for idx, sku in enumerate(skus, start=5):
            if not sku:
                continue
            for col in periods:
                data_to_write.append({'range': f'{col}{idx}', 'values': [[period_counts[col].get(sku, 0)]]})
            for col in cancellation_periods:
                data_to_write.append({'range': f'{col}{idx}', 'values': [[cancellation_counts[col].get(sku, 0)]]})
            for col in delivered_periods:
                data_to_write.append({'range': f'{col}{idx}', 'values': [[delivered_counts[col].get(sku, 0)]]})

        # Заменили %Y на %y для двухзначного года
        for col, days in delivered_periods.items():
            start_period = (today_moscow - timedelta(days=days - 1)).strftime('%d.%m.%y')
            data_to_write.append({'range': f'{col}3', 'values': [[start_period]]})

        sheet.batch_update(data_to_write)

    except Exception as e:
        logging.error(f"Ошибка: {e}", exc_info=True)
    finally:
        logging.info(f"Скрипт завершён за {time.time() - start_time:.2f} секунд")

if __name__ == "__main__":
    main()
