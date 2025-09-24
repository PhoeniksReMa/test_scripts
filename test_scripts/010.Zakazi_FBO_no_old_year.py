import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, timezone
import logging
import pytz
import time
from collections import defaultdict
from dateutil.relativedelta import relativedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

def read_api_credentials(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = [line.strip() for line in file.readlines() if line.strip()]
        if len(lines) >= 4:
            return lines[0], lines[1], lines[2], lines[3]
        raise ValueError("Файл должен содержать 4 строки: client_id, API ключ, ID таблицы и название листа")

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

def get_last_year_month_range(month_offset):
    today = datetime.now(pytz.timezone('Europe/Moscow'))
    target_month = (today.replace(day=1) + relativedelta(months=month_offset)).replace(year=today.year - 1)
    start = target_month
    end = (target_month + relativedelta(months=1)) - timedelta(seconds=1)
    return start, end

def get_delivered_count_by_sku(api, start_dt, end_dt):
    offset = 0
    limit = 1000
    postings = []
    logging.info(f"Загрузка данных с {start_dt.strftime('%d.%m.%Y')} по {end_dt.strftime('%d.%m.%Y')}")
    while True:
        data = {
            "dir": "DESC",
            "filter": {
                "since": start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "to": end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            },
            "limit": limit,
            "offset": offset,
            "translit": True,
            "with": {"analytics_data": True, "financial_data": True}
        }
        response = api.get_fbo_posting_list(data)
        if not response:
            break
        new_postings = response['result'] if isinstance(response['result'], list) else response['result'].get('postings', [])
        if not new_postings:
            break
        postings.extend(new_postings)
        offset += limit
        if len(new_postings) < limit:
            break
        time.sleep(0.5)

    counts = defaultdict(int)
    for posting in postings:
        if posting.get('status') == 'delivered':
            for product in posting.get('products', []):
                counts[product['offer_id']] += product.get('quantity', 0)
    logging.info(f"Загружено {len(postings)} доставленных заказов")
    return counts

def main():
    try:
        logging.info("Чтение конфигурации API и подключение к Google Sheets...")
        client_id, api_key, sheet_id, sheet_name = read_api_credentials('(1)API.txt')
        ozon_api = OzonAPI(client_id, api_key)

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sheet_id).worksheet(sheet_name)

        skus = sheet.col_values(4)[4:]  # Столбец D начиная с 5 строки

        # Очистка диапазонов перед записью данных
        logging.info("Очистка старых данных из столбцов EL, EN, EP...")
        clear_ranges = ['EL5:EL', 'EN5:EN', 'EP5:EP']
        sheet.batch_clear(clear_ranges)

        data_to_write = []

        # Обновлённые столбцы: EL, EN, EP
        periods = {
            'EL': get_last_year_month_range(1),
            'EN': get_last_year_month_range(2),
            'EP': get_last_year_month_range(3)
        }

        month_names = {
            1: "январь", 2: "февраль", 3: "март", 4: "апрель",
            5: "май", 6: "июнь", 7: "июль", 8: "август",
            9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь"
        }

        # Обновлённые диапазоны для записи названий месяцев
        month_ranges = {
            'EL': 'EL2:EM2',
            'EN': 'EN2:EO2',
            'EP': 'EP2:EQ2'
        }

        for col, (start_dt, _) in periods.items():
            month_name = month_names[start_dt.month].capitalize()
            data_to_write.append({'range': month_ranges[col], 'values': [[month_name, month_name]]})

        for col, (start_dt, end_dt) in periods.items():
            logging.info(f"Обработка столбца {col}: период {start_dt.strftime('%B %Y')}")
            delivered_counts = get_delivered_count_by_sku(ozon_api, start_dt, end_dt)
            for idx, sku in enumerate(skus, start=5):
                if not sku:
                    continue
                data_to_write.append({'range': f'{col}{idx}', 'values': [[delivered_counts.get(sku, 0)]]})
            date_range_str = f"{start_dt.strftime('%d.%m.%y')} {end_dt.strftime('%d.%m.%y')}"
            data_to_write.append({'range': f'{col}3', 'values': [[date_range_str]]})

        logging.info("Обновление Google Sheets...")
        sheet.batch_update(data_to_write)
        logging.info("Данные успешно обновлены в таблице.")

    except Exception as e:
        logging.error(f"Ошибка: {e}", exc_info=True)

if __name__ == "__main__":
    main()
