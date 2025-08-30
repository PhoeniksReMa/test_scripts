# -*- coding: utf-8 -*-
"""
Скрипт для выгрузки данных об остатках с Ozon в Google Таблицы
Записывает:
- 'ads' в столбец AJ
- 'idc' (Дней хватит) в столбец AK с округлением в большую сторону
- 'turnover_grade' в столбец AL с переводом значений
"""

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time
import sys
import math

# Словарь перевода для turnover_grade
TURNOVER_GRADE_TRANSLATION = {
    'GRADES_NONE': 'ожидаются поставки',
    'GRADES_NOSALES': 'нет продаж',
    'GRADES_GREEN': 'хороший',
    'GRADES_YELLOW': 'средний',
    'GRADES_RED': 'плохой',
    'GRADES_CRITICAL': 'критич.',
    '': ''
}

def read_api_credentials(file_path='(1)API.txt'):
    """Чтение учетных данных из файла"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = [line.strip() for line in file.readlines() if line.strip()]
            
            if len(lines) >= 4:
                return {
                    'client_id': lines[0],          # ID клиента Ozon
                    'api_key': lines[1],            # API ключ Ozon
                    'spreadsheet_id': lines[2],     # ID Google таблицы
                    'sheet_name': lines[3]          # Название листа
                }
            raise ValueError("Файл (1)API.txt должен содержать 4 строки с данными")
    except FileNotFoundError:
        raise FileNotFoundError(f"Файл {file_path} не найден")

def get_all_ozon_data(client_id, api_key, max_retries=3):
    """Получение данных об остатках с Ozon API"""
    url = "https://api-seller.ozon.ru/v1/analytics/turnover/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }

    all_items = []
    offset = 0
    retry_count = 0

    while True:
        try:
            payload = {"limit": 1000, "offset": offset}
            print(f"Запрос данных (offset: {offset})...")

            response = requests.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()

            data = response.json()
            items = data.get('items', [])

            if not items:
                break

            all_items.extend(items)
            offset += len(items)

            print(f"Получено {len(items)} товаров (всего: {len(all_items)})")
            if len(items) < 1000:
                break

            time.sleep(65)  # Лимит API - 1 запрос в 65 секунд

        except requests.exceptions.RequestException as e:
            retry_count += 1
            if retry_count > max_retries:
                raise
            print(f"Ошибка ({retry_count}/{max_retries}): {str(e)}")
            time.sleep(60)
            continue

    return all_items

def update_google_sheets(items, credentials, start_row=5):
    """Обновление Google Таблицы"""
    try:
        # Авторизация в Google Sheets
        scope = ['https://spreadsheets.google.com/feeds', 
                'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            'credentials.json', scope)
        client = gspread.authorize(creds)
        
        # Открытие таблицы
        sheet = client.open_by_key(credentials['spreadsheet_id']).worksheet(
            credentials['sheet_name'])

        # Получение offer_id из колонки D
        offer_ids = sheet.col_values(4)[start_row-1:]  # Колонка D

        # Подготовка данных для записи
        data_to_write = []
        
        for offer_id in offer_ids:
            offer_id = str(offer_id).strip()
            item = next((item for item in items if item.get('offer_id') == offer_id), None)
            
            # Формируем строку данных
            row_data = [
                item.get('ads', '') if item else '',  # Столбец AJ
                math.ceil(item['idc']) if item and isinstance(item.get('idc'), (int, float)) else '',  # Столбец AK
                TURNOVER_GRADE_TRANSLATION.get(item.get('turnover_grade', ''), '') if item else ''  # Столбец AL
            ]
            data_to_write.append(row_data)

        # Очистка и запись данных
        print("Очистка диапазона AJ{0}:AL{1}...".format(start_row, start_row + len(data_to_write)))
        sheet.batch_clear([f"AJ{start_row}:AL{start_row + len(data_to_write)}"])
        
        # Запись всех данных разом
        print("Запись данных в таблицу...")
        sheet.update(
            range_name=f"AJ{start_row}:AL{start_row + len(data_to_write) - 1}",
            values=data_to_write
        )

    except Exception as e:
        print(f"Ошибка при работе с Google Таблицами: {str(e)}")
        raise

def main():
    print("=== Ozon Stocks Data Importer ===")
    print("Загрузка данных в таблицу:\n"
          "- Столбец AJ: ads\n"
          "- Столбец AK: idc (с округлением вверх)\n"
          "- Столбец AL: turnover_grade (с переводом значений)\n")

    try:
        # Загрузка учетных данных
        credentials = read_api_credentials()
        
        # Получение данных с Ozon
        start_time = datetime.now()
        items = get_all_ozon_data(credentials['client_id'], credentials['api_key'])
        
        if not items:
            print("Нет данных для обновления!")
            return

        # Обновление таблицы
        update_google_sheets(items, credentials)
        
        duration = (datetime.now() - start_time).total_seconds() / 60
        print(f"\nУспешно завершено за {duration:.1f} минут")
        print(f"Обработано товаров: {len(items)}")

    except Exception as e:
        print(f"\n!!! Критическая ошибка: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()