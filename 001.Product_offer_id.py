import requests
import json
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import time

def read_api_credentials(file_path):
    """Чтение API ключа, client_id, spreadsheet_id и sheet_name из файла"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            lines = [line.strip() for line in lines if line.strip()]
            
            if len(lines) >= 4:
                client_id = lines[0]
                api_key = lines[1]
                spreadsheet_id = lines[2]
                sheet_name = lines[3]
                return api_key, client_id, spreadsheet_id, sheet_name
            else:
                raise ValueError("Файл должен содержать 4 строки: client_id, api_key, spreadsheet_id, sheet_name")
    except Exception as e:
        print(f"Ошибка при чтении API ключей: {e}")
        raise

def get_all_products():
    """Получение списка товаров с Ozon"""
    all_products = []
    last_id = ""
    limit = 1000

    while True:
        data = {
            "filter": {},
            "last_id": last_id,
            "limit": limit
        }

        response = requests.post(
            'https://api-seller.ozon.ru/v3/product/list',
            headers={
                'Client-Id': client_id,
                'Api-Key': api_key,
                'Content-Type': 'application/json'
            },
            data=json.dumps(data)
        )

        if response.status_code == 200:
            result = response.json().get('result', {})
            products = result.get('items', [])
            all_products.extend(products)

            if len(products) < limit:
                break
            else:
                last_id = result.get('last_id', "")
                if not last_id:
                    break
        else:
            print(f"Ошибка: {response.status_code} - {response.text}")
            break

    return all_products

def remove_filter(service, spreadsheet_id, sheet_name):
    """Удаление фильтра с листа"""
    try:
        sheet_metadata = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title))"
        ).execute()
        
        sheet = next(
            (s for s in sheet_metadata['sheets'] if s['properties']['title'] == sheet_name), 
            None
        )
        if not sheet:
            raise ValueError(f"Лист '{sheet_name}' не найден")

        requests = [{
            "clearBasicFilter": {
                "sheetId": sheet['properties']['sheetId']
            }
        }]
        
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': requests}
        ).execute()
        
        print("✅ Фильтр удален со всего листа")
        return response
    except Exception as e:
        print(f"❌ Ошибка при удалении фильтра: {e}")
        return None

def add_full_range_filter(service, spreadsheet_id, sheet_name):
    """Добавляет фильтр на ВСЕ столбцы, начиная с 4 строки до конца данных"""
    try:
        sheet_metadata = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title,gridProperties(columnCount,rowCount)))"
        ).execute()
        
        sheet = next(
            (s for s in sheet_metadata['sheets'] if s['properties']['title'] == sheet_name), 
            None
        )
        if not sheet:
            raise ValueError(f"Лист '{sheet_name}' не найден")

        sheet_id = sheet['properties']['sheetId']
        grid_props = sheet['properties']['gridProperties']
        
        # Берем ВСЕ столбцы таблицы
        end_column_index = grid_props['columnCount']
        
        # Определяем последнюю строку с данными
        values = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A:A",
            majorDimension="COLUMNS"
        ).execute().get('values', [[]])
        
        last_row = len(values[0]) if values and values[0] else 4
        end_row_index = last_row

        # Устанавливаем фильтр на весь диапазон
        requests = [{
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 3,  # 4 строка (0-based)
                        "endRowIndex": end_row_index,
                        "startColumnIndex": 0,  # С первого столбца
                        "endColumnIndex": end_column_index  # До последнего
                    },
                    "sortSpecs": [],
                    "filterSpecs": []
                }
            }
        }]
        
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': requests}
        ).execute()
        
        print(f"✅ Фильтр добавлен на все столбцы, строки 4-{end_row_index}")
        return response
        
    except Exception as e:
        print(f"❌ Ошибка при добавлении фильтра: {e}")
        return None

def clear_google_sheet_range(spreadsheet_id, sheet_name):
    """Очистка указанных диапазонов"""
    try:
        creds = service_account.Credentials.from_service_account_file('credentials.json')
        service = build('sheets', 'v4', credentials=creds)

        clear_ranges = [
            f"{sheet_name}!A5:ZZZ",  # Все данные с 5 строки
            f"{sheet_name}!J4:M4",
            f"{sheet_name}!AQ3:BR4",
            f"{sheet_name}!BT3:CU4",
            f"{sheet_name}!W3:AB3"  # Добавленный диапазон
        ]
        
        batch_clear_request = {"ranges": clear_ranges}
        
        response = service.spreadsheets().values().batchClear(
            spreadsheetId=spreadsheet_id,
            body=batch_clear_request
        ).execute()
        
        print("✅ Указанные диапазоны очищены")
        return response
    except Exception as e:
        print(f"❌ Ошибка при очистке: {e}")
        return None

def adjust_sheet_size(spreadsheet_id, sheet_name, required_rows, service):
    """Настройка размера листа"""
    try:
        sheet_metadata = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title,gridProperties(rowCount)))"
        ).execute()
        
        sheet = next(
            (s for s in sheet_metadata['sheets'] if s['properties']['title'] == sheet_name), 
            None
        )
        if not sheet:
            print(f"Лист '{sheet_name}' не найден.")
            return

        sheet_id = sheet['properties']['sheetId']
        current_rows = sheet['properties']['gridProperties']['rowCount']

        if current_rows < required_rows + 4:
            append_request = {
                "appendDimension": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "length": required_rows + 4 - current_rows
                }
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [append_request]}
            ).execute()
            print(f"Добавлено {required_rows + 4 - current_rows} строк.")

    except Exception as e:
        print(f"Ошибка в adjust_sheet_size: {e}")

def remove_empty_rows_after_data(service, spreadsheet_id, sheet_name, data_row_count):
    """Удаление пустых строк после данных (начиная с строки data_row_count + 5)"""
    try:
        # Получаем ID листа
        sheet_id = get_sheet_id(service, spreadsheet_id, sheet_name)
        if not sheet_id:
            print("❌ Не удалось получить ID листа")
            return

        # Получаем информацию о листе
        sheet_metadata = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title,gridProperties(rowCount)))"
        ).execute()
        
        sheet = next(
            (s for s in sheet_metadata['sheets'] if s['properties']['title'] == sheet_name), 
            None
        )
        if not sheet:
            print(f"Лист '{sheet_name}' не найден.")
            return

        total_rows = sheet['properties']['gridProperties']['rowCount']
        start_delete_row = data_row_count + 5  # Начинаем удалять с этой строки (5 + количество данных)
        
        if start_delete_row < total_rows:
            # Удаляем строки через batchUpdate
            delete_request = {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_delete_row - 1,  # 0-based индекс
                        "endIndex": total_rows
                    }
                }
            }
            
            response = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [delete_request]}
            ).execute()
            
            deleted_rows = total_rows - start_delete_row + 1
            print(f"✅ Удалено {deleted_rows} пустых строк (с {start_delete_row} по {total_rows})")
        else:
            print("✅ Пустых строк для удаления нет")
        
    except Exception as e:
        print(f"❌ Ошибка при удалении пустых строк: {e}")

def get_sheet_id(service, spreadsheet_id, sheet_name):
    """Получение ID листа по имени"""
    try:
        sheet_metadata = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title))"
        ).execute()
        
        sheet = next(
            (s for s in sheet_metadata['sheets'] if s['properties']['title'] == sheet_name), 
            None
        )
        return sheet['properties']['sheetId'] if sheet else None
    except Exception as e:
        print(f"❌ Ошибка при получении ID листа: {e}")
        return None

def clear_excess_rows(service, spreadsheet_id, sheet_name, required_rows):
    """Очистка лишних строк после данных"""
    try:
        # Очищаем строки после данных
        clear_range = f"{sheet_name}!A{required_rows + 5}:ZZZ"
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=clear_range,
            body={}
        ).execute()
        
        print(f"✅ Очищены строки после {required_rows + 4} строки")
        
    except Exception as e:
        print(f"❌ Ошибка при очистке лишних строк: {e}")

def save_products_to_google_sheets(products, spreadsheet_id, sheet_name):
    """Сохранение товаров в Google Sheets"""
    try:
        products = [product for product in products if 'product_id' in product and 'offer_id' in product]
        df = pd.DataFrame(products)
        df = df[['product_id', 'offer_id']]
        df.insert(1, 'Empty_B', '')
        df.insert(2, 'Empty_C', '')
        df['offer_id'] = df.pop('offer_id')

        creds = service_account.Credentials.from_service_account_file('credentials.json')
        service = build('sheets', 'v4', credentials=creds)

        # 1. Удаляем старый фильтр
        remove_filter(service, spreadsheet_id, sheet_name)
        
        # 2. Очищаем указанные диапазоны
        clear_google_sheet_range(spreadsheet_id, sheet_name)

        # 3. Записываем данные
        values = df.values.tolist()
        data_row_count = len(values)
        print(f"Записывается {data_row_count} строк товаров")

        body = {'values': values}
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A5",
            valueInputOption="RAW",
            body=body
        ).execute()

        # 4. Настраиваем размер листа
        adjust_sheet_size(spreadsheet_id, sheet_name, data_row_count, service)
        
        # 5. Очищаем лишние строки после данных
        print("⏳ Очистка лишних строк...")
        clear_excess_rows(service, spreadsheet_id, sheet_name, data_row_count)
        
        # 6. УДАЛЯЕМ ПУСТЫЕ СТРОКИ ПЕРЕД УСТАНОВКОЙ ФИЛЬТРА
        print("⏳ Удаление пустых строк...")
        remove_empty_rows_after_data(service, spreadsheet_id, sheet_name, data_row_count)
        
        # 7. Ждем 3 секунды
        print("⏳ Ожидание 3 секунды перед добавлением фильтра...")
        time.sleep(3)
        
        # 8. Добавляем фильтр на весь диапазон
        add_full_range_filter(service, spreadsheet_id, sheet_name)

    except Exception as e:
        print(f"Ошибка при сохранении данных: {e}")

def write_update_date(spreadsheet_id, sheet_name):
    """Запись даты обновления"""
    try:
        now = datetime.now()
        update_date = now.strftime("%d.%m %H:%M")

        creds = service_account.Credentials.from_service_account_file('credentials.json')
        service = build('sheets', 'v4', credentials=creds)

        body = {'values': [[update_date]]}
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!H1",
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()

        print(f"✅ Дата обновления: {update_date}")

    except Exception as e:
        print(f"❌ Ошибка при записи даты: {e}")

if __name__ == "__main__":
    try:
        print("\n🔹 Начало работы скрипта...")
        api_key, client_id, spreadsheet_id, sheet_name = read_api_credentials("(1)API.txt")
        print(f"🔹 Client ID: {client_id[:3]}...")
        print(f"🔹 API Key: {api_key[:6]}...")
        print(f"🔹 Spreadsheet ID: {spreadsheet_id}")
        print(f"🔹 Sheet Name: {sheet_name}")

        products = get_all_products()
        if products:
            save_products_to_google_sheets(products, spreadsheet_id, sheet_name)
            write_update_date(spreadsheet_id, sheet_name)
            print("✅ Скрипт успешно завершил работу!")
        else:
            print("⚠️ Не удалось получить товары из Ozon")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")