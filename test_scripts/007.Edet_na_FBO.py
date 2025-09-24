import time
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from typing import Dict, List, Optional

def read_api_config(filename: str = "(1)API.txt") -> tuple:
    """Чтение конфигурационных данных из файла"""
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            lines = [line.strip() for line in file.readlines()]
            if len(lines) < 4:
                raise ValueError("Файл конфигурации должен содержать 4 строки: client id, api-key, sheet id, лист")
            return lines[0], lines[1], lines[2], lines[3]
    except FileNotFoundError:
        raise FileNotFoundError(f"Файл конфигурации {filename} не найден")

# Чтение конфигурации из файла
try:
    CLIENT_ID, API_KEY, SHEET_ID, SHEET_NAME = read_api_config()
    print(f"Настройки успешно прочитаны. Название листа: '{SHEET_NAME}'")
except Exception as e:
    print(f"Ошибка при чтении конфигурации: {e}")
    exit()

CREDENTIALS_FILE = "credentials.json"

# Ozon API
HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json"
}

# Статусы заказов
VALID_STATUSES = [
    "ORDER_STATE_DATA_FILLING",
    "ORDER_STATE_READY_TO_SUPPLY",
    "ORDER_STATE_ACCEPTED_AT_SUPPLY_WAREHOUSE",
    "ORDER_STATE_IN_TRANSIT",
    "ORDER_STATE_ACCEPTANCE_AT_STORAGE_WAREHOUSE",
    "ORDER_STATE_REPORTS_CONFIRMATION_AWAITING"
]

# Соответствие статусов столбцам
STATUS_TO_COLUMN = {
    "ORDER_STATE_DATA_FILLING": "W",
    "ORDER_STATE_READY_TO_SUPPLY": "X",
    "ORDER_STATE_ACCEPTED_AT_SUPPLY_WAREHOUSE": "Y",
    "ORDER_STATE_IN_TRANSIT": "Z",
    "ORDER_STATE_ACCEPTANCE_AT_STORAGE_WAREHOUSE": "AA",
    "ORDER_STATE_REPORTS_CONFIRMATION_AWAITING": "AB"
}

class OzonDataProcessor:
    def __init__(self):
        try:
            self.sheet = self._init_google_sheet()
        except gspread.exceptions.WorksheetNotFound:
            print(f"Ошибка: Лист с названием '{SHEET_NAME}' не найден в таблице. Пожалуйста, проверьте:")
            print(f"1. Что ID таблицы правильный: {SHEET_ID}")
            print(f"2. Что название листа точно совпадает (включая символы и регистр): '{SHEET_NAME}'")
            print("3. Что у сервисного аккаунта есть доступ к этой таблице")
            exit()
        except Exception as e:
            print(f"Ошибка при инициализации Google Sheets: {e}")
            exit()
            
    def _init_google_sheet(self):
        """Инициализация подключения к Google Sheets"""
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        
        # Отладочная информация
        print(f"Попытка открыть таблицу с ID: {SHEET_ID}")
        spreadsheet = client.open_by_key(SHEET_ID)
        print(f"Доступные листы в таблице: {[ws.title for ws in spreadsheet.worksheets()]}")
        print(f"Попытка получить лист: '{SHEET_NAME}'")
        
        return spreadsheet.worksheet(SHEET_NAME)
    
    def get_sku_list(self) -> List[str]:
        """Получение списка SKU из столбца B (начиная с 5 строки)"""
        sku_list = self.sheet.col_values(2)[4:]  # B5:B
        return [str(sku).strip() for sku in sku_list if sku]
    
    def fetch_supply_orders(self) -> List[Dict]:
        """Получение списка заказов из Ozon API"""
        supply_ids = self._fetch_supply_list()
        if not supply_ids:
            print("Нет поставок для обработки")
            return []
        return self._fetch_supply_details(supply_ids)
    
    def _fetch_supply_list(self, from_id: int = 0, limit: int = 100) -> List[str]:
        """Получение ID поставок"""
        url = "https://api-seller.ozon.ru/v2/supply-order/list"
        payload = {
            "filter": {"states": VALID_STATUSES},
            "paging": {"from_supply_order_id": from_id, "limit": limit}
        }
        resp = requests.post(url, headers=HEADERS, json=payload)
        resp.raise_for_status()
        return resp.json().get("supply_order_id", [])
    
    def _fetch_supply_details(self, order_ids: List[str]) -> List[Dict]:
        """Получение деталей поставок"""
        url = "https://api-seller.ozon.ru/v2/supply-order/get"
        payload = {"order_ids": order_ids}
        resp = requests.post(url, headers=HEADERS, json=payload)
        resp.raise_for_status()
        return resp.json().get("orders", [])
    
    def fetch_bundle_items(self, bundle_id: str) -> List[Dict]:
        """Получение товаров в поставке"""
        items = []
        last_id = ""
        has_next = True
        
        while has_next:
            payload = {
                "bundle_ids": [bundle_id],
                "is_asc": True,
                "limit": 100,
                "query": "",
                "sort_field": "UNSPECIFIED"
            }
            if last_id:
                payload["last_id"] = last_id
            
            resp = requests.post(
                "https://api-seller.ozon.ru/v1/supply-order/bundle",
                headers=HEADERS,
                json=payload
            )
            resp.raise_for_status()
            data = resp.json()
            
            items.extend(data.get("items", []))
            has_next = data.get("has_next", False)
            last_id = data.get("last_id", "")
            time.sleep(0.5)
            
        return items
    
    def collect_sku_data(self) -> Dict[str, Dict[str, int]]:
        """Сбор данных по SKU и их статусам"""
        sku_counts = {}
        orders = self.fetch_supply_orders()
        
        for order in orders:
            if order.get("state") == "ORDER_STATE_CANCELLED":
                continue
                
            status_code = order.get("state")
            if status_code not in STATUS_TO_COLUMN:
                continue
                
            bundle_id = order.get("supplies", [{}])[0].get("bundle_id")
            if not bundle_id:
                continue
                
            try:
                items = self.fetch_bundle_items(bundle_id)
                for item in items:
                    sku = str(item.get('sku'))
                    quantity = int(item.get('quantity', 0))
                    
                    if not sku:
                        continue
                        
                    if sku not in sku_counts:
                        sku_counts[sku] = {}
                    
                    if status_code in sku_counts[sku]:
                        sku_counts[sku][status_code] += quantity
                    else:
                        sku_counts[sku][status_code] = quantity
                        
            except Exception as e:
                print(f"Ошибка при обработке bundle {bundle_id}: {e}")
                continue
                
        return sku_counts
    
    def update_sheet(self, sku_counts: Dict[str, Dict[str, int]]):
        """Обновление таблицы данными и расчет сумм"""
        sku_list = self.get_sku_list()
        if not sku_list:
            print("Не найдены SKU в таблице")
            return
            
        # Очищаем диапазоны перед записью
        last_row = len(sku_list) + 4
        self.sheet.batch_clear([
            f"W3:AB3",  # Очищаем строку с суммами
            f"T5:T{last_row}",  # Очищаем столбец с суммами
            f"W5:W{last_row}",
            f"X5:X{last_row}",
            f"Y5:Y{last_row}",
            f"Z5:Z{last_row}",
            f"AA5:AA{last_row}",
            f"AB5:AB{last_row}"
        ])
        
        # Подготавливаем обновления
        updates = []
        column_sums = {col: 0 for col in STATUS_TO_COLUMN.values()}
        
        for row, sku in enumerate(sku_list, start=5):
            counts = sku_counts.get(sku, {})
            row_total = 0  # Сумма для текущей строки
            
            # Записываем данные по статусам
            for status, col in STATUS_TO_COLUMN.items():
                if status in counts:
                    quantity = counts[status]
                    updates.append({
                        'range': f"{col}{row}",
                        'values': [[quantity]]
                    })
                    column_sums[col] += quantity
                    row_total += quantity
            
            # Добавляем сумму в столбец T
            if row_total > 0:
                updates.append({
                    'range': f"T{row}",
                    'values': [[row_total]]
                })
        
        # Добавляем суммы в 3 строку только если они не равны 0
        for col, total in column_sums.items():
            if total != 0:
                updates.append({
                    'range': f"{col}3",
                    'values': [[total]]
                })
        
        # Применяем обновления
        if updates:
            self.sheet.batch_update(updates)
            print(f"Обновлено {len(updates)} ячеек")
            print(f"Суммы в 3 строке: W3={column_sums['W']}, X3={column_sums['X']}, Y3={column_sums['Y']}, Z3={column_sums['Z']}, AA3={column_sums['AA']}, AB3={column_sums['AB']}")
        else:
            print("Нет данных для обновления")
    
    def run(self):
        """Основной метод выполнения"""
        print(f"{datetime.now(ZoneInfo('Europe/Moscow'))}: Начало обработки")
        
        try:
            print("Получаем данные с Ozon...")
            sku_counts = self.collect_sku_data()
            
            if not sku_counts:
                print("Нет данных для обновления")
                return
                
            print(f"Обработано {len(sku_counts)} SKU")
            print("Обновляем таблицу...")
            self.update_sheet(sku_counts)
            
            print(f"{datetime.now(ZoneInfo('Europe/Moscow'))}: Успешно завершено")
            
        except Exception as e:
            print(f"Ошибка: {str(e)}")
            raise

if __name__ == "__main__":
    processor = OzonDataProcessor()
    processor.run()