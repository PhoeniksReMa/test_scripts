# -*- coding: utf-8 -*-

import requests
import json
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

# ===== утилиты =====
def col_index_to_letter(col_idx: int) -> str:
    result = ""
    while col_idx > 0:
        col_idx, rem = divmod(col_idx - 1, 26)
        result = chr(65 + rem) + result
    return result

# ===== конфиг =====
def read_api_config(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        lines = [line.strip() for line in file.readlines()]
        if len(lines) < 4:
            raise ValueError("Файл API.txt должен содержать 4 строки: client_id, api-key, spreadsheet_id, sheet_name")
        return {
            'client_id': lines[0],
            'api_key': lines[1],
            'spreadsheet_id': lines[2],
            'sheet_name': lines[3]
        }

# ===== запрос данных с пагинацией =====
def fetch_data_with_pagination(url, headers, payload):
    all_data = []
    offset = 0
    limit = 1000
    while True:
        payload['offset'] = offset
        payload['limit'] = limit
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"Ошибка при запросе: {e}")
            break

        results = data.get('result', {}).get('data', [])
        all_data.extend(results)
        if len(results) < limit:
            break
        offset += limit
        time.sleep(1)
    return all_data

def main():
    # --- Конфигурация ---
    try:
        config = read_api_config('(1)API.txt')
        print("Конфигурация успешно загружена")
    except Exception as e:
        print(f"Ошибка чтения файла конфигурации: {e}")
        return

    # --- Доступ к Google Sheets ---
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(config['spreadsheet_id']).worksheet(config['sheet_name'])
        print("Успешное подключение к Google Таблицам")
    except Exception as e:
        print(f"Ошибка доступа к Google Таблицам: {e}")
        return

    # --- SKU из колонки B5:B ---
    try:
        sku_list = sheet.col_values(2)[4:]  # B5:B
        sku_dict = {sku: idx+5 for idx, sku in enumerate(sku_list) if sku}
        print(f"Получено {len(sku_dict)} SKU")
    except Exception as e:
        print(f"Ошибка при получении SKU: {e}")
        return

    # --- Ozon API ---
    url = "https://api-seller.ozon.ru/v1/analytics/data"
    headers = {
        "Client-Id": config['client_id'],
        "Api-Key": config['api_key'],
        "Content-Type": "application/json",
        "Host": "api-seller.ozon.ru"
    }

    today = datetime.now().date().isoformat()
    metrics_unique = [
        "hits_view_search","hits_view_pdp","hits_tocart_pdp","ordered_units","revenue",
        "position_category","session_view_search","session_view_pdp","conv_tocart_pdp",
        "hits_view","session_view","returns","hits_tocart_search","conv_tocart_search"
    ]

    payload = {
        "date_from": today,
        "date_to": today,
        "dimension": ["sku", "day"],
        "metrics": metrics_unique,
        "limit": 1000
    }

    api_data = fetch_data_with_pagination(url, headers, payload)
    print(f"Итого получено {len(api_data)} записей")

    sku_metrics = {}
    for item in api_data:
        try:
            sku = item['dimensions'][0]['id']
            vals = item['metrics']
            vals = (vals + [None]*len(metrics_unique))[:len(metrics_unique)]
            sku_metrics[sku] = vals
        except (KeyError, IndexError):
            continue

    # --- Жёсткая маппинг-таблица столбец → метрика ---
    column_metric_map = {
        "O": "ordered_units",
        "P": "revenue",
        "Q": "position_category",
        "GH": "hits_view_search",
        "GI": "hits_view_pdp",
        "GJ": "hits_tocart_pdp",
        "GK": "ordered_units",
        "GL": "revenue",
        "GM": "position_category",
        "GN": "hits_view_search",
        "GO": "ordered_units",
        "GP": "hits_view_search",
        "GQ": "session_view_search",
        "GR": "session_view_pdp",
        "GS": "conv_tocart_pdp",
        "GT": "ordered_units",
        "GU": "session_view_pdp",
        "GV": "conv_tocart_pdp",
        "GW": "ordered_units",
        "GX": "revenue",
        "GY": "hits_view",
        "GZ": "session_view",
        "HA": "returns",
        "HB": "hits_tocart_search",
        "HC": "conv_tocart_search"
    }

    metric_idx_map = {m:i for i,m in enumerate(metrics_unique)}

    updates = []
    missing_skus = []

    for sku, row in sku_dict.items():
        if sku in sku_metrics:
            base = sku_metrics[sku]
            row_values = []
            for col, metric in column_metric_map.items():
                idx = metric_idx_map.get(metric)
                val = base[idx] if idx is not None else ""
                if metric == "position_category" and val is not None:
                    val = round(val)
                row_values.append(val)
            # batch_update требует: range + values как список списков
            updates.append({
                "range": f"{column_metric_map.keys().__iter__().__next__()}{row}",  # placeholder, потом заменим по каждой колонке
                "values": [row_values]
            })
        else:
            missing_skus.append(sku)
            row_values = [""]*len(column_metric_map)
            updates.append({
                "range": f"{column_metric_map.keys().__iter__().__next__()}{row}",
                "values": [row_values]
            })

    # --- Для правильного распределения: нужно делать отдельный update для каждого столбца ---
    batch_updates = []
    for update in updates:
        row = int(update['range'][1:])
        for i, col in enumerate(column_metric_map.keys()):
            batch_updates.append({
                "range": f"{col}{row}",
                "values": [[update['values'][0][i]]]
            })

    if batch_updates:
        try:
            sheet.batch_update(batch_updates)
            print(f"Успешно обновлено {len(batch_updates)} ячеек")
        except Exception as e:
            print(f"Ошибка при обновлении данных: {e}")

    if missing_skus:
        print("\nДанные не найдены для SKU:", ", ".join(missing_skus))

if __name__ == "__main__":
    main()
