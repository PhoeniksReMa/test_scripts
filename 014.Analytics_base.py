# -*- coding: utf-8 -*-
import requests
from datetime import datetime, timezone, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def read_api_config(filename: str) -> dict:
    """Read API configuration from file."""
    with open(filename, 'r', encoding='utf-8') as file:
        lines = [line.strip() for line in file.readlines()]
    if len(lines) < 4:
        raise ValueError("File (1)API.txt must contain 4 lines: client_id, api-key, Google spreadsheet ID, sheet name")
    return {
        'client_id': lines[0],
        'api_key': lines[1],
        'spreadsheet_id': lines[2],
        'sheet_name': lines[3],
    }


def fetch_product_queries_data(url: str, headers: dict, payload: dict) -> list:
    """Get product queries data (one batch up to 1000 SKU)."""
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code != 200:
            print(f"Response status: {resp.status_code} - {resp.text[:200]}")
            return []
        data = resp.json()
        return data.get('items', []) or []
    except Exception as e:
        print(f"Request error: {e}")
        return []


def chunks(lst, n):
    """Split list into chunks of n elements."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def msk_utc_bounds_for_date(target_date):
    """Return UTC bounds for date in Moscow time."""
    msk_offset = timedelta(hours=3)
    date_from = (datetime.combine(target_date, datetime.min.time()) - msk_offset).isoformat() + "Z"
    date_to = (datetime.combine(target_date, datetime.max.time()) - msk_offset - timedelta(seconds=1)).isoformat() + "Z"
    return date_from, date_to


def main():
    # Read configuration
    try:
        config = read_api_config('(1)API.txt')
        print("Configuration loaded successfully")
    except Exception as e:
        print(f"Error reading config file: {e}")
        return

    # Google Sheets
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(config['spreadsheet_id']).worksheet(config['sheet_name'])
        print("Successfully connected to Google Sheets")
    except Exception as e:
        print(f"Error accessing Google Sheets: {e}")
        return

    # SKU
    try:
        sku_col = sheet.col_values(2)[4:]  # B5:B
        skus = [sku.strip() for sku in sku_col if sku.strip()]
        sku_to_row = {sku: idx + 5 for idx, sku in enumerate(sku_col) if sku.strip()}
        print(f"Got {len(sku_to_row)} SKU from Google Sheet")
        if not skus:
            print("SKU list is empty.")
            return
    except Exception as e:
        print(f"Error getting SKU: {e}")
        return

    # API Ozon
    url = "https://api-seller.ozon.ru/v1/analytics/product-queries"
    headers = {
        "Client-Id": config['client_id'],
        "Api-Key": config['api_key'],
        "Content-Type": "application/json",
    }

    target_date = datetime.now(timezone.utc).date() - timedelta(days=3)
    date_from, date_to = msk_utc_bounds_for_date(target_date)

    date_str = target_date.strftime('%d.%m.%Y')
    print(f"Data for {date_str}")

    # === Header in merged cell FX3:GB3 ===
    try:
        # merge cells; if already merged - continue silently
        try:
            sheet.merge_cells('FX3:GB3')
        except Exception:
            pass
        # correct write without DeprecationWarning
        sheet.update(values=[[f"Данные от {date_str}"]], range_name='FX3')
        print("Date written to FX3:GB3")
    except Exception as e:
        print(f"Error writing date to FX3:GB3: {e}")

    # Collect data in batches
    all_items = []
    for i, batch in enumerate(chunks(skus, 1000), start=1):
        payload = {
            "date_from": date_from,
            "date_to": date_to,
            "skus": batch,
            "page_size": 1000,
            "sort_by": "BY_SEARCHES",
            "sort_dir": "DESCENDING",
        }
        items = fetch_product_queries_data(url, headers, payload)
        print(f"Batch {i}: {len(items)} records")
        all_items.extend(items)

    print(f"Total received {len(all_items)} records")

    # Dictionary SKU -> data
    sku_data = {str(it.get('sku', '')).strip(): it for it in all_items if it.get('sku')}

    updates = []
    missing_skus = []

    # >>> Write metrics to FX:GB
    # FX: Unique searches, FY: Position, FZ: Unique views, GA: Conversion, GB: GMV
    for sku, row in sku_to_row.items():
        if sku in sku_data:
            it = sku_data[sku]
            values = [
                it.get('unique_search_users', ''),  # FX
                it.get('position', ''),             # FY
                it.get('unique_view_users', ''),    # FZ
                it.get('view_conversion', ''),      # GA
                it.get('gmv', ''),                  # GB
            ]
        else:
            missing_skus.append(sku)
            values = [""] * 5
        updates.append({'range': f"FX{row}:GB{row}", 'values': [values]})

    if missing_skus:
        print(f"Data not found for {len(missing_skus)} SKU")

    # Update data
    if updates:
        try:
            sheet.batch_update(updates)
            print(f"Updated {len(updates)} rows of data (columns FX:GB)")
        except Exception as e:
            print(f"Error updating data: {e}")


if __name__ == "__main__":
    main()