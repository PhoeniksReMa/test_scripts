import requests
import json
import gspread
import math
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

def read_api_credentials(file_path):
    """Read credentials from API file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = [line.strip() for line in file.readlines() if line.strip()]
            if len(lines) >= 4:
                return lines[0], lines[1], lines[2], lines[3]
            else:
                raise ValueError("API file must contain 4 lines")
    except Exception as e:
        raise ValueError(f"Error reading file: {str(e)}")

def authorize_google_sheets(credentials_file):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    return gspread.authorize(creds)

def safe_float(value):
    """Safe conversion to float"""
    try:
        return float(value) if value is not None else 0.0
    except (ValueError, TypeError):
        return 0.0

def read_product_ids_from_sheet(spreadsheet_id, worksheet_name):
    try:
        client = authorize_google_sheets("credentials.json")
        sheet = client.open_by_key(spreadsheet_id)
        worksheet = sheet.worksheet(worksheet_name)
        return [int(pid) for pid in worksheet.col_values(1)[4:] if pid and pid.isdigit()]
    except Exception as e:
        raise Exception(f"Error reading product_ids: {str(e)}")

def get_ozon_prices(client_id, api_key, product_ids, limit=1000):
    url = "https://api-seller.ozon.ru/v5/product/info/prices"
    headers = {
        "Client-Id": str(client_id),
        "Api-Key": str(api_key),
        "Content-Type": "application/json"
    }

    all_prices = {}
    for i in range(0, len(product_ids), limit):
        chunk = product_ids[i:i + limit]
        data = {"filter": {"product_id": chunk}, "limit": limit}

        try:
            response = requests.post(url, headers=headers, json=data, timeout=30)
            response.raise_for_status()
            result = response.json()

            if 'items' in result:
                for item in result['items']:
                    if item is None:
                        print(f"Warning: Received None for product in chunk {chunk}")
                        continue
                    if 'product_id' in item:
                        all_prices[item['product_id']] = item
                    else:
                        print(f"Warning: Item without product_id in chunk {chunk}")
            else:
                print(f"Unexpected API response for chunk {chunk}")
        except Exception as e:
            print(f"Error for products {chunk}: {str(e)}")

    return all_prices

def prepare_data_for_sheet(data, product_ids, dq1_value):
    color_index_mapping = {
        "WITHOUT_INDEX": "НЕТ",
        "GREEN": "ХОРОШИЙ",
        "YELLOW": "СРЕДНИЙ",
        "RED": "ПЛОХОЙ"
    }
    
    EXCLUDED_ACTIONS = [
        "Рассрочка 0-0-6 на всё РФ товары",
        "WOW-БЭК_Кэшбэк на покупку Ozon Fashion списание 2.0",
        "ВАУ баллы 50% 9 волна 3-я волна (основная)",
        "Ozon Fashion + Jardin 500 вау баллов  списание",
        "Ozon Fashion + Jardin 1000 списание",
        "[Ozon Fashion + Jardin 10 000 списание",
        "Ozon Fashion + Jardin списание 1 млн",
        "Ozon Fashion + Jardin списание 1 млн вторая",
        "Ozon Fashion + Jardin_Запасная акция 500 вау баллов списание",
        "Ozon Fashion + Jardin_Запасная акция 1000 вау баллов списание",
        "Ozon Fashion + Jardin_Запасная акция 10 000 вау баллов списание",
        "Ozon Fashion + Jardin 10 000 списание"
    ]

    rows = []
    for product_id in product_ids:
        if product_id in data and data[product_id] is not None:
            item = data[product_id]
            
            # Initialize all nested dictionaries safely
            price_data = item.get('price', {}) if item.get('price') is not None else {}
            commissions = item.get('commissions', {}) if item.get('commissions') is not None else {}
            price_indexes = item.get('price_indexes', {}) if item.get('price_indexes') is not None else {}
            marketing_actions = item.get('marketing_actions', {}) if item.get('marketing_actions') is not None else {}
            marketing_actions = marketing_actions.get('actions', []) if marketing_actions.get('actions') is not None else []

            # Main data with safe access
            auto_action = "🔥" if price_data.get('auto_action_enabled', False) else "🔕"
            old_price = safe_float(price_data.get('old_price'))
            min_price = safe_float(price_data.get('min_price'))
            price = safe_float(price_data.get('price'))
            marketing_seller_price = safe_float(price_data.get('marketing_seller_price'))
            marketing_price = safe_float(price_data.get('marketing_price'))
            net_price = safe_float(price_data.get('net_price'))  # Получаем net_price из API
            
            color_index = color_index_mapping.get(
                price_indexes.get('color_index', 'WITHOUT_INDEX'),
                'WITHOUT_INDEX'
            )

            # Commissions and calculations with safe access
            acquiring = math.ceil(safe_float(item.get('acquiring')))
            sales_percent_fbo = safe_float(commissions.get('sales_percent_fbo'))
            sales_percent_fbs = safe_float(commissions.get('sales_percent_fbs'))

            fbo_transport = math.ceil(safe_float(commissions.get('fbo_direct_flow_trans_max_amount')))
            fbs_transport = math.ceil(safe_float(commissions.get('fbs_direct_flow_trans_max_amount')))
            fbo_delivery = math.ceil(safe_float(commissions.get('fbo_deliv_to_customer_amount')))
            fbs_delivery = math.ceil(safe_float(commissions.get('fbs_deliv_to_customer_amount')))

            # Calculations
            dr_value = math.ceil((marketing_seller_price * sales_percent_fbo) / 100) if marketing_seller_price and sales_percent_fbo else 0
            ds_value = math.ceil((marketing_seller_price * sales_percent_fbs) / 100) if marketing_seller_price and sales_percent_fbs else 0
            dt_value = math.ceil(acquiring + dr_value + fbo_transport + fbo_delivery)
            du_value = math.ceil(acquiring + fbs_transport + fbs_delivery + ds_value + dq1_value)

            # Process marketing actions
            action_titles = []
            actions_count = 0
            for action in marketing_actions:
                if action and isinstance(action, dict):
                    title = action.get('title', '').strip()
                    if title and title not in EXCLUDED_ACTIONS:
                        action_titles.append(f"[{title}]")
                        actions_count += 1
            action_title = " ".join(action_titles) if action_titles else ""

            row = [
                acquiring, sales_percent_fbo, dr_value, fbo_transport, fbo_delivery,
                safe_float(commissions.get('fbo_return_flow_amount')),
                sales_percent_fbs, ds_value, fbs_transport, fbs_delivery,
                safe_float(commissions.get('fbs_return_flow_amount')),
                dt_value, du_value,
                "", auto_action, old_price, min_price, price,
                marketing_seller_price, marketing_price, color_index,
                action_title,
                actions_count,
                net_price  # Добавляем net_price из API
            ]
        else:
            row = [''] * 27  # Увеличиваем на 1 для нового столбца net_price

        rows.append(row)

    return rows

def write_data_to_sheet(spreadsheet_id, worksheet_name, data, product_ids):
    try:
        client = authorize_google_sheets("credentials.json")
        sheet = client.open_by_key(spreadsheet_id)
        worksheet = sheet.worksheet(worksheet_name)

        # Get value from ET2 safely
        dq1_cell = worksheet.acell('ET2')
        dq1_value = safe_float(dq1_cell.value) if dq1_cell else 0

        # Clear the range ET5:FQ (расширяем до FQ для нового столбца net_price)
        worksheet.batch_clear([f'ET5:FQ{4 + len(product_ids)}'])

        # Prepare data
        rows = prepare_data_for_sheet(data, product_ids, dq1_value)

        # Write all data at once
        if rows:
            worksheet.update(
                range_name=f'ET5:FQ{4 + len(rows)}',  # Обновляем до FQ
                values=rows,
                value_input_option='USER_ENTERED'
            )

        print(f"Successfully updated {len(rows)} rows")
    except Exception as e:
        raise Exception(f"Write error: {str(e)}")

def main():
    try:
        print("Starting script...")
        client_id, api_key, spreadsheet_id, worksheet_name = read_api_credentials("(1)API.txt")

        product_ids = read_product_ids_from_sheet(spreadsheet_id, worksheet_name)
        if not product_ids:
            raise Exception("No product_ids to process")

        data = get_ozon_prices(client_id, api_key, product_ids)
        if not data:
            raise Exception("No data received from API")

        write_data_to_sheet(spreadsheet_id, worksheet_name, data, product_ids)
        print("Script completed successfully")
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()