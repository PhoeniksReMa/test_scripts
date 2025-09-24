# -*- coding: utf-8 -*-
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, timezone
from pytz import timezone as tz
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

def read_api_keys(filename='(1)API.txt'):
    with open(filename, 'r', encoding='utf-8') as file:
        lines = [line.strip() for line in file if line.strip()]
        if len(lines) >= 4:
            return lines[0], lines[1], lines[2], lines[3]
        raise ValueError("Файл должен содержать минимум 4 строки")

def setup_google_sheets(sheet_id, target_sheet_name):
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

    # Очищаем только с 5-й строки, чтобы не затронуть EM4, EO4, EQ4
    worksheet.batch_clear(["EM5:EM", "EO5:EO", "EQ5:EQ", "ER5:ER"])
    return worksheet

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
            postings = data.get('result', {}).get('postings', [])
            if not postings:
                break
            all_orders.extend(postings)
            offset += limit
            if not data['result'].get('has_next', False):
                break
        except Exception as e:
            logger.error(f"Ошибка API Ozon: {e}")
            break

    return all_orders

def get_last_year_month_delivered(orders, year, month):
    moscow_tz = tz('Europe/Moscow')
    delivered_map = defaultdict(int)

    start_date = datetime(year, month, 1, tzinfo=moscow_tz)
    if month == 12:
        end_date = datetime(year + 1, 1, 1, tzinfo=moscow_tz)
    else:
        end_date = datetime(year, month + 1, 1, tzinfo=moscow_tz)

    for order in orders:
        if order.get("status") != "delivered":
            continue
        try:
            utc_time = datetime.fromisoformat(order["in_process_at"].replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
            moscow_time = utc_time.astimezone(moscow_tz)
            if start_date <= moscow_time < end_date:
                for product in order.get("products", []):
                    offer_id = product.get("offer_id")
                    quantity = int(product.get("quantity", 0))
                    if offer_id:
                        delivered_map[offer_id] += quantity
        except Exception as e:
            logger.warning(f"Ошибка обработки заказа: {e}")
            continue

    period = f"{start_date.strftime('%d.%m.%y')} {end_date.strftime('%d.%m.%y')}"
    return delivered_map, period

def main():
    try:
        client_id, api_key, sheet_id, target_sheet_name = read_api_keys()
        worksheet = setup_google_sheets(sheet_id, target_sheet_name)

        now = datetime.now(tz('Europe/Moscow'))
        last_year = now.year - 1
        months_needed = [
            ((now.month) % 12) + 1,        # следующий месяц
            ((now.month + 1) % 12) + 1,    # через 2 месяца
            ((now.month + 2) % 12) + 1     # через 3 месяца
        ]

        start_range = datetime(last_year, min(months_needed), 1, tzinfo=tz('Europe/Moscow'))
        end_range = datetime(last_year + (1 if max(months_needed) == 12 else 0), (max(months_needed) % 12) + 1, 1, tzinfo=tz('Europe/Moscow'))
        since_date = start_range.astimezone(timezone.utc).isoformat()
        to_date = end_range.astimezone(timezone.utc).isoformat()

        orders = get_ozon_orders(client_id, api_key, since_date, to_date)
        if not orders:
            logger.warning("Нет заказов от API")
            return

        offer_column = worksheet.col_values(4)[4:]  # Столбец D, начиная с 5 строки
        months_cols = [('EM', months_needed[0]), ('EO', months_needed[1]), ('EQ', months_needed[2])]

        for col_letter, month in months_cols:
            delivered_data, period = get_last_year_month_delivered(orders, last_year, month)
            worksheet.update(range_name=f"{col_letter}3", values=[[period]])
            values = [[delivered_data.get(offer_id, 0)] for offer_id in offer_column]
            worksheet.update(range_name=f"{col_letter}5:{col_letter}", values=values)
            logger.info(f"Обновлён столбец {col_letter} за период {period}")

        # Сумма EL + EM + EN + EO + EP + EQ -> ER
        el = worksheet.col_values(142)[4:]  # EL
        em = worksheet.col_values(143)[4:]  # EM
        en = worksheet.col_values(144)[4:]  # EN
        eo = worksheet.col_values(145)[4:]  # EO
        ep = worksheet.col_values(146)[4:]  # EP
        eq = worksheet.col_values(147)[4:]  # EQ

        sums = []
        for values in zip(el, em, en, eo, ep, eq):
            try:
                total = sum(int(v) if v.strip().isdigit() else 0 for v in values)
                sums.append([total])
            except Exception:
                sums.append([0])

        worksheet.update(range_name="ER5:ER", values=sums)
        logger.info("Обновлён столбец ER (сумма EL:EQ)")

    except Exception as e:
        logger.error(f"Ошибка выполнения: {e}", exc_info=True)
    finally:
        logger.info("Скрипт завершён")

if __name__ == "__main__":
    main()