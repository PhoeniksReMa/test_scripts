import asyncio
import aiohttp
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def read_api_credentials(file_path='(1)API.txt'):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = [line.strip() for line in file.readlines() if line.strip()]
            
            if len(lines) >= 4:
                return lines[0], lines[1], lines[2], lines[3]  # client_id, api_key, spreadsheet_id, sheet_name
            raise ValueError("Файл должен содержать 4 строки: Client-Id, Api-Key, ID таблицы, название листа")
    except Exception as e:
        logger.error(f"Ошибка чтения файла: {e}")
        exit(1)

client_id, api_key, spreadsheet_id, sheet_name = read_api_credentials()

headers = {
    'Client-Id': client_id,
    'Api-Key': api_key,
    'Content-Type': 'application/json'
}

product_info_url = 'https://api-seller.ozon.ru/v3/product/info/list'

def format_date(iso_date):
    if not iso_date:
        return 'N/A'
    try:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                return datetime.strptime(iso_date, fmt).strftime("%d.%m.%Y")
            except ValueError:
                continue
        return 'N/A'
    except Exception:
        return 'N/A'

def write_update_date_to_h1():
    """Записывает текущую дату и время в ячейку H1 в формате 'дд.мм чч:мм'"""
    try:
        creds = service_account.Credentials.from_service_account_file('credentials.json')
        service = build('sheets', 'v4', credentials=creds)
        
        # Получаем текущую дату и время в нужном формате
        current_date = datetime.now().strftime("%d.%m %H:%M")
        
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': [
                {
                    'range': f"{sheet_name}!H1",
                    'values': [[current_date]]
                }
            ]
        }
        
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        logger.info(f"Дата обновления записана в H1: {current_date}")
        
    except Exception as e:
        logger.error(f"Ошибка записи даты обновления в H1: {e}")

def get_product_ids_from_google_sheets():
    try:
        creds = service_account.Credentials.from_service_account_file('credentials.json')
        service = build('sheets', 'v4', credentials=creds)
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A5:A"
        ).execute()
        return [int(row[0]) for row in result.get('values', []) if row and row[0].strip().isdigit()]
    except Exception as e:
        logger.error(f"Ошибка получения ID: {e}")
        return []

def write_data_to_google_sheets(product_ids, data):
    try:
        creds = service_account.Credentials.from_service_account_file('credentials.json')
        service = build('sheets', 'v4', credentials=creds)
        
        data_dict = {item['id']: item for item in data}
        values_b, values_c = [], []
        values_e, values_f, values_g = [], [], []
        values_i, values_n = [], []

        for product_id in product_ids:
            item = data_dict.get(product_id, {})
            
            # Обработка изображения (как в старом рабочем коде)
            primary_image = item.get('primary_image', [])
            image_url = primary_image[0] if isinstance(primary_image, list) and primary_image else primary_image
            image_formula = f'=IMAGE("{image_url}")' if image_url and isinstance(image_url, str) and image_url.startswith(('http://', 'https://')) else 'N/A'
            
            # Остальные данные
            sources = item.get('sources', [])
            sku = sources[0].get('sku', 'N/A') if sources else 'N/A'
            barcode = item.get('barcodes', ['N/A'])[0]
            is_super = '✔️' if item.get('is_super') is True else '❌' if item.get('is_super') is False else 'N/A'
            
            # Даты
            created_at = format_date(item.get('created_at'))
            updated_at = format_date(item.get('updated_at'))

            values_b.append([sku])
            values_c.append([image_formula])
            values_e.append([item.get('name', 'N/A')])
            values_f.append([barcode])
            values_g.append([is_super])
            values_i.append([created_at])
            values_n.append([updated_at])

        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': [
                {'range': f"{sheet_name}!B5:B{len(values_b)+4}", 'values': values_b},
                {'range': f"{sheet_name}!C5:C{len(values_c)+4}", 'values': values_c},
                {'range': f"{sheet_name}!E5:E{len(values_e)+4}", 'values': values_e},
                {'range': f"{sheet_name}!F5:F{len(values_f)+4}", 'values': values_f},
                {'range': f"{sheet_name}!G5:G{len(values_g)+4}", 'values': values_g},
                {'range': f"{sheet_name}!I5:I{len(values_i)+4}", 'values': values_i},
                {'range': f"{sheet_name}!N5:N{len(values_n)+4}", 'values': values_n}
            ]
        }
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        logger.info("Данные успешно записаны")
        
        # Записываем дату обновления после успешной записи данных
        write_update_date_to_h1()
        
    except Exception as e:
        logger.error(f"Ошибка записи: {e}")

async def fetch_product_info(session, product_ids):
    try:
        async with session.post(product_info_url, headers=headers, json={"product_id": product_ids}) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('items', [])
            logger.error(f"Ошибка API: {response.status}")
            return []
    except Exception as e:
        logger.error(f"Ошибка запроса: {e}")
        return []

async def main():
    product_ids = get_product_ids_from_google_sheets()
    if not product_ids:
        return

    async with aiohttp.ClientSession() as session:
        all_data = []
        for i in range(0, len(product_ids), 1000):
            batch = product_ids[i:i+1000]
            data = await fetch_product_info(session, batch)
            if data:
                all_data.extend(data)
        
        if all_data:
            write_data_to_google_sheets(product_ids, all_data)
        else:
            logger.error("Нет данных для записи")

if __name__ == "__main__":
    asyncio.run(main())