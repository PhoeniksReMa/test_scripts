import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Функция для чтения данных из файла (1)API.txt с указанием кодировки utf-8
def read_api_credentials(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        client_id = lines[0].strip()      # Первая строка - Client-Id (254177)
        api_key = lines[1].strip()        # Вторая строка - Api-Key (2cab5901-e102-4d12-b78a-4da7324c2128)
        sheet_id = lines[2].strip()      # Третья строка - ID таблицы Google Sheets
        sheet_name = lines[3].strip()     # Четвертая строка - Название листа
    return client_id, api_key, sheet_id, sheet_name

# Читаем данные из файла с указанием кодировки
try:
    client_id, api_key, sheet_id, sheet_name = read_api_credentials("(1)API.txt")
except UnicodeDecodeError:
    # Попробуем другую кодировку, если utf-8 не сработает
    with open("(1)API.txt", 'r', encoding='cp1251') as file:
        lines = file.readlines()
        client_id = lines[0].strip()
        api_key = lines[1].strip()
        sheet_id = lines[2].strip()
        sheet_name = lines[3].strip()

# Убедитесь, что ваш JSON-файл с учетными данными находится в той же папке, что и скрипт
credentials_file = "credentials.json"

# Настройка для доступа к Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
client = gspread.authorize(creds)

# Откройте Google Sheets по ID таблицы и выберите лист
sheet = client.open_by_key(sheet_id).worksheet(sheet_name)

# Получаем product_id из Google Sheets начиная с 5-й строки столбца A
product_ids = [int(pid) for pid in sheet.col_values(1)[4:] if pid.strip().isdigit()]  # Преобразуем в int и проверяем, что это число

# Очищаем только R5:R и S5:S перед загрузкой новых данных (столбец I не трогаем)
if product_ids:  # Проверяем, что есть хотя бы один product_id
    sheet.batch_clear(['R5:R', 'S5:S'])  # Очищаем только R и S

# Устанавливаем URL нового метода API
api_url = "https://api-seller.ozon.ru/v4/product/info/stocks"

# Запрашиваем остатки товаров
headers = {
    "Client-Id": client_id,
    "Api-Key": api_key,
    "Content-Type": "application/json"
}

def get_stock_data(product_ids_chunk):
    body = {
        "filter": {
            "product_id": product_ids_chunk
        },
        "limit": 1000
    }

    response = requests.post(api_url, headers=headers, json=body)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Ошибка при запросе: {response.status_code}")
        print(f"Тело ответа: {response.text}")  # Используем text вместо json(), чтобы избежать ошибок при невалидном JSON
        return None

def update_google_sheet(data, product_ids):
    rows_fbo = []
    rows_fbs = []
    
    stock_dict = {str(item['product_id']): item['stocks'] for item in data['items']}

    for product_id in product_ids:
        stocks = stock_dict.get(str(product_id), [])
        
        fbo_stock = 0
        fbs_stock = 0
        
        for stock in stocks:
            if stock['type'] == 'fbo':
                fbo_stock += stock['present']
            elif stock['type'] == 'fbs':
                fbs_stock += stock['present']
        
        rows_fbo.append([fbo_stock])
        rows_fbs.append([fbs_stock])
    
    start_row = 5
    if rows_fbo:  # Проверяем, что есть данные для записи
        sheet.update(range_name=f'R{start_row}:R{start_row + len(rows_fbo) - 1}', values=rows_fbo)
        sheet.update(range_name=f'S{start_row}:S{start_row + len(rows_fbs) - 1}', values=rows_fbs)

# Разбиваем product_ids на блоки по 1000 элементов
chunks = [product_ids[i:i + 1000] for i in range(0, len(product_ids), 1000)] if product_ids else []

final_data = {'items': []}

for chunk in chunks:
    stock_data = get_stock_data(chunk)
    if stock_data:
        final_data['items'].extend(stock_data['items'])

if final_data['items']:  # Проверяем, что есть данные для обновления
    update_google_sheet(final_data, product_ids)
    print("Данные успешно загружены в Google Sheets (столбцы R и S).")
else:
    print("Нет данных для обновления.")