import os
import gspread
import math
import numpy as np
import re
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from pprint import pprint
from datetime import datetime

def log_step(message, data=None):
    print(f"\n[ШАГ] {message}")
    if data:
        print("[ДАННЫЕ]", end=" ")
        pprint(data)

def log_error(message, error=None):
    print(f"\n[ОШИБКА] {message}")
    if error:
        print("[ДЕТАЛИ ОШИБКИ]", end=" ")
        if isinstance(error, Exception):
            print(f"{type(error).__name__}: {str(error)}")
        else:
            pprint(error)

def read_api_config(file_path='(1)API.txt'):
    """Чтение конфигурации из файла (1)API.txt"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
            
        if len(lines) < 4:
            raise ValueError("Файл API.txt должен содержать как минимум 4 строки с данными")
            
        return {
            'some_id': lines[0],
            'uuid': lines[1],
            'spreadsheet_id': lines[2].split(' - ')[0].strip(),
            'sheet_name': lines[3].split(' - ')[0].strip()
        }
    except Exception as e:
        log_error("Ошибка чтения файла API.txt", e)
        raise

def process_value(value):
    """Обработка значения: округление чисел, проверка на ноль и NaN"""
    if pd.isna(value) or str(value).lower() in ['nan', 'none', '']:
        return None
    
    try:
        num = float(value)
        if num == 0:
            return None
        return math.ceil(num)
    except (ValueError, TypeError):
        return str(value) if value else None

def prepare_data_for_column(articles, data_map):
    """Подготовка данных для одного столбца"""
    values = []
    stats = {'matched': 0, 'zeros_skipped': 0, 'nan_skipped': 0, 'not_found': 0}
    
    for article in articles:
        val = data_map.get(article)
        
        if val is None:
            if article in data_map:
                if isinstance(data_map[article], float) and math.isnan(data_map[article]):
                    values.append([''])
                    stats['nan_skipped'] += 1
                else:
                    values.append([''])
                    stats['zeros_skipped'] += 1
            else:
                values.append([''])
                stats['not_found'] += 1
        else:
            values.append([val])
            stats['matched'] += 1
    
    stats['total'] = len(articles)
    stats['match_percent'] = round(stats['matched']/stats['total']*100, 2)
    return values, stats

def read_excel_columns(xls, config):
    """Чтение данных из Excel файла"""
    articles_col = pd.read_excel(
        xls, sheet_name=0,
        usecols=config['xlsx_columns']['article_column'],
        header=None,
        skiprows=config['xlsx_columns']['start_row']-1
    )
    
    columns = {}
    for col_type in ['price', 'additional1', 'additional2', 'additional3']:
        col_key = f"{'price' if col_type == 'price' else 'additional'}{'' if col_type == 'price' else col_type[-1]}_column"
        columns[col_type] = pd.read_excel(
            xls, sheet_name=0,
            usecols=config['xlsx_columns'][col_key],
            header=None,
            skiprows=config['xlsx_columns']['start_row']-1
        ).replace({np.nan: None})
    
    return articles_col, columns

def update_totals_and_date(worksheet, file_path=None):
    """Обновление итоговых сумм и даты в шапке таблицы"""
    try:
        # Получаем дату из названия файла
        if file_path:
            file_name = os.path.basename(file_path)
            # Ищем дату в формате YYYY-MM-DD в начале названия файла
            date_match = re.search(r'^(\d{4})-(\d{2})-(\d{2})', file_name)
            if date_match:
                year, month, day = date_match.groups()
                current_date = f"XLS от {day}.{month}"
            else:
                # Если дата не найдена в имени файла, используем текущую дату
                current_date = f"Файл от {datetime.now().strftime('%d.%m.%Y')}"
        else:
            current_date = f"Файл от {datetime.now().strftime('%d.%m.%Y')}"
        
        # Получаем данные из столбцов
        j_values = worksheet.get('J5:J')
        k_values = worksheet.get('K5:K')
        l_values = worksheet.get('L5:L')
        
        # Функция для расчета суммы
        def calculate_sum(values):
            total = 0
            for row in values:
                if row and row[0]:
                    try:
                        total += float(row[0])
                    except (ValueError, TypeError):
                        continue
            return total
        
        # Вычисляем суммы
        j_sum = calculate_sum(j_values)
        k_sum = calculate_sum(k_values)
        l_sum = calculate_sum(l_values)
        
        # Обновляем ячейки
        worksheet.update(values=[[j_sum]], range_name='J4')
        worksheet.update(values=[[k_sum]], range_name='K4')
        worksheet.update(values=[[l_sum]], range_name='L4')
        worksheet.update(values=[[current_date]], range_name='J1')
        
        log_step("Итоговые суммы и дата обновлены", {
            'J4 (сумма J5:J)': j_sum,
            'K4 (сумма K5:K)': k_sum,
            'L4 (сумма L5:L)': l_sum,
            'J1 (дата)': current_date
        })
        
    except Exception as e:
        log_error("Ошибка при обновлении итоговых сумм и даты", e)
        raise

def main():
    log_step("Начало выполнения скрипта")
    
    try:
        # Чтение конфигурации
        api_config = read_api_config()
        log_step("Конфигурация из API.txt", api_config)
        
        # Основная конфигурация
        CONFIG = {
            'credentials_file': 'credentials.json',
            'spreadsheet_id': api_config['spreadsheet_id'],
            'sheet_name': api_config['sheet_name'],
            'search_folder': 'Path',
            'file_pattern': 'Стоимость размещения по товарам на складе Ozon',
            'google_sheet_columns': {
                'articles': 'D5:D',
                'target_price': 'J5:J',
                'target_additional1': 'K5:K',
                'target_additional2': 'L5:L',
                'target_additional3': 'M5:M'
            },
            'xlsx_columns': {
                'article_column': 'B',
                'price_column': 'D',
                'additional1_column': 'F',
                'additional2_column': 'M',
                'additional3_column': 'O',
                'start_row': 3
            }
        }

        # Авторизация и открытие таблицы
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            CONFIG['credentials_file'], 
            ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(CONFIG['spreadsheet_id'])
        worksheet = spreadsheet.worksheet(CONFIG['sheet_name'])

        # Получение артикулов
        articles = [str(item).strip() for sublist in worksheet.get(CONFIG['google_sheet_columns']['articles']) 
                  for item in sublist if item]
        log_step(f"Получено {len(articles)} артикулов для обработки")

        # Поиск файла с данными
        files = [f for f in os.listdir(CONFIG['search_folder']) 
               if CONFIG['file_pattern'] in f and f.lower().endswith(('.xlsx', '.xls'))]
        if not files:
            raise FileNotFoundError(f"Файлы с паттерном '{CONFIG['file_pattern']}' не найдены")
        target_file = os.path.join(CONFIG['search_folder'], files[0])
        log_step(f"Найден файл для обработки: {target_file}")

        # Чтение и обработка данных из Excel
        with pd.ExcelFile(target_file, engine='openpyxl') as xls:
            articles_col, columns = read_excel_columns(xls, CONFIG)
            
            data_maps = {}
            for col_type in ['price', 'additional1', 'additional2', 'additional3']:
                data_maps[col_type] = {
                    str(art).strip(): process_value(val)
                    for art, val in zip(articles_col.iloc[:, 0], columns[col_type].iloc[:, 0])
                    if pd.notna(art)
                }

        # Подготовка данных для всех столбцов
        results = {}
        for col_type in ['price', 'additional1', 'additional2', 'additional3']:
            values, stats = prepare_data_for_column(articles, data_maps[col_type])
            results[col_type] = {'values': values, 'stats': stats}

        # Запись данных в Google Sheets
        column_mapping = {
            'price': 'target_price',
            'additional1': 'target_additional1',
            'additional2': 'target_additional2',
            'additional3': 'target_additional3'
        }

        for col_type, target in column_mapping.items():
            start_row = int(CONFIG['google_sheet_columns'][target].split(':')[0][1:])
            end_row = start_row + len(results[col_type]['values']) - 1
            target_range = f"{CONFIG['google_sheet_columns'][target][0]}{start_row}:{CONFIG['google_sheet_columns'][target][-1]}{end_row}"
            
            try:
                worksheet.update(values=results[col_type]['values'], range_name=target_range)
                log_step(f"Данные ({col_type}) записаны в {target_range}", results[col_type]['stats'])
            except Exception as e:
                log_error(f"Ошибка при записи данных в {target_range}", e)
                continue

        # Обновление итоговых сумм и даты
        update_totals_and_date(worksheet, target_file)

        # Итоговая статистика
        log_step("Итоговая статистика", {
            'Обработано артикулов': len(articles),
            'Дата обработки': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Конфигурация': {
                'ID таблицы': CONFIG['spreadsheet_id'],
                'Имя листа': CONFIG['sheet_name']
            },
            'Результаты': {
                'Цены (D→J)': results['price']['stats'],
                'Данные F→L': results['additional1']['stats'],
                'Данные M→K': results['additional2']['stats'],
                'Данные O→M': results['additional3']['stats']
            }
        })

    except Exception as e:
        log_error("Произошла ошибка при выполнении скрипта", e)
        return

    log_step("Скрипт успешно завершил работу")

if __name__ == "__main__":
    main()