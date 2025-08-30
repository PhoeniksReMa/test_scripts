import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from typing import List, Dict, Optional
import time
from collections import defaultdict


class OzonAnalyticsStocks:
    # Краткие русские названия для колонки "Статус ликвидности (код)"
    TURNOVER_GRADE_RU = {
        "TURNOVER_GRADE_NONE": "Нет статуса ликвидности",
        "DEFICIT": "Дефицитный",
        "POPULAR": "Очень популярный",
        "ACTUAL": "Популярный",
        "SURPLUS": "Избыточный",
        "NO_SALES": "Без продаж",
        "WAS_NO_SALES": "Был без продаж",
        "RESTRICTED_NO_SALES": "Без продаж, ограничен",
        "COLLECTING_DATA": "Сбор данных",
        "WAITING_FOR_SUPPLY": "Ожидаем поставки",
        "WAS_DEFICIT": "Был дефицитным",
        "WAS_POPULAR": "Был очень популярным",
        "WAS_ACTUAL": "Был популярным",
        "WAS_SURPLUS": "Был избыточным",
        "UNSPECIFIED": "Не определено",
    }

    # Только описание (без названия) для колонки "Статус ликвидности (описание)"
    TURNOVER_GRADE_DESCRIPTIONS = {
        "TURNOVER_GRADE_NONE": "",
        "DEFICIT": "Хватит до 28 дней.",
        "POPULAR": "Хватит на 28–56 дней.",
        "ACTUAL": "Хватит на 56–120 дней.",
        "SURPLUS": "Хватит более чем на 120 дней.",
        "NO_SALES": "Нет продаж последние 28 дней.",
        "WAS_NO_SALES": "Не было продаж и остатков последние 28 дней",
        "RESTRICTED_NO_SALES": "Запрет FBO. Не было продаж более 120 дней",
        "COLLECTING_DATA": "Собираем данные в течение 60 дней после поставки",
        "WAITING_FOR_SUPPLY": "Нет остатков. Сделайте поставку для начала сбора данных",
        "WAS_DEFICIT": "Нет остатков. Товар был дефицитным последние 56 дней",
        "WAS_POPULAR": "Нет остатков. Товар был очень популярным последние 56 дней",
        "WAS_ACTUAL": "Нет остатков. Товар был популярным последние 56 дней",
        "WAS_SURPLUS": "Нет остатков. Товар был избыточным последние 56 дней",
        "UNSPECIFIED": "Нет данных",
    }

    # Диапазон очистки данных — записываем в AD..AI
    SHEET_RANGE_CLEAR = "AD5:AI1000"

    def __init__(self, credentials_path: str = "credentials.json"):
        # Читаем конфиг
        with open("(1)API.txt", "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
        if len(lines) < 4:
            raise ValueError(
                "Файл (1)API.txt должен содержать 4 строки (client_id, api_key, spreadsheet_id, sheet_name)"
            )

        self.client_id = lines[0]
        self.api_key = lines[1]
        self.spreadsheet_id = lines[2]
        self.sheet_name = lines[3]

        self.base_url = "https://api-seller.ozon.ru"
        self.headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

        self.gc = self._init_google_sheets(credentials_path)

    # -------------------------------
    # Infrastructure / utils
    # -------------------------------

    def _init_google_sheets(self, credentials_path: str):
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        return gspread.authorize(creds)

    @staticmethod
    def _as_int_or_none(v) -> Optional[int]:
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_float_or_none(v) -> Optional[float]:
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_sheet_cell(value) -> str:
        # None -> пустая ячейка, иначе строка
        return "" if value is None else str(value)

    @staticmethod
    def _to_sheet_cell_blank_if_zero(value: Optional[int]) -> str:
        # Для "Возвраты поставщику": 0 и None -> пусто
        if value is None or value == 0:
            return ""
        return str(value)

    # -------------------------------
    # Data fetching
    # -------------------------------

    def get_sku_list_from_sheet(self) -> List[str]:
        sh = self.gc.open_by_key(self.spreadsheet_id)
        worksheet = sh.worksheet(self.sheet_name)
        # SKU из колонки B (2), начиная с 5-й строки
        skus = [str(sku).strip() for sku in worksheet.col_values(2)[4:] if str(sku).strip()]
        print(f"Получено {len(skus)} SKU из таблицы")
        return skus

    def get_stocks_data(self, skus: List[str]) -> Dict:
        batch_size = 50
        all_items = []

        for i in range(0, len(skus), batch_size):
            batch = skus[i : i + batch_size]
            payload = {"skus": batch, "warehouse_ids": [], "limit": batch_size}

            try:
                # Не печатаем список SKU пачки
                response = requests.post(
                    f"{self.base_url}/v1/analytics/stocks",
                    headers=self.headers,
                    json=payload,
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()
                all_items.extend(data.get("items", []))
            except requests.exceptions.RequestException as e:
                print(f"Ошибка при запросе: {e}")
                continue

            time.sleep(1)

        return {"items": all_items}

    # -------------------------------
    # Transform
    # -------------------------------

    def turnover_grade_ru(self, grade: Optional[str]) -> Optional[str]:
        return self.TURNOVER_GRADE_RU.get(grade) if grade else None

    def describe_turnover_grade(self, grade: Optional[str]) -> Optional[str]:
        return self.TURNOVER_GRADE_DESCRIPTIONS.get(grade) if grade else None

    def build_export_rows(self, skus: List[str], items: List[dict]) -> List[List[str]]:
        # репрезентативная запись + сумма возвратов по SKU
        rep_item: Dict[str, dict] = {}
        returns_sum: Dict[str, Optional[int]] = defaultdict(lambda: None)

        for it in items:
            sku_str = str(it.get("sku")) if it.get("sku") is not None else None
            if not sku_str:
                continue

            if sku_str not in rep_item:
                rep_item[sku_str] = it

            val = self._as_int_or_none(it.get("return_to_seller_stock_count"))
            if val is not None:
                if returns_sum[sku_str] is None:
                    returns_sum[sku_str] = val
                else:
                    returns_sum[sku_str] += val

        export_rows: List[List[str]] = []

        for sku in skus:
            base = rep_item.get(sku, {})

            days_without_sales = self._as_int_or_none(base.get("days_without_sales"))
            idc = self._as_int_or_none(base.get("idc"))
            ads = self._as_float_or_none(base.get("ads"))
            turnover_code = base.get("turnover_grade")
            grade_ru = self.turnover_grade_ru(turnover_code)
            grade_desc = self.describe_turnover_grade(turnover_code)
            rts_sum = returns_sum.get(sku)

            # AD, AE — 0 показываем как "0"
            ad_days = self._to_sheet_cell(days_without_sales)
            ae_idc = self._to_sheet_cell(idc)

            # AF — Возвраты поставщику: 0 скрываем
            af_returns = self._to_sheet_cell_blank_if_zero(rts_sum)

            # AG — Статус (код) на русском (кратко)
            ag_grade_ru = self._to_sheet_cell(grade_ru)

            # AH — Только описание
            ah_grade_desc = self._to_sheet_cell(grade_desc)

            # AI — ADS: округляем до 2 знаков, но 0.00 → "0"
            if ads is None:
                ai_ads = ""
            else:
                rounded = round(ads, 2)
                ai_ads = "0" if rounded == 0 else f"{rounded:.2f}"

            export_rows.append([ad_days, ae_idc, af_returns, ag_grade_ru, ah_grade_desc, ai_ads])

        return export_rows

    # -------------------------------
    # Export
    # -------------------------------

    def export_data_to_sheet(self, data: List[List[str]]):
        sh = self.gc.open_by_key(self.spreadsheet_id)
        worksheet = sh.worksheet(self.sheet_name)

        # Очищаем AD..AI и заливаем строки — заголовки НЕ трогаем
        worksheet.batch_clear([self.SHEET_RANGE_CLEAR])

        if data:
            bottom_row = 4 + len(data)
            worksheet.update(data, f"AD5:AI{bottom_row}")

        print(f"Экспортировано {len(data)} строк в таблицу")

    # -------------------------------
    # Pipeline
    # -------------------------------

    def process(self) -> bool:
        try:
            print("1. Получение SKU из таблицы...")
            skus = self.get_sku_list_from_sheet()
            if not skus:
                print("Не найдены SKU в таблице")
                return False

            print("2. Запрос данных Ozon...")
            response = self.get_stocks_data(skus)
            items = response.get("items", [])
            if not items:
                print("Нет данных от API Ozon")
                self.export_data_to_sheet([])
                return False

            print("3. Подготовка строк...")
            export_rows = self.build_export_rows(skus, items)

            print("4. Экспорт в таблицу...")
            self.export_data_to_sheet(export_rows)

            print("Готово!")
            return True

        except Exception as e:
            print(f"Ошибка: {str(e)}")
            return False


if __name__ == "__main__":
    analyzer = OzonAnalyticsStocks()
    if not analyzer.process():
        print("Произошли ошибки при выполнении")
