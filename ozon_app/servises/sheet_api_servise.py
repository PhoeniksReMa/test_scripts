from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Optional, Dict, List

import pandas as pd
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError


logger = logging.getLogger(__name__)


class SheetAPIService:
    """
    Утилита для работы с Google Sheets через уже созданный сервис.
    Ожидается, что `service` — это объект `build("sheets", "v4", credentials=creds)`.
    """

    def __init__(self, service: Resource):
        self.service = service

    # ---------- ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ----------

    def _get_sheet_metadata(self, spreadsheet_id: str, fields: str) -> dict:
        try:
            return (
                self.service.spreadsheets()
                .get(spreadsheetId=spreadsheet_id, fields=fields)
                .execute()
            )
        except HttpError as e:
            raise RuntimeError(f"Ошибка получения метаданных книги: {e}") from e

    def _find_sheet(self, spreadsheet_id: str, sheet_name: str) -> Optional[dict]:
        meta = self._get_sheet_metadata(
            spreadsheet_id,
            fields="sheets(properties(sheetId,title,gridProperties(rowCount,columnCount)))",
        )
        return next(
            (s for s in meta.get("sheets", []) if s["properties"]["title"] == sheet_name),
            None,
        )

    def _get_sheet_id(self, spreadsheet_id: str, sheet_name: str) -> int:
        sheet = self._find_sheet(spreadsheet_id, sheet_name)
        if not sheet:
            raise RuntimeError(f"Лист '{sheet_name}' не найден.")
        return sheet["properties"]["sheetId"]

    # ---------- ОПЕРАЦИИ С ФИЛЬТРОМ ----------

    def remove_filter(self, spreadsheet_id: str, sheet_name: str) -> dict:
        """Удалить basic filter с листа."""
        sheet_id = self._get_sheet_id(spreadsheet_id, sheet_name)
        requests = [{"clearBasicFilter": {"sheetId": sheet_id}}]
        try:
            resp = (
                self.service.spreadsheets()
                .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests})
                .execute()
            )
            logger.info("✅ Фильтр удалён с листа '%s'", sheet_name)
            return resp
        except HttpError as e:
            raise RuntimeError(f"Ошибка при удалении фильтра: {e}") from e

    def add_full_range_filter(self, spreadsheet_id: str, sheet_name: str, start_row: int = 4) -> dict:
        """
        Добавить basic filter на ВСЕ столбцы, начиная с `start_row` (1-based) и до последней заполненной строки.
        """
        sheet = self._find_sheet(spreadsheet_id, sheet_name)
        if not sheet:
            raise RuntimeError(f"Лист '{sheet_name}' не найден.")
        sheet_id = sheet["properties"]["sheetId"]
        end_column_index = sheet["properties"]["gridProperties"]["columnCount"]

        # определяем последнюю занятую строку по столбцу A
        try:
            values = (
                self.service.spreadsheets()
                .values()
                .get(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A:A",
                    majorDimension="COLUMNS",
                )
                .execute()
                .get("values", [[]])
            )
        except HttpError as e:
            raise RuntimeError(f"Ошибка чтения данных для фильтра: {e}") from e

        last_row = len(values[0]) if values and values[0] else start_row
        end_row_index = max(last_row, start_row)  # защита от инверсии

        requests = [
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": start_row - 1,  # 0-based
                            "endRowIndex": end_row_index,     # 0-based exclusive
                            "startColumnIndex": 0,
                            "endColumnIndex": end_column_index,
                        },
                        "sortSpecs": [],
                        "filterSpecs": [],
                    }
                }
            }
        ]

        try:
            resp = (
                self.service.spreadsheets()
                .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests})
                .execute()
            )
            logger.info("✅ Фильтр добавлен на '%s' (строки %s-%s)", sheet_name, start_row, end_row_index)
            return resp
        except HttpError as e:
            raise RuntimeError(f"Ошибка при добавлении фильтра: {e}") from e

    # ---------- ОЧИСТКА ДИАПАЗОНОВ / РАЗМЕР ЛИСТА ----------

    def clear_google_sheet_range(self, spreadsheet_id: str, sheet_name: str) -> dict:
        """
        Очистка фиксированных диапазонов:
        - все данные с 5 строки
        - вспомогательные блоки J4:M4, AQ3:BR4, BT3:CU4, W3:AB3
        """
        clear_ranges = [
            f"{sheet_name}!A5:ZZZ",
            f"{sheet_name}!J4:M4",
            f"{sheet_name}!AQ3:BR4",
            f"{sheet_name}!BT3:CU4",
            f"{sheet_name}!W3:AB3",
        ]
        try:
            resp = (
                self.service.spreadsheets()
                .values()
                .batchClear(spreadsheetId=spreadsheet_id, body={"ranges": clear_ranges})
                .execute()
            )
            logger.info("✅ Указанные диапазоны очищены")
            return resp
        except HttpError as e:
            raise RuntimeError(f"Ошибка при очистке диапазонов: {e}") from e

    def adjust_sheet_size(self, spreadsheet_id: str, sheet_name: str, required_rows: int) -> None:
        """
        Увеличивает число строк до required_rows + 4 (запас под шапку),
        если текущих строк меньше.
        """
        sheet = self._find_sheet(spreadsheet_id, sheet_name)
        if not sheet:
            raise RuntimeError(f"Лист '{sheet_name}' не найден.")
        sheet_id = sheet["properties"]["sheetId"]
        current_rows = sheet["properties"]["gridProperties"]["rowCount"]
        target_rows = required_rows + 4

        if current_rows >= target_rows:
            return

        append_len = target_rows - current_rows
        request = {
            "appendDimension": {"sheetId": sheet_id, "dimension": "ROWS", "length": append_len}
        }
        try:
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body={"requests": [request]}
            ).execute()
            logger.info("Добавлено %s строк на лист '%s'.", append_len, sheet_name)
        except HttpError as e:
            raise RuntimeError(f"Ошибка увеличения числа строк: {e}") from e

    def remove_empty_rows_after_data(
        self, spreadsheet_id: str, sheet_name: str, data_row_count: int
    ) -> None:
        """
        Удаляет пустые строки после данных, начиная с (data_row_count + 5)-й строки (1-based).
        """
        sheet = self._find_sheet(spreadsheet_id, sheet_name)
        if not sheet:
            raise RuntimeError(f"Лист '{sheet_name}' не найден.")
        sheet_id = sheet["properties"]["sheetId"]
        total_rows = sheet["properties"]["gridProperties"]["rowCount"]

        start_delete_row = data_row_count + 5  # 1-based
        if start_delete_row >= total_rows:
            logger.info("✅ Пустых строк для удаления нет")
            return

        request = {
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": start_delete_row - 1,  # 0-based inclusive
                    "endIndex": total_rows,              # 0-based exclusive
                }
            }
        }

        try:
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body={"requests": [request]}
            ).execute()
            deleted = total_rows - (start_delete_row - 1)
            logger.info("✅ Удалено %s пустых строк (с %s по %s).", deleted, start_delete_row, total_rows)
        except HttpError as e:
            raise RuntimeError(f"Ошибка удаления пустых строк: {e}") from e

    def clear_excess_rows(
        self, spreadsheet_id: str, sheet_name: str, required_rows: int
    ) -> None:
        """Очищает содержимое строк после требуемых данных (с required_rows + 5)."""
        clear_from = required_rows + 5
        clear_range = f"{sheet_name}!A{clear_from}:ZZZ"
        try:
            self.service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id, range=clear_range, body={}
            ).execute()
            logger.info("✅ Очищены строки после %s-й строки", required_rows + 4)
        except HttpError as e:
            raise RuntimeError(f"Ошибка очистки лишних строк: {e}") from e

    # ---------- ЗАПИСЬ ДАННЫХ / СЕРВИСНЫЕ ДЕЙСТВИЯ ----------

    def save_products_to_google_sheets(
        self,
        products: List[Dict],
        spreadsheet_id: str,
        sheet_name: str,
        delay_before_filter_sec: float = 0.0,
    ) -> None:
        """
        Сохраняет товары (product_id, offer_id) в лист, настраивает размеры,
        чистит диапазоны и ставит фильтр.
        """
        # нормализуем данные → только нужные поля
        rows = [
            {"product_id": p["product_id"], "offer_id": p["offer_id"]}
            for p in products
            if isinstance(p, dict) and "product_id" in p and "offer_id" in p
        ]
        df = pd.DataFrame(rows, columns=["product_id", "offer_id"])
        # вставляем 2 пустых столбца B и C
        df.insert(1, "Empty_B", "")
        df.insert(2, "Empty_C", "")

        values = df.values.tolist()
        data_row_count = len(values)
        logger.info("К записи подготовлено %s строк.", data_row_count)

        # 1) снять фильтр, 2) очистить диапазоны
        self.remove_filter(spreadsheet_id, sheet_name)
        self.clear_google_sheet_range(spreadsheet_id, sheet_name)

        # 3) записать данные с A5
        try:
            self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A5",
                valueInputOption="RAW",
                body={"values": values},
            ).execute()
        except HttpError as e:
            raise RuntimeError(f"Ошибка записи данных: {e}") from e

        # 4) подогнать число строк
        self.adjust_sheet_size(spreadsheet_id, sheet_name, data_row_count)

        # 5) очистить лишнее ниже данных (без удаления строк)
        self.clear_excess_rows(spreadsheet_id, sheet_name, data_row_count)

        # 6) удалить полностью пустые строки после данных (физически)
        self.remove_empty_rows_after_data(spreadsheet_id, sheet_name, data_row_count)

        # 7) необязательная пауза
        if delay_before_filter_sec > 0:
            time.sleep(delay_before_filter_sec)

        # 8) снова поставить фильтр
        self.add_full_range_filter(spreadsheet_id, sheet_name)

    def write_update_date(self, spreadsheet_id: str, sheet_name: str, when: Optional[datetime] = None) -> None:
        """Пишет дату/время обновления в H1 (формат 'дд.мм чч:мм')."""
        when = when or datetime.now()
        update_date = when.strftime("%d.%m %H:%M")

        try:
            self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!H1",
                valueInputOption="USER_ENTERED",
                body={"values": [[update_date]]},
            ).execute()
            logger.info("✅ Дата обновления записана: %s", update_date)
        except HttpError as e:
            raise RuntimeError(f"Ошибка записи даты обновления: {e}") from e
