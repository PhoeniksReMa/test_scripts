import requests
import json


class OzonAPIService:
    def __init__(self, api_key: str, client_id: str, timeout: float = 15.0):
        self.api_key = api_key
        self.client_id = client_id
        self.base_url = "https://api-seller.ozon.ru"
        self.timeout = timeout

    def get_all_products(
        self,
        all_products: list | None = None,
        last_id: str = "",
        limit: int = 1000,
        filter: dict | None = None,
    ) -> list:
        """Получение списка товаров с Ozon"""

        if all_products is None:
            all_products = []
        if filter is None:
            filter = {}

        headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

        while True:
            try:
                response = requests.post(
                    f"{self.base_url}/v3/product/list",
                    headers=headers,
                    json={"filter": filter, "last_id": last_id, "limit": limit},
                    timeout=self.timeout,
                )
                response.raise_for_status()
                result = response.json().get("result", {})
            except requests.RequestException as e:
                raise RuntimeError(f"Ошибка запроса к Ozon API: {e}") from e
            except ValueError as e:
                raise RuntimeError(f"Ошибка обработки JSON от Ozon API: {e}") from e

            products = result.get("items", [])
            all_products.extend(products)

            if len(products) < limit or not result.get("last_id"):
                break

            last_id = result["last_id"]

        return all_products

