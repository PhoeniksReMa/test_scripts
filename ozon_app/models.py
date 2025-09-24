from django.db import models
from django.conf import settings

MARKETPLACE_OZON = "ozon"
MARKETPLACE_WB = "wb"
MARKETPLACE_YA = "ya"
MARKETPLACE_CHOICES = [
    (MARKETPLACE_OZON, "Ozon"),
    (MARKETPLACE_WB, "Wildberries"),
    (MARKETPLACE_YA, "Yandex"),
]

class Shop(models.Model):
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="shops"
    )
    name = models.CharField(max_length=255)
    marketplace = models.CharField(max_length=10, choices=MARKETPLACE_CHOICES, default=MARKETPLACE_OZON)

    ozon_client_id = models.CharField(max_length=255, blank=True, default="")
    ozon_api_key = models.CharField(max_length=255, blank=True, default="")

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "shop"
        unique_together = ("owner", "name")
        indexes = [models.Index(fields=["owner", "marketplace"])]

    def __str__(self):
        return f"{self.name} ({self.get_marketplace_display()})"


class OzonProduct(models.Model):
    shop = models.ForeignKey(Shop, on_delete=models.CASCADE, related_name="products")

    product_id = models.BigIntegerField()
    offer_id = models.CharField(max_length=255, db_index=True)

    archived = models.BooleanField(default=False)
    has_fbo_stocks = models.BooleanField(default=False)
    has_fbs_stocks = models.BooleanField(default=False)
    is_discounted = models.BooleanField(default=False)

    quants = models.JSONField(default=list, blank=True)
    data = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "ozon_product"
        unique_together = ("shop", "product_id")  # уникален в рамках магазина

    def __str__(self):
        return f"{self.shop_id}:{self.product_id} / {self.offer_id}"