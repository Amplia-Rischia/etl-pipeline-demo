# /config/validation_schemas.py
"""
Schema definitions for data validation
"""

# Expected schemas for each data source
CUSTOMER_SCHEMA = [
    "customer_id",
    "first_name", 
    "last_name",
    "email",
    "phone",
    "address", 
    "city",
    "country",
    "registration_date",
    "age"
]

PRODUCT_SCHEMA = [
    "product_id",
    "name",
    "category",
    "price"
]

CAMPAIGN_SCHEMA = [
    "campaign_id",
    "name",
    "start_date",
    "end_date",
    "budget",
    "channel",
    "status",
    "target_audience",
    "conversion_rate",
    "created_date",
    "updated_date"
]

TRANSACTION_SCHEMA = [
    "transaction_id",
    "customer_id",
    "product_id",
    "campaign_id",
    "amount",
    "quantity",
    "unit_price",
    "discount_amount",
    "tax_amount",
    "transaction_date",
    "status",
    "payment_method",
    "shipping_address",
    "order_notes",
    "created_date",
    "updated_date"
]