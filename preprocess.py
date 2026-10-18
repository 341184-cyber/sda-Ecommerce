import pandas as pd
from pathlib import Path


RAW = Path("data/raw")
OUTPUT = Path("data/processed")

OUTPUT.mkdir(parents=True, exist_ok=True)


print("Loading datasets...")

orders = pd.read_csv(
    RAW / "olist_orders_dataset.csv"
)

items = pd.read_csv(
    RAW / "olist_order_items_dataset.csv"
)

products = pd.read_csv(
    RAW / "olist_products_dataset.csv"
)

customers = pd.read_csv(
    RAW / "olist_customers_dataset.csv"
)


print("Joining datasets...")

# Calculate order-level item values
item_summary = (
    items
    .groupby("order_id")
    .agg(
        total_price=("price", "sum"),
        total_freight=("freight_value", "sum"),
        item_count=("order_item_id", "count")
    )
    .reset_index()
)


# Keep useful customer fields
customer_data = customers[
    [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    ]
]


# Keep useful product information
product_data = products[
    [
        "product_id",
        "product_category_name"
    ]
]


# Get one product/category per order.
# For this project we use the first product category appearing
# in the order.
order_products = (
    items[
        ["order_id", "product_id"]
    ]
    .drop_duplicates("order_id")
)


order_products = order_products.merge(
    product_data,
    on="product_id",
    how="left"
)


# Join everything
df = orders.merge(
    item_summary,
    on="order_id",
    how="left"
)

df = df.merge(
    customer_data,
    on="customer_id",
    how="left"
)

df = df.merge(
    order_products[
        ["order_id", "product_category_name"]
    ],
    on="order_id",
    how="left"
)


# Parse timestamp
df["order_purchase_timestamp"] = pd.to_datetime(
    df["order_purchase_timestamp"]
)


# Remove orders without usable values
df = df.dropna(
    subset=[
        "order_id",
        "order_purchase_timestamp"
    ]
)


# Fill missing numerical values
df["total_price"] = df["total_price"].fillna(0)
df["total_freight"] = df["total_freight"].fillna(0)
df["item_count"] = df["item_count"].fillna(0)


# Rename category
df["product_category_name"] = (
    df["product_category_name"]
    .fillna("Unknown")
)


# Sort chronologically
df = df.sort_values(
    "order_purchase_timestamp"
)


# Save
output_file = OUTPUT / "orders_stream.csv"

df.to_csv(
    output_file,
    index=False
)


print()
print("Preprocessing complete.")
print(f"Rows: {len(df):,}")
print(f"Output: {output_file}")
print()
print(df.head())