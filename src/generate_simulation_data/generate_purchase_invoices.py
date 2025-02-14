from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
import time
import json
import random
from datetime import datetime
import os
import shutil
import math
from decimal import Decimal
from utils.sent_json_to_kafka import do_sent_json_to_kafka

CUSTOMERS = [
    {"customer_id": "C001", "customer_full_name": "John Smith"},
    {"customer_id": "C002", "customer_full_name": "Jane Doe"},
    {"customer_id": "C003", "customer_full_name": "Alex Johnson"},
    {"customer_id": "C004", "customer_full_name": "Emily Davis"},
    {"customer_id": "C005", "customer_full_name": "Michael Brown"},
    {"customer_id": "C006", "customer_full_name": "Sophia Martinez"},
    {"customer_id": "C007", "customer_full_name": "Chris Garcia"},
    {"customer_id": "C008", "customer_full_name": "Olivia Wilson"}
]

query_select_current_inventory = """
    (
        SELECT i.product_id, i.product_name, i.quantity, i.sale_prices, i.reorder_level, i.last_updated
        FROM inventory i
        JOIN (
            SELECT product_id, MAX(last_updated) AS max_last_updated
            FROM inventory
            GROUP BY product_id
        ) latest
        ON i.product_id = latest.product_id AND i.last_updated = latest.max_last_updated
    ) AS latest_inventory
"""

INPUT_PATH = "./input/kafka_purchase"
BACKUP_PATH = "./input/backup/purchase"

jdbc_url = "jdbc:postgresql://localhost:5432/retail_repository"
db_properties = {
    "user": "jeremie",
    "password": "20112001",
    "driver": "org.postgresql.Driver"
}

def load_inventory():
    inventory_df = spark.read.jdbc(
        url=jdbc_url,
        table=query_select_current_inventory,
        properties=db_properties
    )
    return inventory_df

def select_random_customer():
    return random.choice(CUSTOMERS)

def generate_invoice(inventory_df):
    # Chọn khách hàng ngẫu nhiên
    customer = select_random_customer()
    customer_id = customer["customer_id"]
    customer_name = customer["customer_full_name"]

    # Chọn ngẫu nhiên số lượng sản phẩm mua
    num_items = random.randint(1, 3)
    available_products = inventory_df.collect()
    purchased_items = random.sample(available_products, min(num_items, len(available_products)))

    # Kiểm tra tồn kho và thêm vào hóa đơn
    valid_items = []
    for item in purchased_items:
        max_quantity = item["quantity"]
        if max_quantity > 0:
            purchase_quantity = min(random.randint(1, 6), max_quantity)  # Số lượng mua nhỏ hơn số lượng tồn kho
            total_price = purchase_quantity * item["sale_prices"]
            valid_items.append({
                "product_id": item["product_id"],
                "product_name": item["product_name"],
                "quantity": purchase_quantity,
                "price": item["sale_prices"],
                "total_price": total_price
            })

    # Tính tổng hóa đơn
    total_invoice_amount = sum(item["total_price"] for item in valid_items)

    # Cập nhật inventory
    # for item in valid_items:
    #     inventory_df = inventory_df.withColumn(
    #         "quantity",
    #         when(col("product_id") == item["product_id"], col("quantity") - item["quantity"]).otherwise(col("quantity"))
    #     )

    # Ghi hóa đơn
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    invoice = {
        "customer_id": customer_id,
        "customer_name": customer_name,
        "purchased_items": valid_items,
        "total_invoice_amount": total_invoice_amount,
        "timestamp": timestamp
    }
    return invoice

def get_current_id():
    json_files = [int(f.split('.')[0]) for f in os.listdir(INPUT_PATH) if f.endswith('.json')]
    return max(json_files)

def copy_file(source, des):
    shutil.copy(source, des)
    print(f"File copied from {source} to {des}")

def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)

def write_invoice_to_file(invoice, file_path):
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(invoice, file, ensure_ascii=False, default=lambda x: float(x) if isinstance(x, Decimal) else x)

        
# Main program
def main():
    try:
        purchase_id = get_current_id() + 1
    except Exception as e:
        purchase_id = 0
    print('Current id purchase file: ', purchase_id)
    
    while True:
        # Number of items per invoice (random between 3 and 7)
        num_items = random.randint(3, 5)
        
        inventory_df = load_inventory()
        invoice = generate_invoice(inventory_df)
        
        do_sent_json_to_kafka(invoice)
        
        # Print the invoice to the console (optional)
        # pretty_json = json.dumps(str(invoice), indent=4, ensure_ascii=False)

        print(f"Generated Invoice: {invoice}")
        
        # Write the invoice to the JSON file
        write_invoice_to_file(invoice, f"././input/kafka_purchase/{purchase_id}.json")
        
        source = INPUT_PATH + f'/{str(purchase_id)}.json'
        des = BACKUP_PATH + f'/{str(purchase_id)}.json'
        copy_file(source, des)
        
        purchase_id += 1
        time.sleep(10)  # 3600 seconds = 1 hour

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Retail Streaming File") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars", "/home/jeremie/.ivy2/jars/org.postgresql_postgresql-42.5.4.jar") \
        .config("spark.sql.shuffle.partitions", 8) \
        .getOrCreate()
        
        # .config("spark.sql.streaming.schemaInference", "true") \

    main()