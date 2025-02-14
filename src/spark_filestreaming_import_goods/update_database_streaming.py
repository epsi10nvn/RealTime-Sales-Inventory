from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp
from pyspark.sql import functions as f
from pyspark.sql.types import *
import os
import time

jdbc_url = "jdbc:postgresql://localhost:5432/retail_repository"
db_properties = {
    "user": "jeremie",
    "password": "20112001",
    "driver": "org.postgresql.Driver"
}

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

reorder_levels = {
    "A1": 200,   # Ballpoint Pen
    "A2": 100,   # Notebook
    "A3": 150,   # Ruler
    "A4": 200,   # Eraser
    "A5": 10,    # Backpack
    "A6": 250,   # Pencil
    "A7": 20,    # Calculator
    "A8": 50,    # A4 Paper
    "A9": 50,    # Scissors
    "A10": 100   # Correction Pen
}
def update_inventory(inventory_df, items_df):
    if inventory_df.isEmpty():
        update_df = items_df.select(
            f.col("product_id"),
            f.col("product_name"),
            f.col("quantity"), 
            (f.col("price") * 1.2).alias("sale_prices"),
            f.lit(50).alias("reorder_level"),
            f.current_timestamp().alias("last_updated")
        )
    else:
        join_condition = inventory_df["product_id"] == items_df["product_id"]
        
        update_df = inventory_df.join(items_df, join_condition, "full_outer") \
            .select(
                f.coalesce(inventory_df["product_id"], items_df["product_id"]).alias("product_id"),
                f.coalesce(inventory_df["product_name"], items_df["product_name"]).alias("product_name"),
                (f.coalesce(inventory_df["quantity"], f.lit(0)) + f.coalesce(items_df["quantity"], f.lit(0))).alias("quantity"),
                f.when(inventory_df["sale_prices"].isNotNull(), inventory_df["sale_prices"])
                .otherwise(items_df["price"] * 1.2).alias("sale_prices"),
                f.coalesce(inventory_df["reorder_level"], f.lit(50)).alias("reorder_level"),
                f.current_timestamp().alias("last_updated")
            )
    return update_df

def write_invoices_and_items(batch_df, batch_id):
    invoices_df = batch_df.select(
        col("import_id"),
        col("delivery_hour"),
        col("total_invoice_amount")
    ).distinct() \
    .withColumn("timestamp_inserted", current_timestamp())

    # Tách line_item thành bảng import_items
    items_df = batch_df.select(
        col("import_id"),
        col("line_item.product_id").alias("product_id"),
        col("line_item.product_name").alias("product_name"),
        col("line_item.quantity").cast("int").alias("quantity"),
        col("line_item.price").cast(DecimalType(15, 2)).alias("price"),
        col("line_item.total_amount").cast(DecimalType(15, 2)).alias("total_amount")
    )
    
    
    
    inventory_df = spark.read.jdbc(
        url=jdbc_url,
        table=query_select_current_inventory,
        properties=db_properties
    )
    
    update_df = update_inventory(inventory_df, items_df)
    
    update_df.show()

    # Update inventory
    update_df.write.jdbc(
        url=jdbc_url,
        table="inventory",
        mode="append",
        # mode="overwrite",
        properties=db_properties
    )
    
    
    # Ghi invoices trước
    invoices_df.write.jdbc(
        url=jdbc_url,
        table="import_invoices",
        mode="append",
        properties=db_properties
    )

    # Ghi items sau
    items_df.write.jdbc(
        url=jdbc_url,
        table="import_items",
        mode="append",
        properties=db_properties
    )
    
    invoices_df.show()
    items_df.show()

    
def convert_data_types(df):
    # Chuyển đổi kiểu dữ liệu
    df = df.withColumn("delivery_hour", col("delivery_hour").cast(TimestampType()))
    return df
    
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

    # logger = Log4j(spark)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    INPUT_PATH = os.path.abspath(os.path.join(current_dir, "../../input/filestreaming_import_goods"))
    print(INPUT_PATH)
    
    # INPUT_PATH = "../../input/filestreaming_import_goods/"
    
    schema = StructType([
        StructField("import_id", StringType()),
        StructField("delivery_hour", StringType()),
        StructField("total_invoice_amount", DecimalType(15, 2)),
        StructField("items", ArrayType(StructType([
            StructField("product_id", StringType()),
            StructField("product_name", StringType()),
            StructField("quantity", IntegerType()),
            StructField("price", DecimalType(15, 2)),
            StructField("total_amount", DecimalType(15, 2))
        ])))
    ])
    
    raw_df = spark.readStream \
        .format("json") \
        .schema(schema) \
        .option("path", INPUT_PATH) \
        .option("maxFilesPerTrigger", "1") \
        .option("cleanSource", "delete") \
        .load()
    
    # raw_df.printSchema()
        
    explode_df = raw_df.selectExpr("import_id",
                                   "delivery_hour",
                                   "total_invoice_amount",
                                   "explode(items) as line_item")
    explode_df.printSchema()
    
    
    # Chuyển đổi kiểu dữ liệu
    df_converted = convert_data_types(explode_df)
    
    # Áp dụng hàm write_invoices_and_items
    df_converted.writeStream \
        .foreachBatch(write_invoices_and_items) \
        .queryName("Invoices and Items Writer") \
        .trigger(processingTime="30 seconds") \
        .option("checkpointLocation", "chk-point-dir-invoices-items") \
        .start() \
        .awaitTermination()
        
    
    
    


    
    
