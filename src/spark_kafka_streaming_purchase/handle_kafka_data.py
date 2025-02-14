import os
import findspark

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/home/jeremie/Downloads/Spark"

findspark.init()


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

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

# items_df = batch_df.select(
#         col("product_id"),
#         col("product_name"),
#         col("quantity"),
#         col("price").cast(DecimalType(15, 2)).alias("price"),
#         col("total_price").cast(DecimalType(15, 2)).alias("total_price")
#     )

def update_inventory(inventory_df, items_df):
    if inventory_df.isEmpty():
        update_df = items_df.select(
            col("product_id"),
            col("product_name"),
            col("quantity"), 
            (col("price") * 1.2).alias("sale_prices"),
            lit(50).alias("reorder_level"),
            current_timestamp().alias("last_updated")
        )
    else:
        join_condition = inventory_df["product_id"] == items_df["product_id"]
        
        update_df = inventory_df.join(items_df, join_condition, "inner") \
            .select(
                coalesce(inventory_df["product_id"], items_df["product_id"]).alias("product_id"),
                coalesce(inventory_df["product_name"], items_df["product_name"]).alias("product_name"),
                (coalesce(inventory_df["quantity"], lit(0)) - coalesce(items_df["quantity"], lit(0))).alias("quantity"),
                when(inventory_df["sale_prices"].isNotNull(), inventory_df["sale_prices"])
                .otherwise(items_df["price"] * 1.2).alias("sale_prices"),
                coalesce(inventory_df["reorder_level"], lit(50)).alias("reorder_level"),
                current_timestamp().alias("last_updated")
            )
    return update_df


# processed_df = explode_df.select(
#         "customer_id",
#         "customer_name",
#         "total_invoice_amount",
#         "timestamp",
#         col("item.product_id").alias("product_id"),
#         col("item.product_name").alias("product_name"),
#         col("item.quantity").alias("quantity"),
#         col("item.price").alias("price"),
#         col("item.total_price").alias("total_price")
        
def write_invoices_and_items(batch_df, batch_id):
    # Tách line_item thành bảng import_items
    items_df = batch_df.select(
        col("product_id"),
        col("product_name"),
        col("quantity"),
        col("price").cast(DecimalType(15, 2)).alias("price"),
        col("total_price").cast(DecimalType(15, 2)).alias("total_price")
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
    
def convert_data_types(df):
    # Chuyển đổi kiểu dữ liệu
    df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
    return df

if __name__ == "__main__":
    # Tạo SparkSession
    spark = SparkSession.builder \
        .appName("KafkaJSONConsumer") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars", "/home/jeremie/.ivy2/jars/org.postgresql_postgresql-42.5.4.jar") \
        .config("spark.sql.shuffle.partitions", 8) \
        .getOrCreate()

    # Đọc dữ liệu từ Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test_input") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Định nghĩa schema cho JSON
    json_schema = StructType([
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType()),
        StructField("purchased_items", ArrayType(
            StructType([
                StructField("product_id", StringType()),
                StructField("product_name", StringType()),
                StructField("quantity", IntegerType()),
                StructField("price", DecimalType()),
                StructField("total_price", DecimalType())
            ])
        )),
        StructField("total_invoice_amount", DecimalType()),
        StructField("timestamp", StringType())
    ])

    value_df = kafka_df.select(from_json(col("value").cast("string"), json_schema).alias("value"))

    explode_df = value_df.select(
        "value.customer_id",
        "value.customer_name",
        "value.total_invoice_amount",
        "value.timestamp",
        explode("value.purchased_items").alias("item")
    )

    processed_df = explode_df.select(
        "customer_id",
        "customer_name",
        "total_invoice_amount",
        "timestamp",
        col("item.product_id").alias("product_id"),
        col("item.product_name").alias("product_name"),
        col("item.quantity").alias("quantity"),
        col("item.price").alias("price"),
        col("item.total_price").alias("total_price")
    )
    
    df_converted = convert_data_types(processed_df)
    
    '''
    Update database
    '''
    # Áp dụng hàm write_invoices_and_items
    df_converted.writeStream \
        .foreachBatch(write_invoices_and_items) \
        .queryName("Invoices and Items Writer") \
        .option("checkpointLocation", "chk-point-dir-items-purchase") \
        .start()

    kafka_target_df = processed_df.selectExpr(
        """to_json(named_struct(
            'customer_id', customer_id,
            'customer_name', customer_name,
            'total_invoice_amount', total_invoice_amount,
            'timestamp', timestamp,
            'product_id', product_id,
            'product_name', product_name,
            'quantity', quantity,
            'price', price,
            'total_price', total_price
        )) as value"""
    )

    # In dữ liệu ra console
    notification_writer_query = kafka_target_df.writeStream \
        .format("kafka") \
        .queryName("Notification Writer") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "test_notify") \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir-kafka") \
        .start()

    notification_writer_query.awaitTermination()
