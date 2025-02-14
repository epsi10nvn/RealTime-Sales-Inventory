import os
import findspark

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/home/jeremie/Downloads/Spark"

findspark.init()


from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaJSONConsumer") \
    .master("local[*]") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test_input") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Định nghĩa schema cho JSON
json_schema = StructType([
    StructField("key1", StringType()),
    StructField("key2", StringType()),
])

value_df = kafka_df.select(f.from_json(f.col("value").cast("string"), json_schema).alias("value"))

explode_df = value_df.selectExpr(
    "value.key1",
    "value.key2"
)

kafka_target_df = explode_df.selectExpr("key1 as key",
                                        """to_json(named_struct(
                                            'key2', key2
                                        )) as value
                                        """)

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
