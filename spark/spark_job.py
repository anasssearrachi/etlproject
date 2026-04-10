import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, round as spark_round,
    sum as spark_sum, count, avg, max as spark_max,
    min as spark_min, countDistinct,
    trim, upper, when, regexp_replace,
    hour, dayofweek, month, year
)
from pyspark.sql.types import StructType, StructField, StringType

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC         = os.getenv("TOPIC_NAME", "retail_topic")
PG_HOST       = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT       = os.getenv("POSTGRES_PORT", "5432")
PG_DB         = os.getenv("POSTGRES_DB", "retail_db")
PG_USER       = os.getenv("POSTGRES_USER", "etl_user")
PG_PASS       = os.getenv("POSTGRES_PASSWORD", "etl_pass")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "user":     PG_USER,
    "password": PG_PASS,
    "driver":   "org.postgresql.Driver"
}

# ── Session Spark ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("RetailETL") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Schéma JSON ───────────────────────────────────────────────────────────────
schema = StructType([
    StructField("InvoiceNo",   StringType()),
    StructField("StockCode",   StringType()),
    StructField("Description", StringType()),
    StructField("Quantity",    StringType()),
    StructField("InvoiceDate", StringType()),
    StructField("UnitPrice",   StringType()),
    StructField("CustomerID",  StringType()),
    StructField("Country",     StringType()),
])

# ── Lecture Kafka ─────────────────────────────────────────────────────────────
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "1000") \
    .load()

# ── Parse JSON + cast types ───────────────────────────────────────────────────
parsed_df = raw_df \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("Quantity",    col("Quantity").cast("integer")) \
    .withColumn("UnitPrice",   col("UnitPrice").cast("double")) \
    .withColumn("InvoiceDate", to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))

# ── Nettoyage ─────────────────────────────────────────────────────────────────
cleaned_df = parsed_df \
    .dropDuplicates(["InvoiceNo", "StockCode"]) \
    .filter(col("Quantity").isNotNull()  & (col("Quantity")  > 0)) \
    .filter(col("UnitPrice").isNotNull() & (col("UnitPrice") > 0)) \
    .filter(col("InvoiceNo").isNotNull()) \
    .filter(col("StockCode").isNotNull()) \
    .withColumn("Description",
        upper(trim(regexp_replace(col("Description"), r"[^\w\s]", "")))) \
    .withColumn("CustomerID",
        when(col("CustomerID").isNull() | (trim(col("CustomerID")) == ""),
             "Unknown")
        .otherwise(trim(col("CustomerID")))) \
    .withColumn("Country", trim(col("Country"))) \
    .withColumn("is_return", col("InvoiceNo").startswith("C")) \
    .filter(col("is_return") == False) \
    .withColumn("TotalAmount",
        spark_round(col("Quantity") * col("UnitPrice"), 2)) \
    .withColumn("hour_of_day",  hour("InvoiceDate")) \
    .withColumn("day_of_week",  dayofweek("InvoiceDate")) \
    .withColumn("month",        month("InvoiceDate")) \
    .withColumn("year",         year("InvoiceDate")) \
    .drop("is_return")

# ── Écriture par batch ────────────────────────────────────────────────────────
def write_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[Spark] Batch {batch_id} — vide, skip")
        return

    row_count = batch_df.count()
    print(f"[Spark] Batch {batch_id} — {row_count} lignes")

    # ── Table 1 : transactions nettoyées ──────────────────────────────────────
    batch_df.write.jdbc(
        url=JDBC_URL, table="retail_transactions",
        mode="append", properties=JDBC_PROPS
    )

    # ── Table 2 : ventes par pays ─────────────────────────────────────────────
    country_agg = batch_df.groupBy("Country").agg(
        spark_sum("TotalAmount").alias("total_revenue"),
        count("InvoiceNo").alias("invoice_count"),
        avg("TotalAmount").alias("avg_order_value"),
        countDistinct("CustomerID").alias("unique_customers"),
        spark_sum("Quantity").alias("total_units_sold")
    )
    country_agg.write.jdbc(
        url=JDBC_URL, table="sales_by_country",
        mode="append", properties=JDBC_PROPS
    )

    # ── Table 3 : ventes par produit ──────────────────────────────────────────
    product_agg = batch_df.groupBy("StockCode", "Description").agg(
        spark_sum("Quantity").alias("total_qty_sold"),
        spark_sum("TotalAmount").alias("total_revenue"),
        avg("UnitPrice").alias("avg_unit_price"),
        countDistinct("CustomerID").alias("unique_buyers"),
        countDistinct("Country").alias("countries_reached")
    )
    product_agg.write.jdbc(
        url=JDBC_URL, table="sales_by_product",
        mode="append", properties=JDBC_PROPS
    )

    # ── Table 4 : KPIs globaux par batch ──────────────────────────────────────
    from pyspark.sql import Row
    from pyspark.sql.functions import lit
    import datetime

    kpis = batch_df.agg(
        spark_sum("TotalAmount").alias("total_revenue"),
        count("InvoiceNo").alias("total_transactions"),
        countDistinct("CustomerID").alias("unique_customers"),
        countDistinct("Country").alias("active_countries"),
        countDistinct("StockCode").alias("unique_products"),
        avg("TotalAmount").alias("avg_basket_value"),
        spark_max("TotalAmount").alias("max_order_value"),
        spark_min("TotalAmount").alias("min_order_value"),
        spark_sum("Quantity").alias("total_units_sold"),
        (spark_sum("TotalAmount") / countDistinct("CustomerID"))
            .alias("revenue_per_customer")
    ).withColumn("batch_id", lit(batch_id)) \
     .withColumn("computed_at", lit(str(datetime.datetime.now())))

    kpis.write.jdbc(
        url=JDBC_URL, table="kpis_global",
        mode="append", properties=JDBC_PROPS
    )

    # ── Table 5 : ventes par heure de la journée ──────────────────────────────
    hourly_agg = batch_df.groupBy("hour_of_day").agg(
        spark_sum("TotalAmount").alias("total_revenue"),
        count("InvoiceNo").alias("transaction_count"),
        avg("TotalAmount").alias("avg_order_value")
    )
    hourly_agg.write.jdbc(
        url=JDBC_URL, table="sales_by_hour",
        mode="append", properties=JDBC_PROPS
    )

    # ── Table 6 : ventes par mois/année ──────────────────────────────────────
    monthly_agg = batch_df.groupBy("year", "month").agg(
        spark_sum("TotalAmount").alias("total_revenue"),
        count("InvoiceNo").alias("invoice_count"),
        countDistinct("CustomerID").alias("unique_customers"),
        avg("TotalAmount").alias("avg_basket_value")
    )
    monthly_agg.write.jdbc(
        url=JDBC_URL, table="sales_by_month",
        mode="append", properties=JDBC_PROPS
    )

    # ── Table 7 : ventes par jour de la semaine ───────────────────────────────
    daily_agg = batch_df.groupBy("day_of_week").agg(
        spark_sum("TotalAmount").alias("total_revenue"),
        count("InvoiceNo").alias("transaction_count"),
        avg("TotalAmount").alias("avg_order_value")
    )
    daily_agg.write.jdbc(
        url=JDBC_URL, table="sales_by_dayofweek",
        mode="append", properties=JDBC_PROPS
    )

    # ── Table 8 : top clients ─────────────────────────────────────────────────
    customer_agg = batch_df \
        .filter(col("CustomerID") != "Unknown") \
        .groupBy("CustomerID", "Country").agg(
            spark_sum("TotalAmount").alias("total_spent"),
            count("InvoiceNo").alias("total_orders"),
            avg("TotalAmount").alias("avg_order_value"),
            spark_sum("Quantity").alias("total_units_bought"),
            spark_max("InvoiceDate").alias("last_purchase_date")
        )
    customer_agg.write.jdbc(
        url=JDBC_URL, table="customer_stats",
        mode="append", properties=JDBC_PROPS
    )

    print(f"[Spark] Batch {batch_id} — 8 tables écrites avec succès")

# ── Démarrage streaming ───────────────────────────────────────────────────────
query = cleaned_df.writeStream \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

print("[Spark] Pipeline démarré — en attente de messages Kafka...")
query.awaitTermination()