from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("ExportAccountSnapshot")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# baca hasil snapshot q1b
snapshot = spark.read.parquet(
    "/workspace/output/account_snapshots/snapshot_date=2025-08-01"
)


# snapshot_date hilang dari kolom karena dibaca langsung dari folder partition,
# jadi kita tambahkan lagi untuk kebutuhan postgres
from pyspark.sql.functions import lit

snapshot = snapshot.withColumn(
    "snapshot_date",
    lit("2025-08-01").cast("date")
)


# urutan kolom disamakan dengan tabel postgres
snapshot = snapshot.select(
    "snapshot_date",
    "account_id",
    "customer_id",
    "account_type",
    "product_name",
    "opened_date",
    "status",
    "balance",
    "credit_limit",
    "interest_rate",
    "daily_net_amount",
    "daily_txn_count"
)


# satu file csv biar gampang di-copy ke postgres
snapshot.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/workspace/output/account_snapshot_csv")


print(f"total row export: {snapshot.count()}")

spark.stop()