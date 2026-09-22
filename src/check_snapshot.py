from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckSnapshot")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("/workspace/output/account_snapshots")

print(f"Total output row: {df.count()}")

df.groupBy("snapshot_date").count().show()

spark.stop()