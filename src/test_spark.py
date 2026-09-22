from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("InsigniaTest")
    .master("local[*]")
    .getOrCreate()
)

transactions = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/workspace/data/raw/transactions.csv")
)

print("Jumlah transaksi:", transactions.count())

transactions.printSchema()

transactions.show(5, truncate=False)

spark.stop()