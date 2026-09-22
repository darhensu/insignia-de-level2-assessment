import os
import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    countDistinct,
    lit,
    when
)
from pyspark.sql.types import DecimalType


# ambil parameter waktu pipeline dijalankan
parser = argparse.ArgumentParser()

parser.add_argument(
    "--snapshot-date",
    required=True
)

parser.add_argument(
    "--source",
    choices=["local", "oracle"],
    default="local"
)

parser.add_argument(
    "--output-path",
    default="/workspace/output/account_snapshots"
)

args = parser.parse_args()


# cek format tanggal biar gak salah input
try:
    snapshot_date = datetime.strptime(
        args.snapshot_date,
        "%Y-%m-%d"
    )
except ValueError:
    raise ValueError(
        "--snapshot-date harus menggunakan format YYYY-MM-DD"
    )

next_date = snapshot_date + timedelta(days=1)
next_date_str = next_date.strftime("%Y-%m-%d")


# kalau local pakai resource container, kalau production cluster yang ngatur
spark_builder = (
    SparkSession.builder
    .appName("NegaraBankAccountSnapshot")
)

if args.source == "local":
    spark_builder = spark_builder.master("local[*]")

spark = spark_builder.getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# overwrite hanya partition tanggal yang sedang diproses
spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)


try:

    # source production dari oracle
    if args.source == "oracle":

        # credential diambil dari environment, bukan ditulis di source code
        oracle_url = os.getenv("ORACLE_URL")
        oracle_user = os.getenv("ORACLE_USER")
        oracle_password = os.getenv("ORACLE_PASSWORD")

        if not all([
            oracle_url,
            oracle_user,
            oracle_password
        ]):
            raise ValueError(
                "credential oracle belum tersedia"
            )


        accounts = (
            spark.read
            .format("jdbc")
            .option("url", oracle_url)
            .option("dbtable", "ACCOUNTS")
            .option("user", oracle_user)
            .option("password", oracle_password)
            .load()
        )


        # filter dilakukan dari oracle supaya gak narik semua transaksi
        transaction_query = f"""
        (
            SELECT
                txn_id,
                account_id,
                txn_date,
                txn_type,
                amount,
                merchant_category,
                channel,
                reference_id,
                status
            FROM TRANSACTIONS
            WHERE txn_date >= TO_TIMESTAMP(
                '{args.snapshot_date} 00:00:00',
                'YYYY-MM-DD HH24:MI:SS'
            )
            AND txn_date < TO_TIMESTAMP(
                '{next_date_str} 00:00:00',
                'YYYY-MM-DD HH24:MI:SS'
            )
            AND status = 'COMPLETED'
        ) TXN
        """

        transactions = (
            spark.read
            .format("jdbc")
            .option("url", oracle_url)
            .option("dbtable", transaction_query)
            .option("user", oracle_user)
            .option("password", oracle_password)
            .load()
        )


    # untuk testing local pakai csv dari dataset assessment
    else:

        accounts = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv("/workspace/data/raw/accounts.csv")
        )

        transactions = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv("/workspace/data/raw/transactions.csv")
        )


    # kolom uang pakai decimal supaya gak muncul angka floating yang aneh
    accounts = (
        accounts
        .withColumn(
            "balance",
            col("balance").cast(DecimalType(15, 2))
        )
        .withColumn(
            "credit_limit",
            col("credit_limit").cast(DecimalType(15, 2))
        )
    )

    transactions = (
        transactions
        .withColumn(
            "amount",
            col("amount").cast(DecimalType(15, 2))
        )
    )


    # ambil transaksi completed untuk tanggal yang sedang diproses
    daily_transactions = (
        transactions
        .filter(
            (col("txn_date") >= snapshot_date) &
            (col("txn_date") < next_date)
        )
        .filter(
            col("status") == "COMPLETED"
        )
    )


    # ringkas transaksi harian per account
    daily_summary = (
        daily_transactions
        .groupBy("account_id")
        .agg(
            spark_sum("amount").alias(
                "daily_net_amount"
            ),
            count("txn_id").alias(
                "daily_txn_count"
            )
        )
    )


    # left join supaya account yang gak transaksi tetap masuk snapshot
    snapshot = (
        accounts
        .join(
            daily_summary,
            "account_id",
            "left"
        )
        .fillna({
            "daily_net_amount": 0,
            "daily_txn_count": 0
        })
        .withColumn(
            "snapshot_date",
            lit(args.snapshot_date).cast("date")
        )
    )


    # cek jumlah account dari source
    account_count = accounts.count()


    # dq dasar untuk jumlah row, null dan duplicate account
    snapshot_metrics = (
        snapshot
        .agg(
            count("*").alias(
                "snapshot_count"
            ),
            countDistinct("account_id").alias(
                "distinct_account_count"
            ),
            spark_sum(
                when(
                    col("account_id").isNull(),
                    1
                ).otherwise(0)
            ).alias(
                "null_account_count"
            )
        )
        .first()
    )


    snapshot_count = snapshot_metrics[
        "snapshot_count"
    ]

    distinct_account_count = snapshot_metrics[
        "distinct_account_count"
    ]

    null_account_count = snapshot_metrics[
        "null_account_count"
    ]


    # jumlah snapshot harus sama dengan jumlah account
    if snapshot_count != account_count:
        raise ValueError(
            f"jumlah snapshot tidak sesuai. "
            f"account={account_count}, "
            f"snapshot={snapshot_count}"
        )


    # account_id gak boleh null
    if null_account_count > 0:
        raise ValueError(
            f"ditemukan {null_account_count} "
            f"account_id null"
        )


    # jumlah account unik harus sama dengan jumlah row
    if distinct_account_count != snapshot_count:
        raise ValueError(
            "ditemukan duplicate account_id "
            "pada snapshot"
        )


    # cek jumlah dan total transaksi sebelum agregasi
    transaction_metrics = (
        daily_transactions
        .agg(
            count("*").alias(
                "transaction_count"
            ),
            spark_sum("amount").alias(
                "transaction_total"
            )
        )
        .first()
    )


    transaction_count = transaction_metrics[
        "transaction_count"
    ]

    transaction_total = (
        transaction_metrics["transaction_total"]
        or 0
    )


    # cek total transaksi setelah diringkas per account
    summary_total = (
        daily_summary
        .agg(
            spark_sum(
                "daily_net_amount"
            ).alias("summary_total")
        )
        .first()["summary_total"]
        or 0
    )


    # pastikan nilai transaksi gak berubah waktu proses agregasi
    if transaction_total != summary_total:
        raise ValueError(
            f"reconciliation gagal. "
            f"transaction={transaction_total}, "
            f"summary={summary_total}"
        )


    # simpan per tanggal supaya rerun gak bikin data double
    snapshot.write \
        .mode("overwrite") \
        .partitionBy("snapshot_date") \
        .parquet(args.output_path)


    # hasil singkat buat monitoring waktu pipeline selesai
    print("")
    print("account snapshot selesai")
    print("-------------------------")
    print(f"source              : {args.source}")
    print(f"snapshot date       : {args.snapshot_date}")
    print(f"completed transaction: {transaction_count}")
    print(f"total account       : {account_count}")
    print(f"snapshot row        : {snapshot_count}")
    print("data quality        : PASS")
    print(f"output              : {args.output_path}")
    print("")


    # tampilkan sample hasil buat pengecekan
    snapshot.select(
        "snapshot_date",
        "account_id",
        "customer_id",
        "account_type",
        "balance",
        "daily_net_amount",
        "daily_txn_count"
    ).show(
        10,
        truncate=False
    )


finally:

    # spark tetap ditutup walaupun proses error
    spark.stop()