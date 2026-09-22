# Insignia Data Engineer Level 2 Assessment

Assessment ini berisi implementasi dan rancangan solusi untuk tiga area utama:

1. Pipeline optimization menggunakan PySpark
2. Advanced SQL untuk regulatory reporting dan fraud detection
3. End-to-end data platform architecture

Untuk testing lokal saya menggunakan:

- Docker Desktop
- Apache Spark 3.5.9
- Python
- PostgreSQL 16
- Draw.io untuk architecture diagram

Dataset assessment mencakup 5.000 customer, 7.954 account, sekitar 1,99 juta transaksi, credit score, clickstream event, support ticket, dan acquisition data.

---

# Project Structure

```text
Insignia-DE-Test/
│
├── data/
│   ├── raw/
│   │   ├── customers.csv
│   │   ├── accounts.csv
│   │   ├── transactions.csv
│   │   ├── credit_scores.csv
│   │   ├── app_events.csv
│   │   ├── support_tickets.csv
│   │   └── acquisition_channels.csv
│   │
│   └── synthetic/
│       └── transaction_locations.csv
│
├── src/
│   ├── account_snapshot.py
│   ├── check_snapshot.py
│   ├── export_snapshot_csv.py
│   └── generate_transaction_locations.py
│
├── sql/
│   ├── setup_postgres.sql
│   ├── q2d_customer_health_scorecard.sql
│   ├── q2e_fraud_detection.sql
│   ├── q2f_incremental_health_scorecard.sql
│   └── q2f_incremental_load.sql
│
├── orchestration/
│   └── q1c_databricks_workflow.md
│
├── architecture/
│   ├── q3g.*
│   ├── q3h.*
│   └── q3i.*
│
├── output/
│   └── account_snapshots/
│
└── README.md
```

Nama file diagram dapat disesuaikan dengan hasil export Draw.io.

---

# Question 1 — Pipeline Optimization

## Q1a. Masalah pada Pipeline Existing

Setelah melihat pipeline PySpark yang diberikan, saya menemukan beberapa masalah yang bisa memengaruhi performa, keamanan, hasil data, dan kestabilan proses ketika dijalankan di production.

| No. | Masalah | Dampak | Solusi |
|---|---|---|---|
| 1 | Username dan password database ditulis langsung di source code | Credential bisa terlihat jika code dibagikan atau masuk repository | Simpan credential di secret manager seperti Databricks Secret Scope atau AWS Secrets Manager |
| 2 | Seluruh tabel transaksi dibaca setiap malam | Pipeline menjadi semakin lambat dan memberi beban tidak perlu ke Oracle | Gunakan incremental loading dan hanya ambil data untuk periode yang sedang diproses |
| 3 | Filter transaksi menggunakan tanggal tetap `2026-01-01` | Data yang dibaca akan terus bertambah setiap hari | Jadikan tanggal proses sebagai parameter |
| 4 | Output menggunakan full `overwrite` | Historical snapshot berpotensi tertimpa | Tambahkan `snapshot_date` dan partition output berdasarkan tanggal |
| 5 | Tidak ada data quality check | Data bermasalah dapat langsung digunakan BI, ML, atau regulatory report | Tambahkan row count, null check, duplicate check, dan reconciliation |
| 6 | Status transaksi belum difilter | Transaksi `FAILED`, `PENDING`, atau `REVERSED` bisa ikut dihitung | Hanya gunakan transaksi yang dianggap valid, dalam implementasi ini `COMPLETED` |
| 7 | Belum ada logging, retry, dan error handling yang jelas | Failure sulit ditelusuri dan downstream pipeline bisa terganggu | Tambahkan monitoring, retry, logging, dan alert |
| 8 | Pipeline belum idempotent | Re-run tanggal yang sama berpotensi menghasilkan duplicate atau hasil berbeda | Gunakan deterministic `snapshot_date` dan replace hanya partition tanggal tersebut |

### Catatan Mengenai Nilai Transaksi

Pipeline existing langsung menggunakan:

```python
.agg({"amount": "sum", "txn_id": "count"})
```

Dari profiling dataset, kolom `amount` sudah membawa arah transaksi.

Contohnya:

```text
CREDIT        → positif
DEBIT         → negatif
TRANSFER_IN   → positif
TRANSFER_OUT  → negatif
FEE           → negatif
```

Karena itu implementasi saya **tidak mengubah tanda transaksi lagi** dan tetap menggunakan nilai `amount` dari source.

Hal ini penting karena membuat mapping tanda transaksi ulang berpotensi menyebabkan nilai berubah dua kali.

Pada production tetap perlu ada business-rule validation untuk memastikan penggunaan tanda pada setiap `txn_type` konsisten dengan source system.

---

# Q1b. Production-Grade Account Snapshot Pipeline

Pipeline diperbaiki menggunakan PySpark dengan fokus pada:

- incremental loading
- keamanan credential
- data quality
- idempotency
- historical snapshot

Implementasi utama terdapat pada:

```text
src/account_snapshot.py
```

## Incremental Loading

Pipeline menerima parameter:

```text
--snapshot-date
```

Contoh:

```text
2025-08-01
```

Untuk production Oracle, filter tanggal dimasukkan langsung ke query JDBC sehingga Spark tidak perlu membaca seluruh tabel transaksi terlebih dahulu.

Secara konsep:

```sql
WHERE txn_date >= snapshot_date
  AND txn_date < snapshot_date + 1 day
  AND status = 'COMPLETED'
```

Dengan cara ini, jika source production memiliki jutaan transaksi per hari, hanya transaksi yang dibutuhkan yang ditarik dari Oracle.

---

## Account Snapshot

Transaksi harian diringkas berdasarkan:

```text
account_id
```

untuk mendapatkan:

```text
daily_net_amount
daily_txn_count
```

Hasil kemudian di-left join ke tabel account sehingga account yang tidak memiliki transaksi pada hari tersebut tetap masuk ke snapshot.

---

## Partitioning dan Time Travel

Output memiliki kolom:

```text
snapshot_date
```

dan disimpan menggunakan partition:

```text
snapshot_date=YYYY-MM-DD
```

Contoh:

```text
output/account_snapshots/
└── snapshot_date=2025-08-01/
```

Pipeline menggunakan dynamic partition overwrite.

Artinya jika job untuk tanggal yang sama dijalankan ulang, hanya partition tersebut yang diganti.

Historical snapshot tanggal lain tidak ikut terhapus.

---

## Secret Management

Credential Oracle tidak disimpan langsung di source code.

Pipeline membaca:

```text
ORACLE_URL
ORACLE_USER
ORACLE_PASSWORD
```

dari environment variable.

Pada production variable tersebut dapat di-inject dari:

```text
Databricks Secret Scope
```

atau:

```text
AWS Secrets Manager
```

---

## Data Quality Check

Sebelum snapshot ditulis, pipeline menjalankan beberapa pengecekan.

### 1. Row Count

Jumlah row snapshot harus sama dengan jumlah account.

### 2. Null Account ID

`account_id` tidak boleh NULL.

### 3. Duplicate Account

Satu account hanya boleh muncul satu kali pada snapshot yang sama.

### 4. Transaction Aggregation Reconciliation

Total nilai transaksi sebelum agregasi harus sama dengan total setelah transaksi diringkas per account.

---

## Hasil Testing Lokal

Pipeline berhasil diuji terhadap dataset assessment.

Contoh hasil:

```text
snapshot date         : 2025-08-01
completed transaction : 10503
total account         : 7954
snapshot row          : 7954
data quality          : PASS
```

Pipeline juga dijalankan ulang menggunakan tanggal yang sama.

Jumlah output tetap:

```text
7954 rows
```

sehingga tidak menghasilkan duplicate snapshot.

---

## Limitation Balance Reconciliation

Dataset assessment hanya menyediakan current account balance dan tidak menyediakan historical account balance untuk setiap tanggal.

Karena itu full reconciliation seperti:

```text
previous balance
+ transaction hari ini
= ending balance
```

belum bisa dilakukan secara lengkap menggunakan sample dataset.

Pada production, reconciliation tersebut bisa dilakukan menggunakan snapshot hari sebelumnya.

---

# Q1c. Orchestration dan Notification

Untuk orchestration saya memilih:

```text
Databricks Workflows
```

karena pipeline Account Snapshot menggunakan PySpark dan pada skenario NegaraBank credit scoring juga berjalan di Databricks.

Workflow utama:

```text
Account Snapshot
      ↓
DQ PASS
      ↓
Credit Scoring
```

Task `Credit Scoring` hanya dijalankan jika Account Snapshot dan data quality check berhasil.

Jika task gagal:

```text
Initial Attempt
      ↓
Retry 1
      ↓
Retry 2
      ↓
Failed after 3 attempts
      ↓
Engineer Alert
```

Retry policy berlaku untuk task yang membutuhkan retry.

Jika proses tetap gagal setelah tiga attempt, notification dikirim ke engineer melalui notification configuration di Databricks.

Karena Credit Scoring langsung memiliki dependency terhadap Account Snapshot, pipeline ML bisa mulai segera setelah snapshot tersedia tanpa harus menunggu schedule terpisah.

Pendekatan ini membantu memenuhi SLA bahwa Credit Scoring harus mulai menggunakan snapshot maksimal 30 menit setelah data tersedia.

---

# Question 2 — Advanced SQL

Testing SQL dilakukan menggunakan PostgreSQL 16 dalam Docker.

Dataset lokal yang digunakan:

```text
customers      : 5,000
accounts       : 7,954
transactions   : 1,991,349
credit_scores  : 7,452
```

---

# Q2d. Monthly Customer Health Scorecard

SQL terdapat pada:

```text
sql/q2d_customer_health_scorecard.sql
```

Query menghasilkan satu row untuk setiap:

```text
customer + month
```

Metric yang dihitung:

- total balance seluruh account
- previous month balance
- month-over-month balance change
- month-over-month balance change percentage
- transaction count berdasarkan `txn_type`
- average transaction amount berdasarkan channel
- credit utilization
- probability of default
- risk flag

---

## Customer Coverage

Base scorecard menggunakan:

```text
bronze.customers
```

bukan langsung `bronze.accounts`.

Hal ini dilakukan karena requirement meminta scorecard untuk **setiap customer**, termasuk customer yang belum memiliki account.

Hasil profiling:

```text
total customer         : 5000
customer punya account : 4000
customer tanpa account : 1000
```

Dengan approach ini 1.000 customer yang belum memiliki account tetap bisa muncul di scorecard.

---

## Month-over-Month Balance

Month-over-month dihitung menggunakan:

```sql
LAG(total_balance)
OVER (
    PARTITION BY customer_id
    ORDER BY month_start
)
```

`LAG` mengambil nilai balance customer pada bulan sebelumnya sehingga perubahan balance dapat dihitung.

---

## Transaction Aggregation

Transaction count dihitung menggunakan conditional aggregation untuk:

```text
DEBIT
CREDIT
TRANSFER_IN
TRANSFER_OUT
PAYMENT
FEE
```

Transaction data diringkas terlebih dahulu ke grain:

```text
customer + month
```

sebelum di-join dengan dataset lain.

Tujuannya agar join tidak dilakukan langsung terhadap raw transaction yang jumlahnya bisa mencapai puluhan juta row pada production.

---

## Average Transaction Amount

Karena source menggunakan nilai positif dan negatif untuk menunjukkan arah transaksi, average transaction amount dihitung menggunakan:

```sql
AVG(ABS(amount))
```

Tujuannya adalah melihat besar nominal transaksi, bukan net cash movement.

---

## Credit Utilization

Untuk credit card:

```text
credit utilization
=
credit card balance / credit limit
```

Hasil profiling dataset menunjukkan:

```text
credit card account : 1971
credit card balance : seluruhnya 0
```

Karena itu credit utilization pada sample dataset bernilai `0`.

Ini merupakan karakteristik dataset, bukan error query.

---

## Risk Flag

Customer diberi:

```text
risk_flag = TRUE
```

jika salah satu kondisi berikut terpenuhi:

```text
credit utilization > 80%
OR
probability of default > 0.30
OR
balance turun > 30% dibanding bulan sebelumnya
```

---

## Catatan Testing MoM

Snapshot yang dibuat saat local testing baru memiliki satu periode:

```text
2025-08
```

Karena belum tersedia snapshot bulan sebelumnya, nilai:

```text
previous_month_balance
mom_balance_change
mom_balance_change_pct
```

masih `NULL`.

Ini expected untuk periode pertama.

---

# Q2e. Fraud Detection

SQL terdapat pada:

```text
sql/q2e_fraud_detection.sql
```

Fraud detection dibuat berdasarkan tiga rule.

Output akhir mengikuti format:

```text
customer_id
alert_type
alert_date
details_json
```

Ketiga detector digabung menggunakan:

```sql
UNION ALL
```

karena satu customer bisa memenuhi lebih dari satu jenis fraud rule pada hari yang sama.

---

## Rule 1 — High Frequency Transaction

Alert:

```text
HIGH_FREQUENCY_1H
```

Rule:

```text
5 atau lebih transaksi dalam window 1 jam
```

Implementasi menggunakan PostgreSQL window function:

```sql
COUNT(*) OVER (
    PARTITION BY customer_id
    ORDER BY EXTRACT(EPOCH FROM txn_date)
    RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
)
```

`3600` berarti 3600 detik atau satu jam.

Untuk mengurangi duplicate alert dari overlapping window, hasil diringkas menjadi satu alert per:

```text
customer + tanggal
```

dan `details_json` menyimpan:

```text
max_transactions_in_1h
first_trigger_at
```

---

## Rule 2 — Multi City Transaction

Alert:

```text
MULTI_CITY_1D
```

Rule:

```text
transaksi di 3 kota atau lebih dalam satu hari
```

Assessment membutuhkan join ke merchant-location data.

Namun dataset yang diberikan tidak memiliki:

```text
merchant_id
merchant_city
merchant_location
```

Karena itu saya membuat synthetic transaction-location dataset khusus untuk menguji logic SQL.

Generator:

```text
src/generate_transaction_locations.py
```

Output:

```text
data/synthetic/transaction_locations.csv
```

Jumlah synthetic location yang terbentuk:

```text
663132 rows
```

Data lokasi dibuat deterministic berdasarkan `txn_id`.

Artinya jika generator dijalankan ulang, transaksi yang sama akan mendapatkan kota yang sama.

Synthetic rule yang digunakan:

```text
95% transaksi → kota customer
5% transaksi  → kota lain
```

Daftar kota tetap diambil dari kota yang memang tersedia pada dataset customer.

Synthetic data ini **hanya digunakan untuk testing SQL** dan tidak dianggap sebagai source production NegaraBank.

Pada production tabel ini harus diganti dengan merchant-location source yang sebenarnya.

Multi-city detection kemudian menggunakan:

```sql
COUNT(DISTINCT merchant_city)
```

per:

```text
customer + tanggal
```

dan alert dibuat jika jumlah kota:

```text
>= 3
```

`details_json` menyimpan jumlah serta daftar kota yang terlibat.

---

## Rule 3 — High Amount Compared to 30-Day Average

Alert:

```text
HIGH_AMOUNT_30D
```

Rule:

```text
single transaction
>
3 × average transaction amount customer selama 30 hari sebelumnya
```

Average dihitung menggunakan rolling window:

```text
30 hari sebelum transaksi saat ini
```

Transaksi yang sedang diperiksa tidak ikut dimasukkan ke average pembanding.

Karena `amount` memiliki nilai positif dan negatif, perbandingan menggunakan:

```text
ABS(amount)
```

Rule juga baru dijalankan setelah dataset memiliki minimal 30 hari history.

Hal ini dilakukan agar transaksi pada hari-hari awal dataset tidak dibandingkan dengan average yang baru terbentuk dari beberapa transaksi saja.

---

## Hasil Fraud Detection

Hasil final:

```text
HIGH_AMOUNT_30D      :   1
HIGH_FREQUENCY_1H   :  25
MULTI_CITY_1D       : 429
--------------------------------
TOTAL                : 455 alerts
```

Contoh high amount alert:

```text
customer        : CUST0000434
date            : 2025-11-05
amount          : 4,798,834.02
30-day average  : 1,512,995.58
ratio           : 3.17x
```

Catatan:

Jumlah `MULTI_CITY_1D` dipengaruhi oleh synthetic merchant-location data sehingga angka tersebut tidak boleh dianggap sebagai fraud rate sebenarnya.

Tujuan synthetic data hanya untuk memastikan query dapat diuji end-to-end.

---

# Q2f. Materialization Optimization

Current materialization membutuhkan sekitar 45 menit karena view dihitung ulang terhadap full dataset setiap malam.

Saya memilih pendekatan:

```text
dbt incremental model
```

dibanding full materialized-view refresh.

---

## Alasan Memilih Incremental

Dengan volume production sekitar 60M+ transactions, menghitung ulang seluruh historical data setiap malam akan membuang compute untuk periode yang sebenarnya sudah tidak berubah.

Incremental processing hanya menghitung:

```text
periode yang berubah
```

Untuk Monthly Customer Health Scorecard:

```text
grain = customer + month
```

Unique key:

```text
(month_start, customer_id)
```

Current month diproses kembali dan previous month juga dapat ikut dibaca untuk kebutuhan:

```text
LAG
MoM calculation
late-arriving transactions
```

---

## Local PostgreSQL Implementation

Target dibuat sebagai:

```text
gold.customer_health_scorecard
```

Primary key:

```text
(month_start, customer_id)
```

Incremental merge disimulasikan menggunakan:

```sql
INSERT ...
ON CONFLICT (month_start, customer_id)
DO UPDATE
```

Dengan cara ini:

```text
new customer/month
→ INSERT

existing customer/month
→ UPDATE
```

bukan menambahkan duplicate.

---

## Hasil Testing Idempotency

Initial load:

```text
month_start    : 2025-08-01
total customer : 5000
risk customer  : 1197
```

Setelah incremental load dijalankan ulang:

```text
month_start    : 2025-08-01
total customer : 5000
risk customer  : 1197
```

Jumlah row tetap sama sehingga rerun bersifat idempotent.

---

## Production dbt Concept

Pada production pola yang sama dapat dibuat menggunakan:

```sql
{{ config(
    materialized='incremental',
    unique_key=['month_start', 'customer_id'],
    incremental_strategy='merge'
) }}
```

Incremental filter dapat mengambil current dan previous period agar late-arriving transaction masih bisa dikoreksi.

Saya memilih dbt incremental karena:

- tidak perlu full recomputation setiap malam
- SQL transformation mudah diuji
- mendukung documentation
- membantu lineage
- tidak terlalu terikat ke satu warehouse tertentu

Snowflake Dynamic Tables juga merupakan pilihan yang valid, tetapi pada rancangan platform ini Databricks sudah digunakan sebagai compute utama sehingga saya menghindari penambahan compute platform yang overlap tanpa kebutuhan yang jelas.

---

# Question 3 — Platform Architecture

## Q3g. End-to-End Data Platform Architecture

Platform dirancang menggunakan pendekatan:

```text
AWS + Databricks Lakehouse
```

Alur utama:

```text
Sources
   ↓
Ingestion
   ↓
Bronze
   ↓
Processing
   ↓
Silver
   ↓
Gold
   ↓
Consumers
```

Governance, security, dan monitoring berjalan melintasi layer-layer tersebut.

---

## Data Sources

Source utama:

```text
Oracle Core Banking
Kafka Mobile App
Zendesk
Braze
Google Ads
```

Oracle berisi data transactional utama seperti account, customer, dan transaction.

Kafka menerima clickstream event dari mobile application.

Zendesk, Braze, dan Google Ads merupakan SaaS/external source.

---

## Ingestion

### Oracle → AWS DMS

Oracle menggunakan:

```text
AWS DMS
```

untuk:

```text
initial load + CDC
```

CDC digunakan agar pipeline hanya menangkap perubahan baru dan tidak terus melakukan full extraction ke core banking.

---

### Kafka → Databricks Structured Streaming

Clickstream diproses menggunakan:

```text
Databricks Structured Streaming
```

karena workload membutuhkan real-time processing dan platform Databricks juga sudah digunakan untuk processing dan ML.

---

### Zendesk / Braze / Google Ads

SaaS source diproses melalui:

```text
Python API ingestion jobs
```

dan dijalankan secara scheduled menggunakan:

```text
Databricks Workflows
```

---

## Bronze Layer

Technology:

```text
Amazon S3
+
Delta Lake
```

Contoh dataset:

```text
bronze.accounts
bronze.transactions
bronze.app_events
bronze.support_tickets
bronze.marketing
```

Bronze mempertahankan data sedekat mungkin dengan source.

Tujuannya supaya jika transformation bermasalah, data raw masih tersedia untuk reprocessing.

---

## Processing Layer

Processing utama menggunakan:

```text
Databricks
PySpark
Spark SQL
dbt
```

Orchestration menggunakan:

```text
Databricks Workflows
```

Pemilihan Databricks juga konsisten dengan Q1 karena pipeline PySpark dan credit-scoring workload pada scenario sudah menggunakan Databricks.

---

## Silver Layer

Silver menyimpan data yang sudah:

- dibersihkan
- distandardisasi
- dideduplicate
- divalidasi
- melewati data quality check

Contoh:

```text
silver.account_snapshots
silver.transactions_clean
silver.customer_profile
silver.app_events_clean
```

Storage tetap menggunakan:

```text
Amazon S3 + Delta Lake
```

---

## Gold Layer

Gold berisi business-ready dataset seperti:

```text
Customer Health Scorecard
Fraud Alerts
Credit Scoring Features
Regulatory Reporting
```

Transformation ke Gold menggunakan incremental processing sehingga historical data tidak perlu dihitung ulang terus-menerus.

---

## Serving Layer

Gold digunakan oleh tiga consumer utama.

### BI

```text
Gold
 ↓
Databricks SQL Warehouse
 ↓
BI Dashboard
```

### Machine Learning

```text
Gold / Feature Data
 ↓
Databricks ML + MLflow
 ↓
Credit Scoring
```

### Regulatory Reporting

```text
Gold
 ↓
Regulatory Views
 ↓
OJK Reports
```

---

## Governance dan Security

### Unity Catalog

Digunakan untuk:

```text
Data Catalog
Column-Level Lineage
Access Control
Data Ownership
```

### AWS IAM

Digunakan untuk:

```text
Role-Based Access
Least Privilege
```

### AWS KMS

Digunakan untuk encryption at rest.

Untuk data in transit digunakan TLS.

### AWS Secrets Manager

Digunakan untuk credential seperti:

```text
database credential
API credential
```

sehingga credential tidak perlu disimpan di source code.

---

## Monitoring

Monitoring menggunakan:

```text
Databricks Job Logs
AWS CloudWatch
Alerts
```

Job failure dapat diteruskan ke engineer sesuai workflow yang digunakan pada Q1c.

---

## Alasan Tidak Menggunakan Snowflake Sebagai Compute Kedua

Snowflake tetap merupakan pilihan yang valid untuk analytical warehouse.

Namun scenario sudah menggunakan:

```text
Databricks untuk ML
PySpark untuk Q1
Structured Streaming untuk real-time
```

Karena platform memiliki budget sekitar `$50K/month`, saya memilih mengurangi duplicate compute platform.

Satu Databricks Lakehouse digunakan untuk:

```text
batch
streaming
SQL
ML
```

sehingga operational complexity dan cost dapat lebih terkontrol.

Snowflake dapat dipertimbangkan kembali jika nantinya terdapat kebutuhan BI concurrency atau workload isolation yang memang membutuhkan dedicated warehouse.

---

## Cost Control

Beberapa cara untuk menjaga biaya:

- Amazon S3 sebagai primary storage
- incremental processing
- autoscaling Databricks job cluster
- mematikan compute ketika tidak digunakan
- scheduled SQL Warehouse
- menghindari duplicate compute Databricks + Snowflake
- lifecycle management untuk historical event

---

# Q3h. Real-Time Clickstream Architecture

Mobile application dapat menghasilkan sekitar:

```text
50K events/sec pada peak traffic
```

Pipeline real-time dirancang:

```text
Mobile App
    ↓
Kafka
    ↓
Databricks Structured Streaming
    ↓
Bronze App Events
    ↓
Stream Cleaning
    ↓
Silver App Events
    ↓
Stream-Static Join
    ↓
Analytics-Ready Events
```

---

## Kafka Partitioning

Kafka topic menggunakan multiple partitions supaya event bisa diproses secara parallel.

Konsep:

```text
Kafka Topic
 ├── Partition 1 → Worker
 ├── Partition 2 → Worker
 ├── Partition 3 → Worker
 └── ...
```

Saya tidak menentukan jumlah partition secara fixed karena jumlah optimal tergantung:

- event size
- target latency
- throughput per consumer
- cluster sizing

Jumlah partition sebaiknya ditentukan menggunakan load testing.

---

## Databricks Structured Streaming

Streaming job menggunakan:

```text
autoscaling
checkpointing
micro-batch
```

Checkpoint menyimpan progress processing sehingga jika job restart:

```text
job failure
   ↓
restart
   ↓
continue from previous progress
```

dan tidak perlu mengulang seluruh stream.

---

## Bronze App Events

Raw clickstream tetap disimpan di:

```text
Amazon S3 + Delta Lake
```

agar event dapat direprocess jika transformation mengalami masalah.

---

## Stream Cleaning

Sebelum masuk Silver dilakukan:

### Schema Validation

Memastikan field event sesuai contract.

### Deduplication

Menggunakan:

```text
event_id
```

sebagai identifier untuk mencegah duplicate event.

### Watermark

Digunakan untuk menangani late-arriving event.

Event yang datang sedikit terlambat masih dapat diproses selama masih berada dalam watermark yang ditentukan.

### Invalid Event

Invalid event tidak membuat seluruh streaming pipeline gagal.

Event tersebut diarahkan ke:

```text
bronze.app_events_quarantine
```

untuk dianalisis kemudian.

---

## Join dengan Batch Data Q1

Clickstream memiliki:

```text
customer_id
```

yang juga tersedia pada customer/account data.

Karena itu event dapat digabung dengan:

```text
silver.customer_profile
silver.account_snapshots
```

menggunakan:

```text
customer_id
```

Pendekatan ini merupakan:

```text
stream-static join
```

Event real-time mendapatkan context dari batch data terbaru seperti:

```text
customer segment
risk information
account context
```

Contoh hasil analytics-ready event:

```text
event_id
customer_id
event_type
event_timestamp
segment
risk_score
account context
```

---

## Hot / Warm / Cold Storage

### HOT — 0 sampai 7 hari

```text
Delta Lake Optimized
Databricks SQL
```

Digunakan untuk:

- recent dashboard
- funnel analysis
- operational monitoring
- near real-time analytics

---

### WARM — 8 sampai 90 hari

```text
Amazon S3 + Delta Lake
```

Digunakan untuk:

- historical analytics
- trend analysis
- customer behavior analysis

Data tetap queryable tetapi tidak membutuhkan performance setinggi hot data.

---

### COLD — lebih dari 90 hari

```text
Amazon S3 Lifecycle
→ Glacier
```

Digunakan untuk:

- long-term retention
- audit
- archive

Cold data tidak ditujukan untuk interactive query.

Jika dibutuhkan kembali, data dapat direstore terlebih dahulu.

---

## Event Partitioning

Event data dapat dipartition berdasarkan:

```text
event_date
```

Contoh:

```text
event_date=2026-09-20
event_date=2026-09-21
event_date=2026-09-22
```

Saya tidak menggunakan `customer_id` sebagai partition karena jumlah customer sangat besar dan dapat menghasilkan terlalu banyak small partition.

---

# Q3i. Column-Level Lineage dan Auditability

Requirement berikutnya adalah memastikan sebuah metric dapat ditelusuri dari source Oracle sampai digunakan pada BI dashboard.

Contoh lineage:

```text
Oracle
ACCOUNTS.BALANCE
        ↓
AWS DMS / CDC
        ↓
bronze.accounts.balance
        ↓
Q1 PySpark
        ↓
silver.account_snapshots.balance
        ↓
dbt Incremental
        ↓
gold.customer_health_scorecard.total_balance
        ↓
Databricks SQL
        ↓
BI Customer Health Dashboard
```

---

## Unity Catalog

Setelah data masuk ke Databricks, Unity Catalog digunakan untuk membantu:

```text
cataloging
column-level lineage
access control
audit
```

Transformasi dari Bronze ke Silver, Gold, dan serving layer dapat dilacak melalui metadata dari pipeline/query yang berjalan di Databricks.

---

## Oracle ke Bronze Lineage

Oracle dan AWS DMS berada di luar Databricks.

Karena itu mapping source ke Bronze juga dicatat melalui ingestion metadata.

Contoh:

```text
source_system : ORACLE
source_table  : ACCOUNTS
source_column : BALANCE

target_table  : bronze.accounts
target_column : balance
```

Metadata minimal:

```text
source_system
source_table
source_column
target_table
target_column
pipeline_name
run_id
```

Dengan cara ini lineage tidak berhenti di Bronze, tetapi tetap bisa ditelusuri sampai source Oracle.

---

## Pipeline Audit Q1

Pipeline Q1 juga menyimpan operational audit menggunakan konsep:

```text
meta.pipeline_runs
```

Informasi yang dicatat:

```text
run_id
pipeline_name
snapshot_date
source
target
row_count
dq_status
job_status
start_time
end_time
```

Contoh:

```text
run_id        : RUN_20250801_001
pipeline      : account_snapshot
snapshot_date : 2025-08-01
source        : accounts + transactions
target        : silver.account_snapshots
row_count     : 7954
dq_status     : PASS
job_status    : SUCCESS
```

---

## Lineage vs Audit

Keduanya digunakan untuk tujuan berbeda.

```text
Lineage
→ data ini asalnya dari mana?

Audit
→ pipeline mana yang memproses data, kapan dijalankan,
  dan apakah prosesnya berhasil?
```

Kombinasi kedua informasi tersebut membantu kebutuhan auditability dan troubleshooting.

---

# Menjalankan Project Secara Lokal

## 1. Menjalankan Account Snapshot PySpark

Project menggunakan Spark Docker image:

```text
spark:3.5.9-scala2.12-java17-python3-ubuntu
```

Contoh:

```powershell
docker run --rm `
  -v "D:/Insignia-DE-Test:/workspace" `
  --entrypoint bash `
  spark:3.5.9-scala2.12-java17-python3-ubuntu `
  -c "/opt/spark/bin/spark-submit /workspace/src/account_snapshot.py --snapshot-date 2025-08-01 --source local"
```

---

## 2. PostgreSQL

SQL testing menggunakan:

```text
PostgreSQL 16
database: negarabank
```

Setup schema dan table:

```powershell
Get-Content .\sql\setup_postgres.sql -Raw | `
docker exec -i insignia-postgres `
psql -v ON_ERROR_STOP=1 -U insignia -d negarabank
```

---

## 3. Q2d Scorecard

```powershell
Get-Content .\sql\q2d_customer_health_scorecard.sql -Raw | `
docker exec -i insignia-postgres `
psql -v ON_ERROR_STOP=1 -U insignia -d negarabank
```

---

## 4. Generate Synthetic Transaction Location

```powershell
python .\src\generate_transaction_locations.py
```

Synthetic file yang dihasilkan:

```text
data/synthetic/transaction_locations.csv
```

File ini digunakan khusus untuk menguji multi-city fraud rule.

---

## 5. Q2e Fraud Detection

```powershell
Get-Content .\sql\q2e_fraud_detection.sql -Raw | `
docker exec -i insignia-postgres `
psql -v ON_ERROR_STOP=1 -U insignia -d negarabank
```

---

## 6. Q2f Incremental Materialization

Buat target Gold table:

```powershell
Get-Content .\sql\q2f_incremental_health_scorecard.sql -Raw | `
docker exec -i insignia-postgres `
psql -v ON_ERROR_STOP=1 -U insignia -d negarabank
```

Jalankan incremental load:

```powershell
Get-Content .\sql\q2f_incremental_load.sql -Raw | `
docker exec -i insignia-postgres `
psql -v ON_ERROR_STOP=1 -U insignia -d negarabank
```

---

# Testing Summary

| Area | Hasil |
|---|---|
| Spark membaca dataset transaksi | PASS |
| Total transactions sample | 1,991,349 |
| Account snapshot | 7,954 rows |
| Account snapshot DQ | PASS |
| Snapshot rerun / idempotency | PASS |
| Customer scorecard | PASS |
| Total customer | 5,000 |
| Customer dengan account | 4,000 |
| Customer tanpa account | 1,000 |
| High-frequency fraud alert | 25 |
| Multi-city fraud alert | 429 |
| High-amount fraud alert | 1 |
| Total fraud alert | 455 |
| Incremental Gold initial load | 5,000 rows |
| Incremental Gold rerun | tetap 5,000 rows |
| Risk customer | 1,197 |
| Incremental idempotency | PASS |

---

# Assumptions dan Limitations

Beberapa hal pada assessment membutuhkan assumption karena sample dataset tidak memiliki semua data production.

### Historical Account Balance

Dataset hanya menyediakan current account balance.

Karena itu historical balance reconciliation belum dapat diuji secara penuh.

### Merchant Location

Dataset tidak menyediakan merchant location.

Saya membuat synthetic transaction-location data khusus untuk testing rule fraud multi-city.

Synthetic data tersebut bukan representasi actual behavior NegaraBank.

### Monthly Snapshot

Local account snapshot baru diuji untuk satu tanggal sehingga month-over-month balance belum memiliki previous period.

### Production Scale

Local dataset hanya sample dari skenario production.

Karena itu hasil runtime lokal tidak digunakan sebagai estimasi langsung untuk performance production dengan 8M account dan 60M+ transactions.

Production sizing tetap membutuhkan:

```text
load testing
query profiling
cluster sizing
cost monitoring
```

---

# Kesimpulan

Solusi assessment ini dibuat dengan satu alur yang konsisten dari Q1 sampai Q3.

```text
Oracle / Kafka / SaaS
        ↓
Ingestion
        ↓
Bronze
        ↓
Databricks Processing
        ↓
Silver
        ↓
Incremental Gold
        ↓
BI / ML / Regulatory
```

Q1 memperbaiki Account Snapshot agar incremental, aman, memiliki data quality check, dan idempotent.

Q2 membangun Customer Health Scorecard, fraud detection, serta incremental materialization agar reporting tidak selalu melakukan full recomputation.

Q3 membawa pipeline tersebut ke rancangan platform end-to-end yang mendukung batch, streaming, ML, BI, regulatory reporting, governance, lineage, security, dan auditability.

Tujuan utama dari desain ini bukan hanya membuat pipeline berhasil jalan, tetapi membuat prosesnya lebih mudah di-maintain, ditelusuri, di-reprocess, dan dikembangkan ketika volume data bertambah.