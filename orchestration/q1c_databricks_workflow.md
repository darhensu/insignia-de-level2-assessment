# Q1c - Databricks Workflow

Saya memilih Databricks Workflows karena pipeline account snapshot pada Q1b menggunakan PySpark dan pada skenario NegaraBank credit scoring juga berjalan di Databricks.

Flow yang digunakan:

Account Snapshot
↓
Data Quality PASS
↓
Credit Scoring
↓
Selesai

Jika Account Snapshot gagal:
↓
Retry
↓
Maksimal 3 percobaan
↓
Jika tetap gagal, kirim alert ke engineer


## Task 1 - Account Snapshot

Task pertama menjalankan:

`src/account_snapshot.py`

Parameter yang diberikan:

`--snapshot-date <tanggal proses>`

Task ini melakukan incremental load, membuat snapshot, menjalankan data quality check, dan menyimpan output berdasarkan `snapshot_date`.

Jika data quality check gagal, script akan menghasilkan error sehingga workflow tidak melanjutkan ke Credit Scoring.


## Task 2 - Credit Scoring

Task Credit Scoring hanya dijalankan jika task Account Snapshot berhasil.

Dependency:

`account_snapshot -> credit_scoring`

Dengan dependency ini, credit scoring tidak akan menggunakan snapshot yang belum selesai atau gagal dalam data quality check.


## Retry dan Alert

Untuk Account Snapshot:

- maksimal 3 kali percobaan
- terdapat jeda antar retry
- jika seluruh percobaan gagal, status job menjadi FAILED
- engineer menerima alert melalui email atau notification channel yang sudah dikonfigurasi di Databricks

Credit Scoring juga dapat menggunakan retry policy yang sama untuk menangani temporary failure.


## SLA

Credit scoring harus mulai maksimal 30 menit setelah account snapshot tersedia.

Karena menggunakan dependency langsung pada workflow yang sama, task Credit Scoring dapat mulai segera setelah Account Snapshot berhasil tanpa perlu menunggu schedule berikutnya.




+----------------------+
|   Account Snapshot   |
|      PySpark         |
+----------+-----------+
           |
           | success
           v
+----------------------+
|    Credit Scoring    |
|      Databricks      |
+----------+-----------+
           |
           v
        selesai


jika gagal:

Account Snapshot
      |
      v
   retry 1
      |
      v
   retry 2
      |
      v
attempt ke-3 gagal
      |
      v
 alert engineer