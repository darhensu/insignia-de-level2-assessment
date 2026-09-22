SELECT *
FROM (

    -- rule 1: 5 transaksi atau lebih dalam 1 jam
    SELECT
        fraud_window.customer_id,
        'HIGH_FREQUENCY_1H' AS alert_type,
        fraud_window.txn_date::date AS alert_date,

        jsonb_build_object(
            'max_transactions_in_1h',
            MAX(fraud_window.txn_count_1h),
            'first_trigger_at',
            MIN(fraud_window.txn_date)
        ) AS details_json

    FROM (

        SELECT
            a.customer_id,
            t.txn_id,
            t.txn_date,

            -- hitung jumlah transaksi customer dalam 1 jam terakhir
            COUNT(*) OVER (
                PARTITION BY a.customer_id
                ORDER BY EXTRACT(EPOCH FROM t.txn_date)
                RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
            ) AS txn_count_1h

        FROM bronze.transactions t

        JOIN bronze.accounts a
            ON t.account_id = a.account_id

        WHERE t.status = 'COMPLETED'

    ) fraud_window

    WHERE fraud_window.txn_count_1h >= 5

    GROUP BY
        fraud_window.customer_id,
        fraud_window.txn_date::date


    UNION ALL


    -- rule 2: transaksi di 3 kota atau lebih dalam 1 hari
    SELECT
        a.customer_id,
        'MULTI_CITY_1D' AS alert_type,
        t.txn_date::date AS alert_date,

        jsonb_build_object(
            'city_count',
            COUNT(DISTINCT l.merchant_city),
            'cities',
            ARRAY_AGG(
                DISTINCT l.merchant_city
                ORDER BY l.merchant_city
            )
        ) AS details_json

    FROM bronze.transactions t

    JOIN bronze.accounts a
        ON t.account_id = a.account_id

    -- data lokasi synthetic dipakai untuk kebutuhan testing
    JOIN bronze.transaction_locations l
        ON t.txn_id = l.txn_id

    WHERE t.status = 'COMPLETED'

    GROUP BY
        a.customer_id,
        t.txn_date::date

    HAVING COUNT(DISTINCT l.merchant_city) >= 3


    UNION ALL


    -- rule 3: nominal transaksi lebih dari 3x rata-rata 30 hari sebelumnya
    SELECT
        txn_avg.customer_id,
        'HIGH_AMOUNT_30D' AS alert_type,
        txn_avg.txn_date::date AS alert_date,

        jsonb_build_object(
            'txn_id',
            txn_avg.txn_id,

            'txn_date',
            txn_avg.txn_date,

            'amount',
            ABS(txn_avg.amount),

            'avg_amount_30d',
            ROUND(txn_avg.avg_amount_30d, 2),

            'ratio_to_avg',
            ROUND(
                ABS(txn_avg.amount)
                / NULLIF(txn_avg.avg_amount_30d, 0),
                2
            )
        ) AS details_json

    FROM (

        SELECT
            a.customer_id,
            t.txn_id,
            t.txn_date,
            t.amount,

            -- hitung rata-rata nominal transaksi selama 30 hari sebelumnya
            AVG(ABS(t.amount)) OVER (
                PARTITION BY a.customer_id
                ORDER BY t.txn_date
                RANGE BETWEEN INTERVAL '30 days' PRECEDING
                      AND INTERVAL '1 microsecond' PRECEDING
            ) AS avg_amount_30d

        FROM bronze.transactions t

        JOIN bronze.accounts a
            ON t.account_id = a.account_id

        WHERE t.status = 'COMPLETED'

    ) txn_avg

    WHERE txn_avg.avg_amount_30d IS NOT NULL

    -- tunggu sampai dataset punya history minimal 30 hari
    AND txn_avg.txn_date >= (
        SELECT
            MIN(txn_date) + INTERVAL '30 days'

        FROM bronze.transactions

        WHERE status = 'COMPLETED'
    )

    AND ABS(txn_avg.amount)
        > 3 * txn_avg.avg_amount_30d

) fraud_alerts

ORDER BY
    alert_date,
    customer_id,
    alert_type;