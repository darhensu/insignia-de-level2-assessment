-- bulan yang mau diproses
\set run_month '2025-08-01'


INSERT INTO gold.customer_health_scorecard (

    month_start,
    customer_id,
    total_balance,
    previous_month_balance,
    mom_balance_change,
    mom_balance_change_pct,
    credit_utilization,
    probability_of_default,

    debit_count,
    credit_count,
    transfer_in_count,
    transfer_out_count,
    payment_count,
    fee_count,

    avg_mobile_app_amount,
    avg_web_amount,
    avg_qris_amount,
    avg_atm_network_amount,
    avg_auto_debit_amount,

    risk_flag,
    refreshed_at
)


SELECT
    final_data.month_start,
    final_data.customer_id,
    final_data.total_balance,
    final_data.previous_month_balance,

    final_data.total_balance
        - final_data.previous_month_balance
        AS mom_balance_change,

    ROUND(
        (
            final_data.total_balance
            - final_data.previous_month_balance
        )
        / NULLIF(
            ABS(final_data.previous_month_balance),
            0
        ),
        6
    ) AS mom_balance_change_pct,

    final_data.credit_utilization,
    final_data.probability_of_default,

    final_data.debit_count,
    final_data.credit_count,
    final_data.transfer_in_count,
    final_data.transfer_out_count,
    final_data.payment_count,
    final_data.fee_count,

    final_data.avg_mobile_app_amount,
    final_data.avg_web_amount,
    final_data.avg_qris_amount,
    final_data.avg_atm_network_amount,
    final_data.avg_auto_debit_amount,

    CASE
        WHEN final_data.credit_utilization > 0.80
          OR COALESCE(
                final_data.probability_of_default,
                0
             ) > 0.30

          OR (
                (
                    final_data.total_balance
                    - final_data.previous_month_balance
                )
                / NULLIF(
                    ABS(final_data.previous_month_balance),
                    0
                )
             ) < -0.30

        THEN TRUE
        ELSE FALSE
    END AS risk_flag,

    CURRENT_TIMESTAMP AS refreshed_at


FROM (

    SELECT
        monthly_data.*,

        -- ambil saldo customer pada bulan sebelumnya
        LAG(monthly_data.total_balance) OVER (
            PARTITION BY monthly_data.customer_id
            ORDER BY monthly_data.month_start
        ) AS previous_month_balance

    FROM (

        SELECT
            customer_month.month_start,
            customer_month.customer_id,

            COALESCE(
                balance_data.total_balance,
                0
            ) AS total_balance,

            COALESCE(
                balance_data.credit_utilization,
                0
            ) AS credit_utilization,

            credit_data.probability_of_default,

            COALESCE(txn_data.debit_count, 0)
                AS debit_count,

            COALESCE(txn_data.credit_count, 0)
                AS credit_count,

            COALESCE(txn_data.transfer_in_count, 0)
                AS transfer_in_count,

            COALESCE(txn_data.transfer_out_count, 0)
                AS transfer_out_count,

            COALESCE(txn_data.payment_count, 0)
                AS payment_count,

            COALESCE(txn_data.fee_count, 0)
                AS fee_count,

            txn_data.avg_mobile_app_amount,
            txn_data.avg_web_amount,
            txn_data.avg_qris_amount,
            txn_data.avg_atm_network_amount,
            txn_data.avg_auto_debit_amount


        FROM (

            -- hanya proses run month dan bulan sebelumnya
            SELECT
                report_month.month_start,
                c.customer_id

            FROM bronze.customers c

            CROSS JOIN (

                SELECT DISTINCT
                    DATE_TRUNC(
                        'month',
                        snapshot_date
                    )::date AS month_start

                FROM silver.account_snapshots

                WHERE DATE_TRUNC(
                    'month',
                    snapshot_date
                )::date BETWEEN
                    DATE :'run_month' - INTERVAL '1 month'
                    AND DATE :'run_month'

            ) report_month

        ) customer_month


        LEFT JOIN (

            -- ambil snapshot terakhir setiap account per bulan
            SELECT
                account_month.month_start,
                account_month.customer_id,

                SUM(
                    account_month.balance
                ) AS total_balance,

                CASE
                    WHEN SUM(
                        CASE
                            WHEN account_month.account_type = 'CREDIT_CARD'
                            THEN account_month.credit_limit
                            ELSE 0
                        END
                    ) = 0
                    THEN 0

                    ELSE
                        SUM(
                            CASE
                                WHEN account_month.account_type = 'CREDIT_CARD'
                                THEN account_month.balance
                                ELSE 0
                            END
                        )
                        /
                        SUM(
                            CASE
                                WHEN account_month.account_type = 'CREDIT_CARD'
                                THEN account_month.credit_limit
                                ELSE 0
                            END
                        )
                END AS credit_utilization

            FROM (

                SELECT
                    DATE_TRUNC(
                        'month',
                        s.snapshot_date
                    )::date AS month_start,

                    s.customer_id,
                    s.account_id,
                    s.account_type,
                    s.balance,
                    s.credit_limit,

                    ROW_NUMBER() OVER (
                        PARTITION BY
                            s.account_id,
                            DATE_TRUNC(
                                'month',
                                s.snapshot_date
                            )

                        ORDER BY
                            s.snapshot_date DESC
                    ) AS rn

                FROM silver.account_snapshots s

                WHERE s.snapshot_date >=
                    DATE :'run_month'
                    - INTERVAL '1 month'

                  AND s.snapshot_date <
                    DATE :'run_month'
                    + INTERVAL '1 month'

            ) account_month

            WHERE account_month.rn = 1

            GROUP BY
                account_month.month_start,
                account_month.customer_id

        ) balance_data

            ON customer_month.customer_id
                = balance_data.customer_id

            AND customer_month.month_start
                = balance_data.month_start


        LEFT JOIN (

            -- agregasi transaksi per customer dan bulan
            SELECT
                DATE_TRUNC(
                    'month',
                    t.txn_date
                )::date AS month_start,

                a.customer_id,

                COUNT(*) FILTER (
                    WHERE t.txn_type = 'DEBIT'
                ) AS debit_count,

                COUNT(*) FILTER (
                    WHERE t.txn_type = 'CREDIT'
                ) AS credit_count,

                COUNT(*) FILTER (
                    WHERE t.txn_type = 'TRANSFER_IN'
                ) AS transfer_in_count,

                COUNT(*) FILTER (
                    WHERE t.txn_type = 'TRANSFER_OUT'
                ) AS transfer_out_count,

                COUNT(*) FILTER (
                    WHERE t.txn_type = 'PAYMENT'
                ) AS payment_count,

                COUNT(*) FILTER (
                    WHERE t.txn_type = 'FEE'
                ) AS fee_count,


                ROUND(
                    AVG(ABS(t.amount)) FILTER (
                        WHERE t.channel = 'MOBILE_APP'
                    ),
                    2
                ) AS avg_mobile_app_amount,

                ROUND(
                    AVG(ABS(t.amount)) FILTER (
                        WHERE t.channel = 'WEB'
                    ),
                    2
                ) AS avg_web_amount,

                ROUND(
                    AVG(ABS(t.amount)) FILTER (
                        WHERE t.channel = 'QRIS'
                    ),
                    2
                ) AS avg_qris_amount,

                ROUND(
                    AVG(ABS(t.amount)) FILTER (
                        WHERE t.channel = 'ATM_NETWORK'
                    ),
                    2
                ) AS avg_atm_network_amount,

                ROUND(
                    AVG(ABS(t.amount)) FILTER (
                        WHERE t.channel = 'AUTO_DEBIT'
                    ),
                    2
                ) AS avg_auto_debit_amount


            FROM bronze.transactions t

            JOIN bronze.accounts a
                ON t.account_id = a.account_id

            WHERE t.status = 'COMPLETED'

              AND t.txn_date >=
                DATE :'run_month'
                - INTERVAL '1 month'

              AND t.txn_date <
                DATE :'run_month'
                + INTERVAL '1 month'

            GROUP BY
                DATE_TRUNC(
                    'month',
                    t.txn_date
                )::date,

                a.customer_id

        ) txn_data

            ON customer_month.customer_id
                = txn_data.customer_id

            AND customer_month.month_start
                = txn_data.month_start


        LEFT JOIN (

            -- ambil credit score terakhir yang tersedia pada bulan tersebut
            SELECT
                score_data.month_start,
                score_data.customer_id,
                score_data.probability_of_default

            FROM (

                SELECT
                    report_month.month_start,
                    cs.customer_id,
                    cs.probability_of_default,

                    ROW_NUMBER() OVER (
                        PARTITION BY
                            report_month.month_start,
                            cs.customer_id

                        ORDER BY
                            cs.score_date DESC
                    ) AS rn

                FROM bronze.credit_scores cs

                JOIN (

                    SELECT DISTINCT
                        DATE_TRUNC(
                            'month',
                            snapshot_date
                        )::date AS month_start

                    FROM silver.account_snapshots

                    WHERE DATE_TRUNC(
                        'month',
                        snapshot_date
                    )::date BETWEEN
                        DATE :'run_month'
                        - INTERVAL '1 month'

                        AND DATE :'run_month'

                ) report_month

                    ON cs.score_date
                        < report_month.month_start
                          + INTERVAL '1 month'

            ) score_data

            WHERE score_data.rn = 1

        ) credit_data

            ON customer_month.customer_id
                = credit_data.customer_id

            AND customer_month.month_start
                = credit_data.month_start

    ) monthly_data

) final_data


-- yang dimasukkan cuma bulan yang sedang diproses
WHERE final_data.month_start
    = DATE :'run_month'


-- kalau row sudah ada, update
ON CONFLICT (
    month_start,
    customer_id
)

DO UPDATE SET

    total_balance =
        EXCLUDED.total_balance,

    previous_month_balance =
        EXCLUDED.previous_month_balance,

    mom_balance_change =
        EXCLUDED.mom_balance_change,

    mom_balance_change_pct =
        EXCLUDED.mom_balance_change_pct,

    credit_utilization =
        EXCLUDED.credit_utilization,

    probability_of_default =
        EXCLUDED.probability_of_default,

    debit_count =
        EXCLUDED.debit_count,

    credit_count =
        EXCLUDED.credit_count,

    transfer_in_count =
        EXCLUDED.transfer_in_count,

    transfer_out_count =
        EXCLUDED.transfer_out_count,

    payment_count =
        EXCLUDED.payment_count,

    fee_count =
        EXCLUDED.fee_count,

    avg_mobile_app_amount =
        EXCLUDED.avg_mobile_app_amount,

    avg_web_amount =
        EXCLUDED.avg_web_amount,

    avg_qris_amount =
        EXCLUDED.avg_qris_amount,

    avg_atm_network_amount =
        EXCLUDED.avg_atm_network_amount,

    avg_auto_debit_amount =
        EXCLUDED.avg_auto_debit_amount,

    risk_flag =
        EXCLUDED.risk_flag,

    refreshed_at =
        CURRENT_TIMESTAMP;