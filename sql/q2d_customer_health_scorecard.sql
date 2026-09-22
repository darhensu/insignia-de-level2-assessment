SELECT
    final_data.month_start,
    final_data.customer_id,
    final_data.total_balance,
    final_data.previous_month_balance,

    -- perubahan balance dibanding bulan sebelumnya
    final_data.total_balance - final_data.previous_month_balance
        AS mom_balance_change,

    ROUND(
        (
            final_data.total_balance
            - final_data.previous_month_balance
        ) / NULLIF(
            ABS(final_data.previous_month_balance),
            0
        ),
        4
    ) AS mom_balance_change_pct,

    ROUND(
        final_data.credit_utilization,
        4
    ) AS credit_utilization,

    final_data.probability_of_default,

    final_data.debit_count,
    final_data.credit_count,
    final_data.transfer_in_count,
    final_data.transfer_out_count,
    final_data.payment_count,
    final_data.fee_count,

    ROUND(final_data.avg_mobile_app_amount, 2)
        AS avg_mobile_app_amount,

    ROUND(final_data.avg_web_amount, 2)
        AS avg_web_amount,

    ROUND(final_data.avg_qris_amount, 2)
        AS avg_qris_amount,

    ROUND(final_data.avg_atm_network_amount, 2)
        AS avg_atm_network_amount,

    ROUND(final_data.avg_auto_debit_amount, 2)
        AS avg_auto_debit_amount,


    -- risk kalau salah satu kondisi terpenuhi
    CASE
        WHEN final_data.credit_utilization > 0.80

            OR COALESCE(
                final_data.probability_of_default,
                0
            ) > 0.30

            OR COALESCE(
                (
                    final_data.total_balance
                    - final_data.previous_month_balance
                )
                / NULLIF(
                    ABS(final_data.previous_month_balance),
                    0
                ),
                0
            ) < -0.30

        THEN TRUE
        ELSE FALSE
    END AS risk_flag

FROM (

    SELECT
        monthly_data.*,

        -- ambil balance customer dari bulan sebelumnya
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


            -- hitung pemakaian limit credit card
            CASE
                WHEN COALESCE(
                    balance_data.total_credit_limit,
                    0
                ) > 0

                THEN
                    balance_data.credit_card_balance
                    / balance_data.total_credit_limit

                ELSE 0
            END AS credit_utilization,


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
            txn_data.avg_auto_debit_amount,

            credit_data.probability_of_default

        FROM (

            -- bikin kombinasi customer dan bulan laporan
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

            ) report_month

        ) customer_month


        LEFT JOIN (

            -- total balance dari snapshot terakhir tiap account dalam bulan tersebut
            SELECT
                account_month.month_start,
                account_month.customer_id,

                SUM(account_month.balance)
                    AS total_balance,

                SUM(
                    CASE
                        WHEN account_month.account_type = 'CREDIT_CARD'
                        THEN account_month.balance
                        ELSE 0
                    END
                ) AS credit_card_balance,

                SUM(
                    CASE
                        WHEN account_month.account_type = 'CREDIT_CARD'
                        THEN COALESCE(
                            account_month.credit_limit,
                            0
                        )
                        ELSE 0
                    END
                ) AS total_credit_limit

            FROM (

                -- pilih snapshot terakhir per account dan bulan
                SELECT
                    DATE_TRUNC(
                        'month',
                        snapshot_date
                    )::date AS month_start,

                    customer_id,
                    account_id,
                    account_type,
                    balance,
                    credit_limit,

                    ROW_NUMBER() OVER (
                        PARTITION BY
                            account_id,
                            DATE_TRUNC(
                                'month',
                                snapshot_date
                            )
                        ORDER BY snapshot_date DESC
                    ) AS rn

                FROM silver.account_snapshots

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

            -- ringkas transaksi per customer dan bulan
            SELECT
                DATE_TRUNC(
                    'month',
                    t.txn_date
                )::date AS month_start,

                a.customer_id,

                SUM(
                    CASE
                        WHEN t.txn_type = 'DEBIT'
                        THEN 1 ELSE 0
                    END
                ) AS debit_count,

                SUM(
                    CASE
                        WHEN t.txn_type = 'CREDIT'
                        THEN 1 ELSE 0
                    END
                ) AS credit_count,

                SUM(
                    CASE
                        WHEN t.txn_type = 'TRANSFER_IN'
                        THEN 1 ELSE 0
                    END
                ) AS transfer_in_count,

                SUM(
                    CASE
                        WHEN t.txn_type = 'TRANSFER_OUT'
                        THEN 1 ELSE 0
                    END
                ) AS transfer_out_count,

                SUM(
                    CASE
                        WHEN t.txn_type = 'PAYMENT'
                        THEN 1 ELSE 0
                    END
                ) AS payment_count,

                SUM(
                    CASE
                        WHEN t.txn_type = 'FEE'
                        THEN 1 ELSE 0
                    END
                ) AS fee_count,


                -- pakai abs supaya yang dihitung besar nominal transaksi
                AVG(
                    CASE
                        WHEN t.channel = 'MOBILE_APP'
                        THEN ABS(t.amount)
                    END
                ) AS avg_mobile_app_amount,

                AVG(
                    CASE
                        WHEN t.channel = 'WEB'
                        THEN ABS(t.amount)
                    END
                ) AS avg_web_amount,

                AVG(
                    CASE
                        WHEN t.channel = 'QRIS'
                        THEN ABS(t.amount)
                    END
                ) AS avg_qris_amount,

                AVG(
                    CASE
                        WHEN t.channel = 'ATM_NETWORK'
                        THEN ABS(t.amount)
                    END
                ) AS avg_atm_network_amount,

                AVG(
                    CASE
                        WHEN t.channel = 'AUTO_DEBIT'
                        THEN ABS(t.amount)
                    END
                ) AS avg_auto_debit_amount

            FROM bronze.transactions t

            JOIN bronze.accounts a
                ON t.account_id = a.account_id

            WHERE t.status = 'COMPLETED'

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

            -- ambil credit score terbaru yang sudah tersedia sampai akhir bulan
            SELECT
                score_month.month_start,
                score_month.customer_id,
                score_month.probability_of_default

            FROM (

                SELECT
                    report_month.month_start,
                    cs.customer_id,
                    cs.probability_of_default,

                    ROW_NUMBER() OVER (
                        PARTITION BY
                            report_month.month_start,
                            cs.customer_id

                        ORDER BY cs.score_date DESC
                    ) AS rn

                FROM bronze.credit_scores cs

                JOIN (

                    SELECT DISTINCT
                        DATE_TRUNC(
                            'month',
                            snapshot_date
                        )::date AS month_start

                    FROM silver.account_snapshots

                ) report_month

                    ON cs.score_date
                       < report_month.month_start
                         + INTERVAL '1 month'

            ) score_month

            WHERE score_month.rn = 1

        ) credit_data

            ON customer_month.customer_id
                = credit_data.customer_id

            AND customer_month.month_start
                = credit_data.month_start

    ) monthly_data

) final_data

ORDER BY
    final_data.month_start,
    final_data.customer_id;