-- target table untuk monthly customer health scorecard
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.customer_health_scorecard (
    month_start                DATE NOT NULL,
    customer_id                VARCHAR(15) NOT NULL,

    total_balance              NUMERIC(20,2),
    previous_month_balance     NUMERIC(20,2),
    mom_balance_change         NUMERIC(20,2),
    mom_balance_change_pct     NUMERIC(18,6),

    credit_utilization         NUMERIC(18,6),
    probability_of_default     NUMERIC(5,4),

    debit_count                BIGINT,
    credit_count               BIGINT,
    transfer_in_count          BIGINT,
    transfer_out_count         BIGINT,
    payment_count              BIGINT,
    fee_count                  BIGINT,

    avg_mobile_app_amount      NUMERIC(20,2),
    avg_web_amount             NUMERIC(20,2),
    avg_qris_amount            NUMERIC(20,2),
    avg_atm_network_amount     NUMERIC(20,2),
    avg_auto_debit_amount      NUMERIC(20,2),

    risk_flag                  BOOLEAN,

    refreshed_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (
        month_start,
        customer_id
    )
);


-- bantu query history per customer
CREATE INDEX IF NOT EXISTS idx_health_customer_month
ON gold.customer_health_scorecard (
    customer_id,
    month_start
);