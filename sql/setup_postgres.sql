-- schema source/raw untuk kebutuhan testing
CREATE SCHEMA IF NOT EXISTS bronze;

-- schema hasil proses pipeline
CREATE SCHEMA IF NOT EXISTS silver;


-- master account
DROP TABLE IF EXISTS bronze.accounts;

CREATE TABLE bronze.accounts (
    account_id      VARCHAR(20),
    customer_id     VARCHAR(15),
    account_type    VARCHAR(20),
    product_name    VARCHAR(50),
    opened_date     DATE,
    status          VARCHAR(15),
    balance         NUMERIC(15,2),
    credit_limit    NUMERIC(15,2),
    interest_rate   NUMERIC(5,4)
);


-- transaksi
DROP TABLE IF EXISTS bronze.transactions;

CREATE TABLE bronze.transactions (
    txn_id              VARCHAR(25),
    account_id          VARCHAR(20),
    txn_date            TIMESTAMP,
    txn_type            VARCHAR(20),
    amount              NUMERIC(15,2),
    merchant_category   VARCHAR(30),
    channel             VARCHAR(20),
    reference_id        VARCHAR(30),
    status              VARCHAR(15)
);


-- data credit score
DROP TABLE IF EXISTS bronze.credit_scores;

CREATE TABLE bronze.credit_scores (
    score_id                    VARCHAR(20),
    customer_id                 VARCHAR(15),
    score_date                  DATE,
    model_version               VARCHAR(10),
    credit_score                INTEGER,
    probability_of_default      NUMERIC(5,4),
    features_used               JSONB
);


-- table ini mewakili output account snapshot dari q1
DROP TABLE IF EXISTS silver.account_snapshots;

CREATE TABLE silver.account_snapshots (
    snapshot_date       DATE,
    account_id          VARCHAR(20),
    customer_id         VARCHAR(15),
    account_type        VARCHAR(20),
    product_name        VARCHAR(50),
    opened_date         DATE,
    status              VARCHAR(15),
    balance             NUMERIC(15,2),
    credit_limit        NUMERIC(15,2),
    interest_rate       NUMERIC(5,4),
    daily_net_amount    NUMERIC(15,2),
    daily_txn_count     BIGINT
);