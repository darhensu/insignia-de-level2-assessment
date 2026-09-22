# NegaraBank Dataset - Level 2 Assessment

This dataset supports the Level 2 technical assessment for Data Engineer (DE), Data Governance & Quality Analyst (DGQA), and Business Intelligence Analyst (BI) roles.

## Scenario Overview
NegaraBank is an Indonesian digital-first bank with 4.2M customers (sample: 5,000), offering savings, loans, credit cards, QRIS payments, and investments. Operating entirely via mobile app and web with no physical branches. Dataset covers August 2025 - February 2026.

## Files Included

### 1. customers.csv
- **Rows**: 5,000 customers
- **Columns**: customer_id, full_name, nik, phone, email, city, province, registration_date, kyc_status, risk_score, segment
- **KYC Statuses**: VERIFIED, PENDING, REJECTED
- **Segments**: MASS, EMERGING_AFFLUENT, AFFLUENT, HIGH_NET_WORTH
- **Purpose**: Customer master data with KYC compliance tracking

### 2. accounts.csv
- **Rows**: 7,954 accounts
- **Columns**: account_id, customer_id, account_type, product_name, opened_date, status, balance, credit_limit, interest_rate
- **Account Types**: SAVINGS, LOAN, CREDIT_CARD, INVESTMENT
- **Statuses**: ACTIVE, DORMANT, CLOSED, SUSPENDED
- **Products**:
  - SAVINGS: Tabungan Negara Plus, Tabungan Haji, etc.
  - LOAN: KPR Rumah, KTA Personal, etc.
  - CREDIT_CARD: Kartu Platinum, Gold, Classic, Virtual
  - INVESTMENT: Reksa Dana, SBN Retail, Deposito, Obligasi

### 3. transactions.csv
- **Rows**: 1,991,349 transactions (~10K/day)
- **Columns**: txn_id, account_id, txn_date, txn_type, amount, merchant_category, channel, reference_id, status
- **Transaction Types**: DEBIT, CREDIT, TRANSFER_IN, TRANSFER_OUT, PAYMENT, FEE
- **Channels**: MOBILE_APP, WEB, QRIS, ATM_NETWORK, AUTO_DEBIT
- **Statuses**: COMPLETED, PENDING, FAILED, REVERSED
- **Merchant Categories**: Groceries, Restaurants, Transportation, Utilities, Entertainment, Shopping, Healthcare, Education

### 4. credit_scores.csv
- **Rows**: 7,452 credit assessments
- **Columns**: score_id, customer_id, score_date, model_version, credit_score, probability_of_default, features_used
- **Score Range**: 300-850
- **Model Versions**: v2.1, v2.2, v2.3
- **Purpose**: Credit risk assessment and monitoring

### 5. app_events.csv
- **Rows**: 500,000 clickstream events (sample from millions)
- **Columns**: event_id, customer_id, event_timestamp, event_type, screen_name, session_id, device_type, app_version
- **Event Types**: screen_view, button_click, transaction_initiated, login, logout, transfer_start, payment_complete
- **Screens**: Dashboard, Transfer, Payment, Account, Profile, History, Card, Investment
- **Device Types**: ANDROID, IOS, WEB

### 6. support_tickets.csv
- **Rows**: 16,953 support tickets
- **Columns**: ticket_id, customer_id, created_at, resolved_at, category, priority, satisfaction_score, channel
- **Categories**: TRANSACTION, ACCOUNT, CARD, LOAN, GENERAL
- **Priorities**: LOW, MEDIUM, HIGH, URGENT
- **Channels**: IN_APP, EMAIL, CALL, SOCIAL_MEDIA
- **CSAT**: 1-5 scale (nullable if unresolved)

### 7. acquisition_channels.csv
- **Rows**: 5,000 customer acquisition records
- **Columns**: customer_id, channel, acquisition_date, campaign_id
- **Channels**: ORGANIC, REFERRAL, PAID_SOCIAL, PAID_SEARCH, PARTNERSHIP
- **Purpose**: Customer acquisition attribution for cohort analysis

## Key Business Rules

### MRR (Monthly Recurring Revenue) Components
1. **Savings Account Fees**: Rp 10,000/month for accounts with balance < Rp 1M
2. **Credit Card Annual Fees**: Prorated monthly
3. **Loan Interest Income**: Based on interest_rate

### Regulatory Requirements (OJK)
- All ACTIVE accounts must have kyc_status = 'VERIFIED'
- KYC PENDING > 30 days requires escalation
- No transactions allowed for kyc_status = 'REJECTED'
- Credit scores should not jump >100 points between assessments
- Customer risk_score and probability_of_default should be correlated

### Data Quality Checks Required
- Transaction balance reconciliation (CREDIT - DEBIT = account balance change)
- Duplicate detection (same NIK, phone)
- Anomaly detection (unusual transaction patterns, fraud indicators)
- Credit utilization = balance / credit_limit (for credit cards)

## Usage Notes

- All currency values are in Indonesian Rupiah (Rp)
- Dates in YYYY-MM-DD format, timestamps in YYYY-MM-DD HH:MM:S
- NIK (National ID) is 16 digits - must be masked for PII compliance
- Phone numbers should be masked (show last 4 digits only)
- features_used in credit_scores is JSON format

## Assessment Coverage

This dataset is designed to test:
- **DE Level 2**: Pipeline optimization, incremental loading, real-time architecture, column-level lineage
- **DGQA Level 2**: Production DQ monitoring, regulatory reconciliation, governance architecture, SCD Type 2
- **BI Level 2**: Metrics framework, cohort analysis, executive dashboards, self-service analytics

## Intentional Complexity

- High transaction volume for performance testing
- Multiple time-series data (events, transactions, scores) for cohort analysis
- Regulatory compliance scenarios for governance testing
- Real-time + batch data sources for architecture design
- PII data requiring masking strategies

---
Generated: January 2026
Version: 2.0
For OJK Compliance Audit Preparation
