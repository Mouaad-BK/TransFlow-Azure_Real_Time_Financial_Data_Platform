-- =========================================================
-- FACT TRANSACTIONS
-- =========================================================

-- Create the fact_transactions view from the Gold Delta table
CREATE VIEW dbo.fact_transactions
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://strtransactions.dfs.core.windows.net/transactions/gold/fact_transactions/',
    FORMAT = 'DELTA'
) AS fact;


-- =========================================================
-- DIMENSIONS
-- =========================================================

-- Create the customer dimension from fact_transactions
CREATE VIEW dbo.dim_customer
AS
SELECT DISTINCT
    customer_id,
    age,
    gender,
    annual_income,
    credit_score,
    number_of_cards
FROM dbo.fact_transactions;


-- Create the card dimension from fact_transactions
CREATE VIEW dbo.dim_card
AS
SELECT DISTINCT
    card_id,
    card_brand,
    card_type,
    has_chip,
    card_limit
FROM dbo.fact_transactions;


-- Create the merchant dimension from fact_transactions
CREATE VIEW dbo.dim_merchant
AS
SELECT DISTINCT
    merchant_id,
    merchant_city,
    merchant_state,
    postal_code,
    merchant_category_code
FROM dbo.fact_transactions;


-- =========================================================
-- GOLD SUMMARY VIEWS
-- =========================================================

-- Card Summary
CREATE VIEW dbo.card_summary
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://strtransactions.dfs.core.windows.net/transactions/gold/card_summary/',
    FORMAT = 'DELTA'
) AS data;


-- Card Usage Method Summary
CREATE VIEW dbo.card_usage_method_summary
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://strtransactions.dfs.core.windows.net/transactions/gold/card_usage_method_summary/',
    FORMAT = 'DELTA'
) AS data;


-- Daily Amount Summary
CREATE VIEW dbo.daily_amount_summary
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://strtransactions.dfs.core.windows.net/transactions/gold/daily_amount_summary/',
    FORMAT = 'DELTA'
) AS data;


-- Daily Status Summary
CREATE VIEW dbo.daily_status_summary
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://strtransactions.dfs.core.windows.net/transactions/gold/daily_status_summary/',
    FORMAT = 'DELTA'
) AS data;


-- Geographic Summary
CREATE VIEW dbo.geographic_summary
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://strtransactions.dfs.core.windows.net/transactions/gold/geographic_summary/',
    FORMAT = 'DELTA'
) AS data;


-- Merchant Category Summary
CREATE VIEW dbo.merchant_category_summary
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://strtransactions.dfs.core.windows.net/transactions/gold/merchant_category_summary/',
    FORMAT = 'DELTA'
) AS data;


-- Monthly Amount Summary
CREATE VIEW dbo.monthly_amount_summary
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://strtransactions.dfs.core.windows.net/transactions/gold/monthly_amount_summary/',
    FORMAT = 'DELTA'
) AS data;


-- Monthly Status Summary
CREATE VIEW dbo.monthly_status_summary
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://strtransactions.dfs.core.windows.net/transactions/gold/monthly_status_summary/',
    FORMAT = 'DELTA'
) AS data;


-- Transaction Error Summary
CREATE VIEW dbo.transaction_error_summary
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://strtransactions.dfs.core.windows.net/transactions/gold/transaction_error_summary/',
    FORMAT = 'DELTA'
) AS data;