-- ===================================================
-- 01_database_setup.sql
-- Database Schema and Table Creation
-- Fintech Payment Analytics Project
-- ===================================================
-- 
-- Purpose: Complete database setup for fintech fraud detection analysis
-- Author: Gokhan Fidan
-- Dataset: Kaggle Credit Card Fraud Detection (284,807 transactions)
--
-- Execution Order: RUN FIRST
-- Dependencies: None
-- Prerequisites: PostgreSQL 12+, creditcard.csv in /data folder
-- ===================================================

-- ===== 1. DATABASE AND SCHEMA CREATION =====

-- Create main database
CREATE DATABASE fintech_analytics;

-- Connect to fintech_analytics database before running below commands

-- Create schemas for organized data management
CREATE SCHEMA IF NOT EXISTS analytics;        -- Core business data
CREATE SCHEMA IF NOT EXISTS fraud_detection;  -- Fraud analysis results  
CREATE SCHEMA IF NOT EXISTS staging;          -- Data import staging area

-- Verify schemas created
SELECT schema_name FROM information_schema.schemata 
WHERE schema_name IN ('analytics', 'fraud_detection', 'staging');


-- ===== 2. CORE BUSINESS TABLES =====

-- Main transactions table (analytics schema)
CREATE TABLE analytics.transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INTEGER, -- Will be populated later through analysis
    amount DECIMAL(10,2) NOT NULL,
    transaction_time INTEGER NOT NULL, -- Seconds from dataset start
    transaction_timestamp TIMESTAMP, -- Converted timestamp format
    -- PCA-transformed features from Kaggle dataset
    v1 DECIMAL(10,6), v2 DECIMAL(10,6), v3 DECIMAL(10,6), v4 DECIMAL(10,6),
    v5 DECIMAL(10,6), v6 DECIMAL(10,6), v7 DECIMAL(10,6), v8 DECIMAL(10,6),
    v9 DECIMAL(10,6), v10 DECIMAL(10,6), v11 DECIMAL(10,6), v12 DECIMAL(10,6),
    v13 DECIMAL(10,6), v14 DECIMAL(10,6), v15 DECIMAL(10,6), v16 DECIMAL(10,6),
    v17 DECIMAL(10,6), v18 DECIMAL(10,6), v19 DECIMAL(10,6), v20 DECIMAL(10,6),
    v21 DECIMAL(10,6), v22 DECIMAL(10,6), v23 DECIMAL(10,6), v24 DECIMAL(10,6),
    v25 DECIMAL(10,6), v26 DECIMAL(10,6), v27 DECIMAL(10,6), v28 DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer analytics table (future enhancements)
CREATE TABLE analytics.customers (
    customer_id SERIAL PRIMARY KEY,
    created_date DATE DEFAULT CURRENT_DATE,
    total_transactions INTEGER DEFAULT 0,
    total_amount DECIMAL(12,2) DEFAULT 0.00,
    avg_transaction_amount DECIMAL(10,2) DEFAULT 0.00,
    risk_score INTEGER DEFAULT 50, -- 0-100 risk assessment
    last_transaction_date TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ===== 3. FRAUD DETECTION TABLES =====

-- Fraud case classification and tracking
CREATE TABLE fraud_detection.fraud_cases (
    case_id SERIAL PRIMARY KEY,
    transaction_id INTEGER REFERENCES analytics.transactions(transaction_id),
    customer_id INTEGER REFERENCES analytics.customers(customer_id),
    is_fraud BOOLEAN NOT NULL, -- True/False from Kaggle dataset
    detection_method VARCHAR(100), -- Method used to detect fraud
    fraud_probability DECIMAL(5,4), -- 0.0000-1.0000 confidence score
    investigation_status VARCHAR(50) DEFAULT 'pending', -- pending, confirmed, false_positive
    reported_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_date TIMESTAMP,
    financial_loss DECIMAL(10,2) DEFAULT 0.00
);


-- ===== 4. DATA STAGING AREA =====

-- Staging table for CSV import (matches Kaggle dataset structure exactly)
CREATE TABLE staging.raw_creditcard (
    time_seconds INTEGER,
    v1 DECIMAL(15,10), v2 DECIMAL(15,10), v3 DECIMAL(15,10), v4 DECIMAL(15,10),
    v5 DECIMAL(15,10), v6 DECIMAL(15,10), v7 DECIMAL(15,10), v8 DECIMAL(15,10),
    v9 DECIMAL(15,10), v10 DECIMAL(15,10), v11 DECIMAL(15,10), v12 DECIMAL(15,10),
    v13 DECIMAL(15,10), v14 DECIMAL(15,10), v15 DECIMAL(15,10), v16 DECIMAL(15,10),
    v17 DECIMAL(15,10), v18 DECIMAL(15,10), v19 DECIMAL(15,10), v20 DECIMAL(15,10),
    v21 DECIMAL(15,10), v22 DECIMAL(15,10), v23 DECIMAL(15,10), v24 DECIMAL(15,10),
    v25 DECIMAL(15,10), v26 DECIMAL(15,10), v27 DECIMAL(15,10), v28 DECIMAL(15,10),
    amount DECIMAL(10,2),
    class INTEGER -- 0=normal, 1=fraud
);


-- ===== 5. PERFORMANCE OPTIMIZATION =====

-- Strategic indexes for query performance
CREATE INDEX idx_transactions_amount ON analytics.transactions(amount);
CREATE INDEX idx_transactions_time ON analytics.transactions(transaction_time);
CREATE INDEX idx_transactions_v3 ON analytics.transactions(v3); -- Key fraud indicator
CREATE INDEX idx_fraud_cases_is_fraud ON fraud_detection.fraud_cases(is_fraud);
CREATE INDEX idx_fraud_cases_transaction_id ON fraud_detection.fraud_cases(transaction_id);

-- Composite indexes for complex queries
CREATE INDEX idx_transactions_amount_time ON analytics.transactions(amount, transaction_time);
CREATE INDEX idx_fraud_detection_lookup ON fraud_detection.fraud_cases(transaction_id, is_fraud);


-- ===== 6. DATA IMPORT INSTRUCTIONS =====

/*
DATA IMPORT PROCESS:
===================

1. Import creditcard.csv into staging.raw_creditcard using DBeaver:
   - Right-click on staging.raw_creditcard → Import Data
   - Select CSV file from /data/creditcard.csv
   - Ensure header mapping: Time→time_seconds, Amount→amount, Class→class
   - Verify 284,807 rows imported successfully

2. Transform and load data into main tables:
   - Execute data transformation queries in section 7 below
   - Validate data integrity and counts

3. Verify setup:
   - staging.raw_creditcard: 284,807 rows
   - analytics.transactions: 284,807 rows  
   - fraud_detection.fraud_cases: 284,807 rows
*/


-- ===== 7. DATA TRANSFORMATION QUERIES =====

-- A) Load transactions from staging to analytics
INSERT INTO analytics.transactions (
    customer_id, amount, transaction_time, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10,
    v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21, v22, v23, v24, v25, v26, v27, v28
)
SELECT 
    NULL as customer_id, -- Will be populated through customer analysis
    amount, time_seconds,
    ROUND(v1, 6), ROUND(v2, 6), ROUND(v3, 6), ROUND(v4, 6), ROUND(v5, 6),
    ROUND(v6, 6), ROUND(v7, 6), ROUND(v8, 6), ROUND(v9, 6), ROUND(v10, 6),
    ROUND(v11, 6), ROUND(v12, 6), ROUND(v13, 6), ROUND(v14, 6), ROUND(v15, 6),
    ROUND(v16, 6), ROUND(v17, 6), ROUND(v18, 6), ROUND(v19, 6), ROUND(v20, 6),
    ROUND(v21, 6), ROUND(v22, 6), ROUND(v23, 6), ROUND(v24, 6), ROUND(v25, 6),
    ROUND(v26, 6), ROUND(v27, 6), ROUND(v28, 6)
FROM staging.raw_creditcard
ORDER BY time_seconds;

-- B) Load fraud cases with precision matching
INSERT INTO fraud_detection.fraud_cases (transaction_id, customer_id, is_fraud, detection_method, fraud_probability)
SELECT DISTINCT
     t.transaction_id,
     NULL::INTEGER as customer_id,
     CASE WHEN s.class = 1 THEN TRUE ELSE FALSE END as is_fraud,
     'kaggle_labeled' as detection_method,
     CASE WHEN s.class = 1 THEN 0.95 ELSE 0.05 END as fraud_probability
FROM analytics.transactions t 
JOIN staging.raw_creditcard s ON(
    t.amount = s.amount
    AND t.transaction_time = s.time_seconds
    AND t.v1 = ROUND(s.v1, 6) AND t.v2 = ROUND(s.v2, 6) AND t.v3 = ROUND(s.v3, 6) 
    AND t.v4 = ROUND(s.v4, 6) AND t.v5 = ROUND(s.v5, 6) AND t.v6 = ROUND(s.v6, 6) 
    AND t.v7 = ROUND(s.v7, 6) AND t.v8 = ROUND(s.v8, 6) AND t.v9 = ROUND(s.v9, 6) 
    AND t.v10 = ROUND(s.v10, 6) AND t.v11 = ROUND(s.v11, 6) AND t.v12 = ROUND(s.v12, 6)
    AND t.v13 = ROUND(s.v13, 6) AND t.v14 = ROUND(s.v14, 6) AND t.v15 = ROUND(s.v15, 6)
    AND t.v16 = ROUND(s.v16, 6) AND t.v17 = ROUND(s.v17, 6) AND t.v18 = ROUND(s.v18, 6)
    AND t.v19 = ROUND(s.v19, 6) AND t.v20 = ROUND(s.v20, 6) AND t.v21 = ROUND(s.v21, 6)
    AND t.v22 = ROUND(s.v22, 6) AND t.v23 = ROUND(s.v23, 6) AND t.v24 = ROUND(s.v24, 6)
    AND t.v25 = ROUND(s.v25, 6) AND t.v26 = ROUND(s.v26, 6) AND t.v27 = ROUND(s.v27, 6) 
    AND t.v28 = ROUND(s.v28, 6)
);


-- ===== 8. DATA VALIDATION QUERIES =====

-- Verify data loading success
SELECT 
    'staging.raw_creditcard' as table_name, 
    COUNT(*) as row_count,
    'Source data' as description
FROM staging.raw_creditcard
UNION ALL
SELECT 
    'analytics.transactions',
    COUNT(*),
    'Main transaction table'
FROM analytics.transactions  
UNION ALL
SELECT 
    'fraud_detection.fraud_cases',
    COUNT(*),
    'Fraud classification table'
FROM fraud_detection.fraud_cases;

-- Validate fraud distribution
SELECT 
    is_fraud,
    COUNT(*) as case_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fraud_detection.fraud_cases), 2) as percentage
FROM fraud_detection.fraud_cases 
GROUP BY is_fraud
ORDER BY is_fraud;

-- Expected results:
-- FALSE: ~284,315 cases (99.83%)
-- TRUE: ~492 cases (0.17%)


-- ===== 9. CLEANUP STAGING DATA (OPTIONAL) =====

-- Uncomment below to remove staging data after successful validation
-- TRUNCATE staging.raw_creditcard;
-- DROP TABLE staging.raw_creditcard;


-- ===== SETUP COMPLETE =====
-- Database is ready for fraud detection analysis
-- Next: Execute 02_fraud_detection_analysis.sql