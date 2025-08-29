-- ===================================================
-- 02_fraud_detection_analysis.sql
-- Advanced Fraud Detection Analysis & Pattern Recognition
-- Fintech Payment Analytics Project
-- ===================================================
-- 
-- Purpose: Comprehensive fraud detection using data-driven thresholds
-- Author: Gokhan Fidan
-- Dataset: 284,807 transactions with 492 fraud cases (0.173%)
--
-- Execution Order: RUN SECOND (after 01_database_setup.sql)
-- Dependencies: analytics.transactions, fraud_detection.fraud_cases
-- Key Discovery: V3 pattern-based detection achieves 89.66% precision
-- ===================================================

-- ===== 1. FRAUD OVERVIEW & BASELINE STATISTICS =====
-- Executive summary of fraud landscape

SELECT 
    'Total Transactions' as metric,
    COUNT(*) as count,
    '100.00%' as percentage
FROM fraud_detection.fraud_cases
UNION ALL
SELECT 
    'Fraud Cases' as metric,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fraud_detection.fraud_cases), 3) || '%' as percentage
FROM fraud_detection.fraud_cases
WHERE is_fraud = true
UNION ALL
SELECT 
    'Normal Cases' as metric,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fraud_detection.fraud_cases), 3) || '%' as percentage
FROM fraud_detection.fraud_cases
WHERE is_fraud = false;


-- ===== 2. DATA-DRIVEN AMOUNT-BASED THRESHOLD OPTIMIZATION =====
-- Testing multiple standard deviation multipliers to find optimal detection threshold

WITH amount_stats AS (
    SELECT
        AVG(t.amount) as avg_amount,
        STDDEV(t.amount) as std_amount
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
),
test_multipliers AS (
    SELECT
        t.transaction_id,
        t.amount,
        fc.is_fraud,
        stats.avg_amount + (1.0 * stats.std_amount) as threshold_1x,
        stats.avg_amount + (1.5 * stats.std_amount) as threshold_1_5x,
        stats.avg_amount + (2.0 * stats.std_amount) as threshold_2x,
        stats.avg_amount + (2.5 * stats.std_amount) as threshold_2_5x,
        stats.avg_amount + (3.0 * stats.std_amount) as threshold_3x
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
    CROSS JOIN amount_stats stats
)
-- Performance comparison of different thresholds
SELECT
    '1.0x StdDev' as threshold_type,
    COUNT(*) FILTER (WHERE amount > threshold_1x) as flagged_transactions,
    COUNT(*) FILTER (WHERE amount > threshold_1x AND is_fraud = true) as fraud_caught,
    ROUND(COUNT(*) FILTER (WHERE amount > threshold_1x AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE amount > threshold_1x), 0), 2) as precision_percentage
FROM test_multipliers
UNION ALL
SELECT
    '1.5x StdDev',
    COUNT(*) FILTER (WHERE amount > threshold_1_5x),
    COUNT(*) FILTER (WHERE amount > threshold_1_5x AND is_fraud = true),
    ROUND(COUNT(*) FILTER (WHERE amount > threshold_1_5x AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE amount > threshold_1_5x), 0), 2)
FROM test_multipliers
UNION ALL
SELECT
    '2.0x StdDev',
    COUNT(*) FILTER (WHERE amount > threshold_2x),
    COUNT(*) FILTER (WHERE amount > threshold_2x AND is_fraud = true),
    ROUND(COUNT(*) FILTER (WHERE amount > threshold_2x AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE amount > threshold_2x), 0), 2)
FROM test_multipliers
UNION ALL
SELECT
    '2.5x StdDev',
    COUNT(*) FILTER (WHERE amount > threshold_2_5x),
    COUNT(*) FILTER (WHERE amount > threshold_2_5x AND is_fraud = true),
    ROUND(COUNT(*) FILTER (WHERE amount > threshold_2_5x AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE amount > threshold_2_5x), 0), 2)
FROM test_multipliers
UNION ALL
SELECT
    '3.0x StdDev',
    COUNT(*) FILTER (WHERE amount > threshold_3x),
    COUNT(*) FILTER (WHERE amount > threshold_3x AND is_fraud = true),
    ROUND(COUNT(*) FILTER (WHERE amount > threshold_3x AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE amount > threshold_3x), 0), 2)
FROM test_multipliers
ORDER BY precision_percentage DESC;

-- Optimal threshold implementation (2.5x StdDev - highest precision: 0.43%)
WITH amount_stats AS (
    SELECT
        AVG(t.amount) as avg_amount,
        STDDEV(t.amount) as std_amount,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY t.amount) as p95_amount,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY t.amount) as p99_amount
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
)
SELECT
    t.transaction_id,
    t.amount,
    fc.is_fraud,
    ROUND(stats.avg_amount, 2) as avg_amount,
    ROUND(stats.avg_amount + (2.5 * stats.std_amount), 2) as optimal_threshold,
    CASE
        WHEN t.amount > stats.avg_amount + (2.5 * stats.std_amount) THEN 'HIGH_PRIORITY_REVIEW'
        ELSE 'NORMAL_MONITORING'
    END as review_priority,
    ROUND((PERCENT_RANK() OVER (ORDER BY t.amount) * 100)::NUMERIC, 2) as amount_percentile
FROM analytics.transactions t
JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
CROSS JOIN amount_stats stats
WHERE t.amount > stats.avg_amount + (2.5 * stats.std_amount)  -- Optimal threshold
ORDER BY t.amount DESC
LIMIT 50;


-- ===== 3. TIME-BASED PATTERN ANALYSIS =====
-- Identifying temporal fraud patterns for operational planning

WITH time_analysis AS (
    SELECT
        t.transaction_id,
        t.transaction_time,
        fc.is_fraud,
        CASE
            WHEN t.transaction_time < 86400 THEN EXTRACT(HOUR FROM (t.transaction_time * INTERVAL '1 second'))
            ELSE EXTRACT(HOUR FROM ((t.transaction_time - 86400) * INTERVAL '1 second'))
        END as hour_of_day
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
)
SELECT
    hour_of_day,
    COUNT(*) as total_transactions,
    COUNT(*) FILTER (WHERE is_fraud = true) as fraud_count,
    ROUND(COUNT(*) FILTER (WHERE is_fraud = true) * 100.0 / COUNT(*), 3) as fraud_rate_percentage,
    ROUND(COUNT(*) FILTER (WHERE is_fraud = true) * 100.0 /
          (SELECT COUNT(*) FROM fraud_detection.fraud_cases WHERE is_fraud = true), 1) as fraud_distribution_percentage
FROM time_analysis
GROUP BY hour_of_day
ORDER BY fraud_rate_percentage DESC;  -- Highest risk hours first


-- ===== 4. STATISTICAL OUTLIER DETECTION - THRESHOLD OPTIMIZATION =====
-- Data-driven Z-score threshold selection for maximum precision

WITH fraud_outliers AS (
    SELECT
        t.transaction_id,
        t.amount,
        fc.is_fraud,
        t.v1, t.v2, t.v3, t.v4, t.v5,
        -- Z-score calculations for top 5 PCA components
        ABS(t.v1 - AVG(t.v1) OVER()) / NULLIF(STDDEV(t.v1) OVER(), 0) as v1_zscore,
        ABS(t.v2 - AVG(t.v2) OVER()) / NULLIF(STDDEV(t.v2) OVER(), 0) as v2_zscore,
        ABS(t.v3 - AVG(t.v3) OVER()) / NULLIF(STDDEV(t.v3) OVER(), 0) as v3_zscore,
        ABS(t.v4 - AVG(t.v4) OVER()) / NULLIF(STDDEV(t.v4) OVER(), 0) as v4_zscore,
        ABS(t.v5 - AVG(t.v5) OVER()) / NULLIF(STDDEV(t.v5) OVER(), 0) as v5_zscore
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
),
zscore_averages AS (
    SELECT
        *,
        (v1_zscore + v2_zscore + v3_zscore + v4_zscore + v5_zscore) / 5 as avg_zscore
    FROM fraud_outliers
)
-- Test multiple Z-score thresholds for optimal performance
SELECT
    'Z-score > 1.0' as threshold_type,
    COUNT(*) FILTER (WHERE avg_zscore > 1.0) as flagged_transactions,
    COUNT(*) FILTER (WHERE avg_zscore > 1.0 AND is_fraud = true) as fraud_caught,
    ROUND(COUNT(*) FILTER (WHERE avg_zscore > 1.0 AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE avg_zscore > 1.0), 0), 2) as precision_percentage
FROM zscore_averages
UNION ALL
SELECT
    'Z-score > 1.5',
    COUNT(*) FILTER (WHERE avg_zscore > 1.5),
    COUNT(*) FILTER (WHERE avg_zscore > 1.5 AND is_fraud = true),
    ROUND(COUNT(*) FILTER (WHERE avg_zscore > 1.5 AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE avg_zscore > 1.5), 0), 2)
FROM zscore_averages
UNION ALL
SELECT
    'Z-score > 2.0',
    COUNT(*) FILTER (WHERE avg_zscore > 2.0),
    COUNT(*) FILTER (WHERE avg_zscore > 2.0 AND is_fraud = true),
    ROUND(COUNT(*) FILTER (WHERE avg_zscore > 2.0 AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE avg_zscore > 2.0), 0), 2)
FROM zscore_averages
UNION ALL
SELECT
    'Z-score > 2.5',
    COUNT(*) FILTER (WHERE avg_zscore > 2.5),
    COUNT(*) FILTER (WHERE avg_zscore > 2.5 AND is_fraud = true),
    ROUND(COUNT(*) FILTER (WHERE avg_zscore > 2.5 AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE avg_zscore > 2.5), 0), 2)
FROM zscore_averages
UNION ALL
SELECT
    'Z-score > 3.0',
    COUNT(*) FILTER (WHERE avg_zscore > 3.0),
    COUNT(*) FILTER (WHERE avg_zscore > 3.0 AND is_fraud = true),
    ROUND(COUNT(*) FILTER (WHERE avg_zscore > 3.0 AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE avg_zscore > 3.0), 0), 2)
FROM zscore_averages
ORDER BY precision_percentage DESC;

-- Optimal Z-score implementation (3.0 threshold - highest precision: 8.35%)
WITH fraud_outliers AS (
    SELECT
        t.transaction_id,
        t.amount,
        fc.is_fraud,
        t.v1, t.v2, t.v3, t.v4, t.v5,
        ABS(t.v1 - AVG(t.v1) OVER()) / NULLIF(STDDEV(t.v1) OVER(), 0) as v1_zscore,
        ABS(t.v2 - AVG(t.v2) OVER()) / NULLIF(STDDEV(t.v2) OVER(), 0) as v2_zscore,
        ABS(t.v3 - AVG(t.v3) OVER()) / NULLIF(STDDEV(t.v3) OVER(), 0) as v3_zscore,
        ABS(t.v4 - AVG(t.v4) OVER()) / NULLIF(STDDEV(t.v4) OVER(), 0) as v4_zscore,
        ABS(t.v5 - AVG(t.v5) OVER()) / NULLIF(STDDEV(t.v5) OVER(), 0) as v5_zscore
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
),
statistical_analysis AS (
    SELECT
        *,
        (v1_zscore + v2_zscore + v3_zscore + v4_zscore + v5_zscore) / 5 as avg_zscore,
        CASE
            WHEN (v1_zscore + v2_zscore + v3_zscore + v4_zscore + v5_zscore) / 5 > 3.0 THEN 'STATISTICAL_OUTLIER'
            ELSE 'NORMAL_PATTERN'
        END as outlier_classification
    FROM fraud_outliers
)
SELECT
    transaction_id,
    amount,
    is_fraud,
    ROUND(v1_zscore::NUMERIC, 2) as v1_zscore,
    ROUND(v2_zscore::NUMERIC, 2) as v2_zscore,
    ROUND(v3_zscore::NUMERIC, 2) as v3_zscore,
    ROUND(v4_zscore::NUMERIC, 2) as v4_zscore,
    ROUND(v5_zscore::NUMERIC, 2) as v5_zscore,
    ROUND(avg_zscore::NUMERIC, 2) as avg_zscore,
    outlier_classification,
    -- Identify dominant outlier feature (critical discovery: V3 dominance)
    CASE
        WHEN v1_zscore = GREATEST(v1_zscore, v2_zscore, v3_zscore, v4_zscore, v5_zscore) THEN 'V1_dominant'
        WHEN v2_zscore = GREATEST(v1_zscore, v2_zscore, v3_zscore, v4_zscore, v5_zscore) THEN 'V2_dominant'
        WHEN v3_zscore = GREATEST(v1_zscore, v2_zscore, v3_zscore, v4_zscore, v5_zscore) THEN 'V3_dominant'
        WHEN v4_zscore = GREATEST(v1_zscore, v2_zscore, v3_zscore, v4_zscore, v5_zscore) THEN 'V4_dominant'
        ELSE 'V5_dominant'
    END as dominant_outlier_feature
FROM statistical_analysis
WHERE avg_zscore > 3.0  -- Optimal threshold from data analysis
ORDER BY is_fraud DESC, avg_zscore DESC
LIMIT 50;

-- V3 Pattern-Based Real-Time Fraud Detection (89.66% precision breakthrough)
WITH fraud_outliers AS (
    SELECT
        t.transaction_id,
        t.amount,
        fc.is_fraud,
        t.v3,
        ABS(t.v3 - AVG(t.v3) OVER()) / NULLIF(STDDEV(t.v3) OVER(), 0) as v3_zscore
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
),
pattern_analysis AS (
    SELECT
        *,
        (v3_zscore) as avg_zscore  -- V3-focused analysis
    FROM fraud_outliers
)
SELECT
    transaction_id,
    amount,
    is_fraud,
    ROUND(v3_zscore::NUMERIC, 2) as v3_zscore,
    CASE
        WHEN v3_zscore > 15 AND amount IN (1, 99.99, 0.01) THEN 'IMMEDIATE_BLOCK'
        WHEN v3_zscore > 12 AND amount < 10 THEN 'HIGH_PRIORITY_REVIEW'
        WHEN v3_zscore > 10 THEN 'MONITOR_CLOSELY'
        ELSE 'NORMAL_PROCESSING'
    END as fraud_action,
    'V3_Pattern_Based' as detection_method
FROM pattern_analysis
WHERE v3_zscore > 10  -- High-risk transactions only
ORDER BY v3_zscore DESC
LIMIT 100;


-- ===== 5. FRAUD DETECTION PERFORMANCE COMPARISON =====
-- Comprehensive evaluation of all detection methods

WITH amount_stats AS (
    SELECT
        AVG(t.amount) as avg_amount,
        STDDEV(t.amount) as std_amount
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
),
outlier_stats AS (
    SELECT
        t.transaction_id,
        fc.is_fraud,
        t.amount,
        -- Amount-based detection (2.5x std threshold)
        CASE WHEN t.amount > stats.avg_amount + (2.5 * stats.std_amount) THEN 1 ELSE 0 END as amount_flagged,
        -- Statistical outlier detection 
        ABS(t.v3 - AVG(t.v3) OVER()) / NULLIF(STDDEV(t.v3) OVER(), 0) as v3_zscore
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
    CROSS JOIN amount_stats stats
),
detection_flags AS (
    SELECT
        *,
        CASE WHEN v3_zscore > 3.0 THEN 1 ELSE 0 END as statistical_flagged,
        CASE WHEN v3_zscore > 15 AND amount IN (1, 99.99, 0.01) THEN 1 ELSE 0 END as pattern_flagged
    FROM outlier_stats
)
-- Performance metrics for each detection method
SELECT
    'Amount-Based (2.5x StdDev)' as detection_method,
    COUNT(*) FILTER (WHERE amount_flagged = 1) as total_flagged,
    COUNT(*) FILTER (WHERE amount_flagged = 1 AND is_fraud = true) as fraud_caught,
    COUNT(*) FILTER (WHERE amount_flagged = 1 AND is_fraud = false) as false_positives,
    ROUND(COUNT(*) FILTER (WHERE amount_flagged = 1 AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE amount_flagged = 1), 0), 2) as precision_percentage,
    ROUND(COUNT(*) FILTER (WHERE amount_flagged = 1 AND is_fraud = true) * 100.0 /
          (SELECT COUNT(*) FROM fraud_detection.fraud_cases WHERE is_fraud = true), 2) as recall_percentage
FROM detection_flags
UNION ALL
SELECT
    'Statistical Outlier (Z-score > 3.0)',
    COUNT(*) FILTER (WHERE statistical_flagged = 1),
    COUNT(*) FILTER (WHERE statistical_flagged = 1 AND is_fraud = true),
    COUNT(*) FILTER (WHERE statistical_flagged = 1 AND is_fraud = false),
    ROUND(COUNT(*) FILTER (WHERE statistical_flagged = 1 AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE statistical_flagged = 1), 0), 2),
    ROUND(COUNT(*) FILTER (WHERE statistical_flagged = 1 AND is_fraud = true) * 100.0 /
          (SELECT COUNT(*) FROM fraud_detection.fraud_cases WHERE is_fraud = true), 2)
FROM detection_flags
UNION ALL
SELECT
    'V3 Pattern-Based (Advanced)',
    COUNT(*) FILTER (WHERE pattern_flagged = 1),
    COUNT(*) FILTER (WHERE pattern_flagged = 1 AND is_fraud = true),
    COUNT(*) FILTER (WHERE pattern_flagged = 1 AND is_fraud = false),
    ROUND(COUNT(*) FILTER (WHERE pattern_flagged = 1 AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE pattern_flagged = 1), 0), 2),
    ROUND(COUNT(*) FILTER (WHERE pattern_flagged = 1 AND is_fraud = true) * 100.0 /
          (SELECT COUNT(*) FROM fraud_detection.fraud_cases WHERE is_fraud = true), 2)
FROM detection_flags
ORDER BY precision_percentage DESC;


-- ===== 6. PRODUCTION-READY VIEWS =====
-- Reusable views for operational fraud detection

-- Hourly fraud statistics for operational monitoring
CREATE OR REPLACE VIEW fraud_detection.fraud_hourly_stats AS
WITH time_analysis AS (
    SELECT
        t.transaction_id,
        t.transaction_time,
        fc.is_fraud,
        CASE
            WHEN t.transaction_time < 86400 THEN EXTRACT(HOUR FROM (t.transaction_time * INTERVAL '1 second'))
            ELSE EXTRACT(HOUR FROM ((t.transaction_time - 86400) * INTERVAL '1 second'))
        END as hour_of_day
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
)
SELECT
    hour_of_day,
    COUNT(*) as total_transactions,
    COUNT(*) FILTER (WHERE is_fraud = true) as fraud_count,
    COUNT(*) FILTER (WHERE is_fraud = false) as normal_count,
    ROUND(COUNT(*) FILTER (WHERE is_fraud = true) * 100.0 / COUNT(*), 3) as fraud_rate_percentage,
    ROUND(COUNT(*) FILTER (WHERE is_fraud = true) * 100.0 /
          (SELECT COUNT(*) FROM fraud_detection.fraud_cases WHERE is_fraud = true), 1) as fraud_distribution_percentage
FROM time_analysis
GROUP BY hour_of_day;

-- High-risk transactions based on V3 pattern analysis
CREATE OR REPLACE VIEW fraud_detection.high_risk_transactions AS
WITH fraud_outliers AS (
    SELECT
        t.transaction_id,
        t.amount,
        fc.is_fraud,
        t.v3,
        ABS(t.v3 - AVG(t.v3) OVER()) / NULLIF(STDDEV(t.v3) OVER(), 0) as v3_zscore
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
)
SELECT
    transaction_id,
    amount,
    is_fraud,
    ROUND(v3_zscore::NUMERIC, 2) as v3_risk_score,
    CASE
        WHEN v3_zscore > 15 AND amount IN (1, 99.99, 0.01) THEN 'IMMEDIATE_BLOCK'
        WHEN v3_zscore > 12 AND amount < 10 THEN 'HIGH_PRIORITY_REVIEW'
        WHEN v3_zscore > 10 THEN 'MONITOR_CLOSELY'
        ELSE 'NORMAL_PROCESSING'
    END as recommended_action,
    CURRENT_TIMESTAMP as analysis_timestamp
FROM fraud_outliers
WHERE v3_zscore > 10;  -- Only high-risk transactions

-- Detection method performance comparison
CREATE OR REPLACE VIEW fraud_detection.detection_performance_summary AS
WITH amount_stats AS (
    SELECT
        AVG(t.amount) as avg_amount,
        STDDEV(t.amount) as std_amount
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
),
outlier_stats AS (
    SELECT
        t.transaction_id,
        fc.is_fraud,
        t.amount,
        CASE WHEN t.amount > stats.avg_amount + (2.5 * stats.std_amount) THEN 1 ELSE 0 END as amount_flagged,
        ABS(t.v3 - AVG(t.v3) OVER()) / NULLIF(STDDEV(t.v3) OVER(), 0) as v3_zscore
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
    CROSS JOIN amount_stats stats
),
detection_flags AS (
    SELECT
        *,
        CASE WHEN v3_zscore > 3.0 THEN 1 ELSE 0 END as statistical_flagged,
        CASE WHEN v3_zscore > 15 AND amount IN (1, 99.99, 0.01) THEN 1 ELSE 0 END as pattern_flagged
    FROM outlier_stats
)
SELECT
    'Amount-Based' as detection_method,
    COUNT(*) FILTER (WHERE amount_flagged = 1) as total_flagged,
    COUNT(*) FILTER (WHERE amount_flagged = 1 AND is_fraud = true) as fraud_caught,
    ROUND(COUNT(*) FILTER (WHERE amount_flagged = 1 AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE amount_flagged = 1), 0), 2) as precision_percentage,
    'Low precision, high false positives' as performance_note
FROM detection_flags
UNION ALL
SELECT
    'Statistical Outlier',
    COUNT(*) FILTER (WHERE statistical_flagged = 1),
    COUNT(*) FILTER (WHERE statistical_flagged = 1 AND is_fraud = true),
    ROUND(COUNT(*) FILTER (WHERE statistical_flagged = 1 AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE statistical_flagged = 1), 0), 2),
    'Moderate precision, manageable workload'
FROM detection_flags
UNION ALL
SELECT
    'V3 Pattern-Based',
    COUNT(*) FILTER (WHERE pattern_flagged = 1),
    COUNT(*) FILTER (WHERE pattern_flagged = 1 AND is_fraud = true),
    ROUND(COUNT(*) FILTER (WHERE pattern_flagged = 1 AND is_fraud = true) * 100.0 /
          NULLIF(COUNT(*) FILTER (WHERE pattern_flagged = 1), 0), 2),
    'Highest precision, optimal for production'
FROM detection_flags;


-- ===== 7. STORED PROCEDURE FOR PARAMETERIZED ANALYSIS =====
-- Flexible analysis function for different time ranges and thresholds

CREATE OR REPLACE FUNCTION fraud_detection.analyze_fraud_by_parameters(
    start_hour INT DEFAULT 0,
    end_hour INT DEFAULT 23,
    v3_threshold NUMERIC DEFAULT 10.0
)
RETURNS TABLE (
    hour_of_day INT,
    total_transactions BIGINT,
    fraud_count BIGINT,
    fraud_rate NUMERIC,
    high_risk_transactions BIGINT
)
LANGUAGE SQL
AS $$
    WITH time_analysis AS (
        SELECT
            t.transaction_id,
            fc.is_fraud,
            CASE
                WHEN t.transaction_time < 86400 THEN EXTRACT(HOUR FROM (t.transaction_time * INTERVAL '1 second'))
                ELSE EXTRACT(HOUR FROM ((t.transaction_time - 86400) * INTERVAL '1 second'))
            END::INT as hour_of_day,
            ABS(t.v3 - AVG(t.v3) OVER()) / NULLIF(STDDEV(t.v3) OVER(), 0) as v3_zscore
        FROM analytics.transactions t
        JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
    )
    SELECT
        hour_of_day,
        COUNT(*) as total_transactions,
        COUNT(*) FILTER (WHERE is_fraud = true) as fraud_count,
        ROUND(COUNT(*) FILTER (WHERE is_fraud = true) * 100.0 / COUNT(*), 3) as fraud_rate,
        COUNT(*) FILTER (WHERE v3_zscore > v3_threshold) as high_risk_transactions
    FROM time_analysis
    WHERE hour_of_day BETWEEN start_hour AND end_hour
    GROUP BY hour_of_day
    ORDER BY hour_of_day;
$$;


-- ===== ANALYSIS COMPLETE =====
-- Key findings:
-- 1. V3 pattern-based detection: 89.66% precision (breakthrough result)
-- 2. Optimal amount threshold: 2.5x standard deviation (0.43% precision)  
-- 3. Peak fraud hours: 02:00-04:00 (2.4% fraud rate)
-- 4. Statistical outlier threshold: Z-score > 3.0 (8.35% precision)
-- 
-- Next: Execute 03_business_intelligence.sql for financial impact analysis