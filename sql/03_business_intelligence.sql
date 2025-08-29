-- ===================================================
-- 03_business_intelligence.sql
-- Business Intelligence & Executive Reporting
-- Fintech Payment Analytics Project
-- ===================================================
-- 
-- Purpose: Executive KPIs, financial impact analysis, and business insights
-- Author: Gokhan Fidan
-- Business Focus: Financial impact, operational metrics, transaction trends
--
-- Execution Order: RUN THIRD (after 02_fraud_detection_analysis.sql)
-- Dependencies: All previous analyses and VIEWs
-- Key Metrics: €60,127.97 fraud loss (0.239% of €25.16M volume)
-- ===================================================

-- ===== 1. EXECUTIVE SUMMARY DASHBOARD =====
-- High-level KPIs for C-level executives

SELECT 
    'TRANSACTION OVERVIEW' as kpi_category,
    'Total Transactions Processed' as metric,
    COUNT(*)::TEXT as value,
    '100%' as benchmark
FROM analytics.transactions
UNION ALL
SELECT 
    'FRAUD OVERVIEW',
    'Total Fraud Cases Detected',
    COUNT(*)::TEXT,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM analytics.transactions), 3) || '%'
FROM fraud_detection.fraud_cases WHERE is_fraud = true
UNION ALL
SELECT 
    'FINANCIAL IMPACT',
    'Total Transaction Volume (EUR)',
    '�' || TO_CHAR(SUM(amount), 'FM999,999,999.00'),
    '100%'
FROM analytics.transactions
UNION ALL
SELECT 
    'FINANCIAL IMPACT',
    'Fraud Loss Amount (EUR)', 
    '�' || TO_CHAR(SUM(t.amount), 'FM999,999.00'),
    ROUND(SUM(t.amount) * 100.0 / (SELECT SUM(amount) FROM analytics.transactions), 3) || '%'
FROM analytics.transactions t
JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
WHERE fc.is_fraud = true
UNION ALL
SELECT 
    'OPERATIONAL EFFICIENCY',
    'Average Transaction Amount (EUR)',
    '€' || TO_CHAR(AVG(amount), 'FM999.00'),
    'Industry Benchmark'
FROM analytics.transactions
ORDER BY kpi_category, metric;


-- ===== 2. FRAUD TREND ANALYSIS =====
-- Time-based patterns for operational planning and resource allocation

-- Hourly transaction and fraud patterns (comprehensive 24-hour analysis)
WITH time_analysis AS (
    SELECT 
        CASE 
            WHEN t.transaction_time < 86400 THEN t.transaction_time
            ELSE t.transaction_time - 86400
        END as day_seconds,
        fc.is_fraud,
        t.amount
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
),
hourly_patterns AS (
    SELECT 
        EXTRACT(HOUR FROM (day_seconds * INTERVAL '1 second')) as hour_of_day,
        COUNT(*) as total_transactions,
        COUNT(*) FILTER (WHERE is_fraud = true) as fraud_transactions,
        SUM(amount) as total_volume,
        SUM(amount) FILTER (WHERE is_fraud = true) as fraud_volume
    FROM time_analysis
    GROUP BY EXTRACT(HOUR FROM (day_seconds * INTERVAL '1 second'))
)
SELECT 
    hour_of_day,
    total_transactions,
    fraud_transactions as fraud_count,
    ROUND(fraud_transactions * 100.0 / total_transactions, 3) as fraud_rate_percentage,
    '�' || ROUND(total_volume, 0)::TEXT as hourly_volume,
    '�' || ROUND(fraud_volume, 2)::TEXT as fraud_loss,
    ROUND(fraud_volume * 100.0 / total_volume, 4) as fraud_loss_ratio_percentage
FROM hourly_patterns
ORDER BY fraud_rate_percentage DESC;


-- ===== 3. FINANCIAL IMPACT ANALYSIS =====
-- Pure data-driven financial impact analysis

WITH fraud_impact AS (
    SELECT 
        SUM(t.amount) as total_fraud_loss,
        AVG(t.amount) as avg_fraud_amount,
        COUNT(*) as total_fraud_cases
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
    WHERE fc.is_fraud = true
),
business_metrics AS (
    SELECT 
        SUM(amount) as total_transaction_volume,
        COUNT(*) as total_transactions,
        AVG(amount) as avg_transaction_amount
    FROM analytics.transactions
)
SELECT 
    'FRAUD FINANCIAL IMPACT' as analysis_category,
    'Total Fraud Loss' as metric,
    '€' || ROUND(fi.total_fraud_loss, 2)::TEXT as value,
    ROUND(fi.total_fraud_loss * 100.0 / bm.total_transaction_volume, 4) || '%' as percentage_of_volume
FROM fraud_impact fi, business_metrics bm
UNION ALL
SELECT 
    'FRAUD FINANCIAL IMPACT',
    'Average Fraud Transaction',
    '€' || ROUND(fi.avg_fraud_amount, 2)::TEXT,
    ROUND(fi.avg_fraud_amount / bm.avg_transaction_amount * 100, 1) || '% vs avg transaction'
FROM fraud_impact fi, business_metrics bm
UNION ALL
SELECT 
    'FRAUD FINANCIAL IMPACT',
    'Fraud Loss per Hour',
    '€' || ROUND(fi.total_fraud_loss / 48, 2)::TEXT,  -- 2-day period = 48 hours
    '2-day period average'
FROM fraud_impact fi
UNION ALL
SELECT 
    'BUSINESS PERFORMANCE',
    'Fraud Detection Rate',
    ROUND(fi.total_fraud_cases * 100.0 / bm.total_transactions, 3) || '%',
    'Industry benchmark: 0.5%'
FROM fraud_impact fi, business_metrics bm
UNION ALL
SELECT 
    'DETECTION EFFECTIVENESS',
    'V3 Method Precision',
    '89.66%',
    'Best performing method'
FROM fraud_impact fi
ORDER BY analysis_category, metric;


-- ===== 4. TRANSACTION SEGMENTATION ANALYSIS =====
-- Amount-based transaction patterns and customer behavior insights

WITH transaction_segments AS (
    SELECT 
        t.transaction_id,
        t.amount,
        fc.is_fraud,
        CASE 
            WHEN t.amount <= 10 THEN 'Micro (≤€10)'
            WHEN t.amount <= 50 THEN 'Small (€10-50)'
            WHEN t.amount <= 200 THEN 'Medium (€50-200)'
            WHEN t.amount <= 1000 THEN 'Large (€200-1000)'
            ELSE 'Premium (>€1000)'
        END as amount_segment
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
),
segment_analysis AS (
    SELECT 
        amount_segment,
        COUNT(*) as total_transactions,
        COUNT(*) FILTER (WHERE is_fraud = true) as fraud_transactions,
        SUM(amount) as total_volume,
        SUM(amount) FILTER (WHERE is_fraud = true) as fraud_volume,
        AVG(amount) as avg_transaction_amount,
        AVG(amount) FILTER (WHERE is_fraud = true) as avg_fraud_amount,
        MIN(amount) as min_amount,
        MAX(amount) as max_amount
    FROM transaction_segments
    GROUP BY amount_segment
)
SELECT 
    amount_segment,
    total_transactions,
    fraud_transactions,
    ROUND(fraud_transactions * 100.0 / total_transactions, 3) as fraud_rate_percentage,
    '€' || ROUND(total_volume, 0)::TEXT as segment_volume,
    '€' || ROUND(fraud_volume, 2)::TEXT as fraud_loss,
    ROUND(fraud_volume * 100.0 / total_volume, 3) as volume_loss_percentage,
    '€' || ROUND(avg_transaction_amount, 2)::TEXT as avg_transaction,
    '€' || ROUND(avg_fraud_amount, 2)::TEXT as avg_fraud_amount,
    ROUND(total_transactions * 100.0 / (SELECT COUNT(*) FROM transaction_segments), 1) as transaction_share_percentage,
    ROUND(total_volume * 100.0 / (SELECT SUM(amount) FROM analytics.transactions), 1) as volume_share_percentage
FROM segment_analysis
ORDER BY 
    CASE amount_segment
        WHEN 'Micro (≤€10)' THEN 1
        WHEN 'Small (€10-50)' THEN 2
        WHEN 'Medium (€50-200)' THEN 3
        WHEN 'Large (€200-1000)' THEN 4
        WHEN 'Premium (>€1000)' THEN 5
    END;


-- ===== 5. VOLUME & PERFORMANCE TRENDS =====
-- Time-series analysis of transaction patterns and system performance

WITH time_periods AS (
    SELECT 
        t.transaction_id,
        t.amount,
        fc.is_fraud,
        t.transaction_time,
        -- Create 4-hour blocks for trend analysis
        CASE 
            WHEN t.transaction_time BETWEEN 0 AND 14399 THEN 'Early Morning (00:00-03:59)'
            WHEN t.transaction_time BETWEEN 14400 AND 28799 THEN 'Morning (04:00-07:59)'  
            WHEN t.transaction_time BETWEEN 28800 AND 43199 THEN 'Business Hours (08:00-11:59)'
            WHEN t.transaction_time BETWEEN 43200 AND 57599 THEN 'Afternoon (12:00-15:59)'
            WHEN t.transaction_time BETWEEN 57600 AND 71999 THEN 'Evening (16:00-19:59)'
            WHEN t.transaction_time BETWEEN 72000 AND 86399 THEN 'Night (20:00-23:59)'
            -- Day 2
            WHEN t.transaction_time BETWEEN 86400 AND 100799 THEN 'Early Morning (00:00-03:59)'
            WHEN t.transaction_time BETWEEN 100800 AND 115199 THEN 'Morning (04:00-07:59)'
            WHEN t.transaction_time BETWEEN 115200 AND 129599 THEN 'Business Hours (08:00-11:59)'
            WHEN t.transaction_time BETWEEN 129600 AND 143999 THEN 'Afternoon (12:00-15:59)'
            WHEN t.transaction_time BETWEEN 144000 AND 158399 THEN 'Evening (16:00-19:59)'
            ELSE 'Night (20:00-23:59)'
        END as time_period,
        -- Transaction value classification
        CASE 
            WHEN t.amount <= 50 THEN 'Low Value'
            WHEN t.amount <= 500 THEN 'Medium Value'
            ELSE 'High Value'
        END as value_category
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
),
period_analysis AS (
    SELECT 
        time_period,
        COUNT(*) as total_transactions,
        COUNT(*) FILTER (WHERE is_fraud = true) as fraud_count,
        SUM(amount) as total_volume,
        SUM(amount) FILTER (WHERE is_fraud = true) as fraud_volume,
        AVG(amount) as avg_transaction_amount,
        -- Value distribution analysis
        COUNT(*) FILTER (WHERE value_category = 'Low Value') as low_value_count,
        COUNT(*) FILTER (WHERE value_category = 'Medium Value') as medium_value_count,
        COUNT(*) FILTER (WHERE value_category = 'High Value') as high_value_count,
        -- Fraud by value category
        COUNT(*) FILTER (WHERE value_category = 'High Value' AND is_fraud = true) as high_value_fraud
    FROM time_periods
    GROUP BY time_period
)
SELECT 
    time_period,
    total_transactions,
    fraud_count,
    ROUND(fraud_count * 100.0 / total_transactions, 3) as fraud_rate_percentage,
    '�' || ROUND(total_volume, 0)::TEXT as period_volume,
    '�' || ROUND(fraud_volume, 2)::TEXT as fraud_loss,
    '�' || ROUND(avg_transaction_amount, 2)::TEXT as avg_transaction,
    ROUND(low_value_count * 100.0 / total_transactions, 1) as low_value_percentage,
    ROUND(medium_value_count * 100.0 / total_transactions, 1) as medium_value_percentage, 
    ROUND(high_value_count * 100.0 / total_transactions, 1) as high_value_percentage,
    high_value_fraud,
    CASE 
        WHEN high_value_count > 0 THEN ROUND(high_value_fraud * 100.0 / high_value_count, 2)
        ELSE 0 
    END as high_value_fraud_rate
FROM period_analysis
ORDER BY 
    CASE time_period
        WHEN 'Early Morning (00:00-03:59)' THEN 1
        WHEN 'Morning (04:00-07:59)' THEN 2
        WHEN 'Business Hours (08:00-11:59)' THEN 3
        WHEN 'Afternoon (12:00-15:59)' THEN 4
        WHEN 'Evening (16:00-19:59)' THEN 5
        WHEN 'Night (20:00-23:59)' THEN 6
    END;


-- ===== 6. OPERATIONAL INSIGHTS SUMMARY =====
-- Key metrics for operational planning and resource allocation

-- Risk-based monitoring recommendations
SELECT 
    'OPERATIONAL RECOMMENDATIONS' as category,
    'Peak Risk Hours' as recommendation_type,
    '02:00-04:00' as timeframe,
    '2.43% fraud rate - Increase monitoring staff' as action_required
UNION ALL
SELECT 
    'OPERATIONAL RECOMMENDATIONS',
    'Transaction Segments',
    'Premium (€1000+)',
    '0.31% fraud rate - Enhanced screening protocols'
UNION ALL
SELECT 
    'OPERATIONAL RECOMMENDATIONS',
    'Detection Method',
    'V3 Pattern-Based',
    '89.66% precision - Deploy for real-time monitoring'
UNION ALL
SELECT 
    'OPERATIONAL RECOMMENDATIONS',
    'Alert Thresholds',
    'V3 Z-score > 15',
    'Immediate block for amounts €1, €99.99, €0.01'
UNION ALL
SELECT 
    'PERFORMANCE BENCHMARKS',
    'Fraud Rate vs Industry',
    '0.173% vs 0.5%',
    '65% better than industry benchmark'
UNION ALL
SELECT 
    'PERFORMANCE BENCHMARKS',
    'Financial Impact',
    '0.239% of volume',
    'Well below industry average of 0.5%+'
ORDER BY category, recommendation_type;


-- ===== 7. BUSINESS INTELLIGENCE VIEWS FOR DASHBOARDS =====
-- Production-ready views for ongoing BI reporting

-- Executive dashboard summary view
CREATE OR REPLACE VIEW fraud_detection.executive_dashboard AS
WITH daily_metrics AS (
    SELECT 
        COUNT(*) as total_transactions,
        COUNT(*) FILTER (WHERE fc.is_fraud = true) as fraud_cases,
        SUM(t.amount) as total_volume,
        SUM(t.amount) FILTER (WHERE fc.is_fraud = true) as fraud_loss,
        AVG(t.amount) as avg_transaction
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
)
SELECT 
    total_transactions,
    fraud_cases,
    ROUND(fraud_cases * 100.0 / total_transactions, 3) as fraud_rate,
    total_volume,
    fraud_loss,
    ROUND(fraud_loss * 100.0 / total_volume, 4) as loss_percentage,
    avg_transaction,
    CURRENT_TIMESTAMP as report_timestamp
FROM daily_metrics;

-- Segment performance view
CREATE OR REPLACE VIEW fraud_detection.segment_performance AS
WITH segment_data AS (
    SELECT 
        CASE 
            WHEN t.amount <= 10 THEN 'Micro'
            WHEN t.amount <= 50 THEN 'Small'
            WHEN t.amount <= 200 THEN 'Medium'
            WHEN t.amount <= 1000 THEN 'Large'
            ELSE 'Premium'
        END as segment,
        COUNT(*) as transactions,
        COUNT(*) FILTER (WHERE fc.is_fraud = true) as fraud_count,
        SUM(t.amount) as volume,
        SUM(t.amount) FILTER (WHERE fc.is_fraud = true) as fraud_volume
    FROM analytics.transactions t
    JOIN fraud_detection.fraud_cases fc ON t.transaction_id = fc.transaction_id
    GROUP BY 1
)
SELECT 
    segment,
    transactions,
    fraud_count,
    ROUND(fraud_count * 100.0 / transactions, 3) as fraud_rate,
    volume,
    fraud_volume,
    ROUND(fraud_volume * 100.0 / volume, 4) as loss_rate,
    CURRENT_TIMESTAMP as report_timestamp
FROM segment_data
ORDER BY volume DESC;


-- ===== BUSINESS INTELLIGENCE COMPLETE =====
-- Summary of key business insights:
-- 1. €60,127.97 total fraud loss (0.239% of €25.16M volume)
-- 2. 0.173% fraud rate (65% better than 0.5% industry benchmark)
-- 3. Premium segment (€1000+) highest risk at 0.306% fraud rate
-- 4. Early morning hours (02:00-04:00) peak risk period at 2.43% fraud rate
-- 5. V3 pattern-based detection optimal for production (89.66% precision)
--
-- Financial Impact: €1,252/hour average fraud loss, manageable operational cost
-- Recommendation: Deploy V3 pattern detection for real-time fraud prevention