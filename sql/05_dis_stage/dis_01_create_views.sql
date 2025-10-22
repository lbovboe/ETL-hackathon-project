-- ============================================================================
-- DIS LAYER: DISSEMINATION VIEWS (BUSINESS INSIGHTS)
-- ============================================================================
-- Purpose: Create business-friendly analytical views that answer:
--          "What lifestyle choices can improve my financial burden?"
-- Layer: Stage 5 of 5 (Final Stage - DST → DIS)
-- 
-- This layer creates 5 main analytical views:
--   1. vw_financial_health_scorecard  - Overall financial health per person
--   2. vw_spending_recommendations    - Personalized cost-saving recommendations
--   3. vw_budget_alerts               - Overspending warnings and alerts
--   4. vw_category_insights           - Category-level insights and opportunities
--   5. vw_lifestyle_improvement_plan  - Actionable lifestyle change suggestions
--
-- Benefits:
--   - Actionable insights (what to do, not just what happened)
--   - Personalized recommendations based on spending patterns
--   - Clear financial health indicators
--   - Prioritized opportunities for cost reduction
-- ============================================================================

-- Drop existing views if they exist (for clean re-runs)
DROP VIEW IF EXISTS vw_financial_health_scorecard CASCADE;
DROP VIEW IF EXISTS vw_spending_recommendations CASCADE;
DROP VIEW IF EXISTS vw_budget_alerts CASCADE;
DROP VIEW IF EXISTS vw_category_insights CASCADE;
DROP VIEW IF EXISTS vw_lifestyle_improvement_plan CASCADE;

-- ============================================================================
-- VIEW 1: Financial Health Scorecard
-- ============================================================================
-- Purpose: Comprehensive financial health assessment per person
-- Grain: One row per person per month
-- Usage: SELECT * FROM vw_financial_health_scorecard WHERE year = 2024 AND month = 12;
-- ============================================================================

CREATE OR REPLACE VIEW vw_financial_health_scorecard AS
WITH latest_month AS (
    SELECT MAX(year * 100 + month) as latest_period
    FROM dst_person_analytics
),
health_scores AS (
    SELECT 
        pa.person_name,
        pa.year,
        pa.month,
        pa.total_spending,
        pa.essential_spending,
        pa.discretionary_spending,
        pa.essential_percent,
        pa.discretionary_percent,
        pa.essential_to_discretionary_ratio,
        pa.mom_percent_change,
        
        -- Financial Health Score (0-100)
        CASE 
            -- Perfect score components:
            -- Essential ratio 30-40% = 30 points
            -- Discretionary < 35% = 30 points
            -- Spending stable/decreasing = 20 points
            -- Good diversity = 20 points
            WHEN pa.essential_percent BETWEEN 30 AND 40 THEN 30
            WHEN pa.essential_percent BETWEEN 25 AND 30 THEN 25
            WHEN pa.essential_percent BETWEEN 40 AND 45 THEN 25
            WHEN pa.essential_percent BETWEEN 20 AND 25 THEN 20
            ELSE 10
        END +
        CASE 
            WHEN pa.discretionary_percent < 25 THEN 30
            WHEN pa.discretionary_percent < 35 THEN 25
            WHEN pa.discretionary_percent < 45 THEN 20
            WHEN pa.discretionary_percent < 55 THEN 15
            ELSE 5
        END +
        CASE 
            WHEN pa.mom_percent_change IS NULL THEN 15
            WHEN pa.mom_percent_change <= 0 THEN 20
            WHEN pa.mom_percent_change < 10 THEN 15
            WHEN pa.mom_percent_change < 20 THEN 10
            ELSE 5
        END +
        CASE 
            WHEN pa.unique_categories_count >= 7 THEN 20
            WHEN pa.unique_categories_count >= 5 THEN 15
            WHEN pa.unique_categories_count >= 3 THEN 10
            ELSE 5
        END AS health_score,
        
        -- Health Grade
        CASE 
            WHEN (
                CASE WHEN pa.essential_percent BETWEEN 30 AND 40 THEN 30 ELSE 10 END +
                CASE WHEN pa.discretionary_percent < 35 THEN 25 ELSE 5 END
            ) >= 75 THEN '🌟 EXCELLENT'
            WHEN (
                CASE WHEN pa.essential_percent BETWEEN 25 AND 40 THEN 25 ELSE 10 END +
                CASE WHEN pa.discretionary_percent < 45 THEN 20 ELSE 5 END
            ) >= 60 THEN '✅ GOOD'
            WHEN (
                CASE WHEN pa.essential_percent >= 20 THEN 20 ELSE 10 END +
                CASE WHEN pa.discretionary_percent < 55 THEN 15 ELSE 5 END
            ) >= 45 THEN '⚡ FAIR'
            ELSE '⚠️ NEEDS IMPROVEMENT'
        END AS health_grade,
        
        -- Specific Issues
        CASE WHEN pa.discretionary_percent > 50 THEN '🔴 High Discretionary' ELSE NULL END AS issue_1,
        CASE WHEN pa.essential_percent < 20 THEN '🔴 Low Essential' ELSE NULL END AS issue_2,
        CASE WHEN pa.mom_percent_change > 20 THEN '🔴 Spending Spike' ELSE NULL END AS issue_3,
        
        -- Opportunity Size (potential monthly savings)
        CASE 
            WHEN pa.discretionary_percent > 40 
            THEN ROUND((pa.discretionary_spending * (pa.discretionary_percent - 35) / 100), 2)
            ELSE 0
        END AS potential_monthly_savings,
        
        pa.top_category,
        pa.top_category_spending,
        pa.transaction_count,
        pa.unique_categories_count
        
    FROM dst_person_analytics pa
)
SELECT 
    person_name,
    year,
    month,
    total_spending,
    essential_spending,
    discretionary_spending,
    essential_percent,
    discretionary_percent,
    essential_to_discretionary_ratio,
    health_score,
    health_grade,
    issue_1,
    issue_2,
    issue_3,
    potential_monthly_savings,
    mom_percent_change,
    top_category,
    unique_categories_count,
    transaction_count
FROM health_scores
ORDER BY year DESC, month DESC, health_score ASC;

COMMENT ON VIEW vw_financial_health_scorecard IS 
'Comprehensive financial health assessment with scores, grades, and identified issues per person per month';

-- ============================================================================
-- VIEW 2: Spending Recommendations
-- ============================================================================
-- Purpose: Personalized, actionable recommendations for cost reduction
-- Grain: Multiple rows per person (one per recommendation)
-- Usage: SELECT * FROM vw_spending_recommendations WHERE person_name = 'John Tan';
-- ============================================================================

CREATE OR REPLACE VIEW vw_spending_recommendations AS
WITH latest_month AS (
    SELECT MAX(year * 100 + month) as latest_period
    FROM dst_person_analytics
),
person_data AS (
    SELECT 
        pa.person_name,
        pa.year,
        pa.month,
        pa.total_spending,
        pa.discretionary_spending,
        pa.discretionary_percent,
        pa.essential_to_discretionary_ratio,
        pa.top_category,
        pa.top_category_spending,
        pa.top_category_percent,
        pa.weekend_spending_percent
    FROM dst_person_analytics pa
),
category_data AS (
    SELECT 
        mss.person_name,
        mss.category_name,
        mss.category_group,
        SUM(mss.total_spending) as category_total,
        SUM(mss.transaction_count) as category_txn_count,
        AVG(mss.avg_transaction_amount) as avg_txn_amount
    FROM dst_monthly_spending_summary mss
    WHERE mss.year * 100 + mss.month = (SELECT latest_period FROM latest_month)
    GROUP BY mss.person_name, mss.category_name, mss.category_group
)
SELECT 
    pd.person_name,
    pd.year,
    pd.month,
    
    -- Recommendation Priority (1 = highest)
    ROW_NUMBER() OVER (PARTITION BY pd.person_name ORDER BY 
        CASE 
            WHEN pd.discretionary_percent > 50 THEN 1
            WHEN pd.top_category = 'Shopping' THEN 2
            WHEN pd.top_category = 'Dining' THEN 3
            WHEN pd.weekend_spending_percent > 40 THEN 4
            ELSE 5
        END,
        cd.category_total DESC
    ) as priority,
    
    -- Recommendation Category
    CASE 
        WHEN pd.discretionary_percent > 50 THEN 'HIGH_PRIORITY'
        WHEN pd.discretionary_percent > 40 THEN 'MEDIUM_PRIORITY'
        ELSE 'LOW_PRIORITY'
    END as recommendation_priority,
    
    -- Recommendation Title
    CASE 
        WHEN cd.category_name = 'Shopping' AND cd.category_total > 500 
            THEN '🛍️ Reduce Shopping Spending'
        WHEN cd.category_name = 'Dining' AND cd.category_total > 300
            THEN '🍽️ Cook More at Home'
        WHEN cd.category_name = 'Entertainment' AND cd.category_total > 200
            THEN '🎬 Reduce Entertainment Costs'
        WHEN cd.category_name = 'Transport' AND cd.category_total > 250
            THEN '🚗 Optimize Transportation'
        WHEN cd.category_name = 'Food' AND cd.category_total > 400
            THEN '🥗 Plan Meals Better'
        ELSE '💡 Review ' || cd.category_name || ' Spending'
    END as recommendation_title,
    
    -- Detailed Recommendation
    CASE 
        WHEN cd.category_name = 'Shopping' AND cd.category_total > 500 
            THEN 'You spent $' || ROUND(cd.category_total, 2) || ' on shopping this month. Try limiting shopping trips to 2x per month and set a $300 budget. Potential savings: $' || ROUND(cd.category_total * 0.30, 2) || '/month.'
        WHEN cd.category_name = 'Dining' AND cd.category_total > 300
            THEN 'You spent $' || ROUND(cd.category_total, 2) || ' dining out (' || cd.category_txn_count || ' times). Cooking at home 3-4 more times per week could save $' || ROUND(cd.category_total * 0.40, 2) || '/month.'
        WHEN cd.category_name = 'Entertainment' AND cd.category_total > 200
            THEN 'Entertainment costs are $' || ROUND(cd.category_total, 2) || '/month. Consider free alternatives like parks, libraries, or home movie nights. Potential savings: $' || ROUND(cd.category_total * 0.35, 2) || '/month.'
        WHEN cd.category_name = 'Transport' AND cd.category_total > 250
            THEN 'Transportation costs: $' || ROUND(cd.category_total, 2) || '/month. Try carpooling, public transit, or combining trips. Could save $' || ROUND(cd.category_total * 0.25, 2) || '/month.'
        WHEN cd.category_name = 'Food' AND cd.category_total > 400
            THEN 'Food spending: $' || ROUND(cd.category_total, 2) || '/month. Meal planning and buying in bulk can reduce costs by 20-30%. Potential savings: $' || ROUND(cd.category_total * 0.25, 2) || '/month.'
        ELSE 'Review your ' || cd.category_name || ' spending ($' || ROUND(cd.category_total, 2) || '/month) for optimization opportunities.'
    END as recommendation_detail,
    
    -- Current Spending
    cd.category_name,
    cd.category_group,
    cd.category_total as current_monthly_spending,
    cd.category_txn_count as current_transaction_count,
    cd.avg_txn_amount as avg_transaction_amount,
    
    -- Target & Savings
    CASE 
        WHEN cd.category_name = 'Shopping' THEN ROUND(cd.category_total * 0.70, 2)
        WHEN cd.category_name = 'Dining' THEN ROUND(cd.category_total * 0.60, 2)
        WHEN cd.category_name = 'Entertainment' THEN ROUND(cd.category_total * 0.65, 2)
        WHEN cd.category_name = 'Transport' THEN ROUND(cd.category_total * 0.75, 2)
        WHEN cd.category_name = 'Food' THEN ROUND(cd.category_total * 0.75, 2)
        ELSE ROUND(cd.category_total * 0.85, 2)
    END as target_monthly_spending,
    
    CASE 
        WHEN cd.category_name = 'Shopping' THEN ROUND(cd.category_total * 0.30, 2)
        WHEN cd.category_name = 'Dining' THEN ROUND(cd.category_total * 0.40, 2)
        WHEN cd.category_name = 'Entertainment' THEN ROUND(cd.category_total * 0.35, 2)
        WHEN cd.category_name = 'Transport' THEN ROUND(cd.category_total * 0.25, 2)
        WHEN cd.category_name = 'Food' THEN ROUND(cd.category_total * 0.25, 2)
        ELSE ROUND(cd.category_total * 0.15, 2)
    END as potential_monthly_savings,
    
    -- Implementation Difficulty
    CASE 
        WHEN cd.category_name IN ('Shopping', 'Entertainment') THEN 'EASY'
        WHEN cd.category_name IN ('Dining', 'Food') THEN 'MODERATE'
        WHEN cd.category_name IN ('Transport', 'Healthcare') THEN 'HARD'
        ELSE 'MODERATE'
    END as implementation_difficulty
    
FROM person_data pd
JOIN category_data cd ON cd.person_name = pd.person_name
WHERE cd.category_group IN ('Discretionary', 'Transport', 'Other')
  AND cd.category_total > 100  -- Only recommend for significant spending
ORDER BY pd.person_name, priority;

COMMENT ON VIEW vw_spending_recommendations IS 
'Personalized cost-saving recommendations with specific actions, targets, and potential savings per person';

-- ============================================================================
-- VIEW 3: Budget Alerts
-- ============================================================================
-- Purpose: Identify overspending, unusual patterns, and budget violations
-- Grain: Multiple rows per person (one per alert)
-- Usage: SELECT * FROM vw_budget_alerts WHERE alert_severity = 'HIGH';
-- ============================================================================

CREATE OR REPLACE VIEW vw_budget_alerts AS
WITH latest_month AS (
    SELECT MAX(year * 100 + month) as latest_period
    FROM dst_person_analytics
),
person_current AS (
    SELECT * FROM dst_person_analytics
    WHERE year * 100 + month = (SELECT latest_period FROM latest_month)
),
category_current AS (
    SELECT 
        mss.person_name,
        mss.category_name,
        mss.category_group,
        SUM(mss.total_spending) as current_spending,
        AVG(mss.mom_percent_change) as mom_change
    FROM dst_monthly_spending_summary mss
    WHERE mss.year * 100 + mss.month = (SELECT latest_period FROM latest_month)
    GROUP BY mss.person_name, mss.category_name, mss.category_group
)
SELECT 
    pc.person_name,
    pc.year,
    pc.month,
    
    -- Alert Type
    CASE 
        WHEN pc.discretionary_percent > 60 THEN 'EXCESSIVE_DISCRETIONARY'
        WHEN pc.discretionary_percent > 50 THEN 'HIGH_DISCRETIONARY'
        WHEN pc.mom_percent_change > 30 THEN 'SPENDING_SPIKE'
        WHEN pc.mom_percent_change > 20 THEN 'SPENDING_INCREASE'
        WHEN cc.mom_change > 50 THEN 'CATEGORY_SPIKE'
        WHEN pc.top_category_percent > 50 THEN 'CONCENTRATION_RISK'
        ELSE 'GENERAL_WARNING'
    END as alert_type,
    
    -- Alert Severity
    CASE 
        WHEN pc.discretionary_percent > 60 OR pc.mom_percent_change > 30 THEN 'HIGH'
        WHEN pc.discretionary_percent > 50 OR pc.mom_percent_change > 20 THEN 'MEDIUM'
        ELSE 'LOW'
    END as alert_severity,
    
    -- Alert Title
    CASE 
        WHEN pc.discretionary_percent > 60 
            THEN '🚨 URGENT: Excessive Discretionary Spending (' || ROUND(pc.discretionary_percent, 1) || '%)'
        WHEN pc.discretionary_percent > 50 
            THEN '⚠️ WARNING: High Discretionary Spending (' || ROUND(pc.discretionary_percent, 1) || '%)'
        WHEN pc.mom_percent_change > 30 
            THEN '📈 ALERT: Major Spending Spike (+' || ROUND(pc.mom_percent_change, 1) || '%)'
        WHEN pc.mom_percent_change > 20 
            THEN '📊 NOTICE: Spending Increased (+' || ROUND(pc.mom_percent_change, 1) || '%)'
        WHEN cc.mom_change > 50 
            THEN '💥 SPIKE: ' || cc.category_name || ' spending surged (+' || ROUND(cc.mom_change, 1) || '%)'
        WHEN pc.top_category_percent > 50 
            THEN '🎯 RISK: ' || pc.top_category || ' dominates spending (' || ROUND(pc.top_category_percent, 1) || '%)'
        ELSE '💡 TIP: Review your spending patterns'
    END as alert_title,
    
    -- Alert Message
    CASE 
        WHEN pc.discretionary_percent > 60 
            THEN 'You spent ' || ROUND(pc.discretionary_percent, 1) || '% on discretionary items ($' || ROUND(pc.discretionary_spending, 2) || '). Target is <35%. Reduce by $' || ROUND(pc.discretionary_spending * (pc.discretionary_percent - 35) / 100, 2) || ' to reach healthy levels.'
        WHEN pc.discretionary_percent > 50 
            THEN 'Discretionary spending at ' || ROUND(pc.discretionary_percent, 1) || '% ($' || ROUND(pc.discretionary_spending, 2) || '). Consider reducing by 15% to improve financial health. Potential savings: $' || ROUND(pc.discretionary_spending * 0.15, 2) || '/month.'
        WHEN pc.mom_percent_change > 30 
            THEN 'Your spending jumped ' || ROUND(pc.mom_percent_change, 1) || '% this month (from $' || ROUND(pc.total_spending / (1 + pc.mom_percent_change/100), 2) || ' to $' || ROUND(pc.total_spending, 2) || '). Review recent purchases and identify one-time expenses.'
        WHEN pc.mom_percent_change > 20 
            THEN 'Spending increased ' || ROUND(pc.mom_percent_change, 1) || '% ($' || ROUND(pc.total_spending - pc.prev_month_total, 2) || ' more than last month). Monitor closely and adjust if trend continues.'
        WHEN cc.mom_change > 50 
            THEN cc.category_name || ' spending jumped ' || ROUND(cc.mom_change, 1) || '% to $' || ROUND(cc.current_spending, 2) || '. This category needs immediate attention. Set a monthly limit and track daily.'
        WHEN pc.top_category_percent > 50 
            THEN pc.top_category || ' represents ' || ROUND(pc.top_category_percent, 1) || '% of your spending ($' || ROUND(pc.top_category_spending, 2) || '). High concentration in one category increases financial risk. Diversify spending.'
        ELSE 'Review your spending patterns for optimization opportunities.'
    END as alert_message,
    
    -- Recommended Action
    CASE 
        WHEN pc.discretionary_percent > 60 THEN 'IMMEDIATE: Cut discretionary by 30%'
        WHEN pc.discretionary_percent > 50 THEN 'THIS WEEK: Reduce discretionary by 15%'
        WHEN pc.mom_percent_change > 30 THEN 'IMMEDIATE: Freeze non-essential purchases'
        WHEN pc.mom_percent_change > 20 THEN 'THIS WEEK: Review and reduce spending'
        WHEN cc.mom_change > 50 THEN 'IMMEDIATE: Set ' || cc.category_name || ' budget limit'
        WHEN pc.top_category_percent > 50 THEN 'THIS MONTH: Diversify spending categories'
        ELSE 'REVIEW: Monitor spending trends'
    END as recommended_action,
    
    -- Metrics
    pc.total_spending,
    pc.discretionary_spending,
    pc.discretionary_percent,
    pc.mom_percent_change,
    pc.top_category,
    pc.top_category_spending,
    cc.category_name as spike_category,
    cc.current_spending as spike_amount
    
FROM person_current pc
LEFT JOIN category_current cc ON cc.person_name = pc.person_name
WHERE 
    pc.discretionary_percent > 50
    OR pc.mom_percent_change > 20
    OR cc.mom_change > 50
    OR pc.top_category_percent > 50
ORDER BY 
    CASE 
        WHEN pc.discretionary_percent > 60 OR pc.mom_percent_change > 30 THEN 1
        WHEN pc.discretionary_percent > 50 OR pc.mom_percent_change > 20 THEN 2
        ELSE 3
    END,
    pc.person_name;

COMMENT ON VIEW vw_budget_alerts IS 
'Proactive alerts for overspending, unusual patterns, and budget violations with recommended actions';

-- ============================================================================
-- VIEW 4: Category Insights
-- ============================================================================
-- Purpose: Category-level opportunities and trends across all users
-- Grain: One row per category per month
-- Usage: SELECT * FROM vw_category_insights WHERE opportunity_score > 70;
-- ============================================================================

CREATE OR REPLACE VIEW vw_category_insights AS
WITH latest_month AS (
    SELECT MAX(year * 100 + month) as latest_period
    FROM dst_category_trends
)
SELECT 
    ct.year,
    ct.month,
    ct.category_name,
    ct.category_group,
    ct.total_spending,
    ct.transaction_count,
    ct.unique_persons,
    ct.avg_transaction_amount,
    
    -- Trends
    ct.mom_percent_change,
    ct.yoy_percent_change,
    ct.mom_trend_direction,
    ct.rolling_3month_avg,
    ct.rolling_6month_avg,
    
    -- Rankings
    ct.category_rank_current,
    ct.percent_of_total_spending,
    
    -- Opportunity Score (higher = more opportunity for savings)
    CASE 
        WHEN ct.category_group = 'Discretionary' THEN
            LEAST(100, (
                CASE WHEN ct.percent_of_total_spending > 30 THEN 40 ELSE ct.percent_of_total_spending END +
                CASE WHEN ct.mom_percent_change > 20 THEN 30
                     WHEN ct.mom_percent_change > 10 THEN 20
                     WHEN ct.mom_percent_change > 0 THEN 10
                     ELSE 0 END +
                CASE WHEN ct.total_spending > 10000 THEN 30
                     WHEN ct.total_spending > 5000 THEN 20
                     WHEN ct.total_spending > 2000 THEN 10
                     ELSE 0 END
            ))
        ELSE 0
    END as opportunity_score,
    
    -- Insight Summary
    CASE 
        WHEN ct.category_group = 'Discretionary' AND ct.percent_of_total_spending > 30 
            THEN '🎯 HIGH IMPACT: ' || ct.category_name || ' is ' || ROUND(ct.percent_of_total_spending, 1) || '% of total spending. Major savings opportunity!'
        WHEN ct.category_group = 'Discretionary' AND ct.mom_percent_change > 20
            THEN '📈 TRENDING UP: ' || ct.category_name || ' increased ' || ROUND(ct.mom_percent_change, 1) || '% MoM. Address before it becomes habit.'
        WHEN ct.category_group = 'Discretionary' AND ct.total_spending > 5000
            THEN '💰 BIG SPENDER: ' || ct.category_name || ' totals $' || ROUND(ct.total_spending, 2) || '/month. Review all transactions.'
        WHEN ct.category_group = 'Essential' AND ct.mom_percent_change > 30
            THEN '⚠️ ESSENTIAL SPIKE: ' || ct.category_name || ' up ' || ROUND(ct.mom_percent_change, 1) || '%. Investigate for wastage or price increases.'
        WHEN ct.category_rank_current <= 3
            THEN '🔝 TOP 3: ' || ct.category_name || ' is #' || ct.category_rank_current || ' category. Monitor closely.'
        ELSE '✅ STABLE: ' || ct.category_name || ' spending is controlled.'
    END as insight_summary,
    
    -- Recommended Action
    CASE 
        WHEN ct.category_group = 'Discretionary' AND ct.percent_of_total_spending > 30 
            THEN 'Set category budget at $' || ROUND(ct.total_spending * 0.70, 2) || '/month (30% reduction)'
        WHEN ct.category_group = 'Discretionary' AND ct.mom_percent_change > 20
            THEN 'Freeze spending in this category for 1 week'
        WHEN ct.category_group = 'Discretionary' AND ct.total_spending > 5000
            THEN 'Review each transaction and eliminate 25% of purchases'
        WHEN ct.category_group = 'Essential' AND ct.mom_percent_change > 30
            THEN 'Audit for wastage, compare prices, consider alternatives'
        ELSE 'Continue monitoring'
    END as recommended_action
    
FROM dst_category_trends ct
WHERE ct.year * 100 + ct.month = (SELECT latest_period FROM latest_month)
ORDER BY opportunity_score DESC, ct.total_spending DESC;

COMMENT ON VIEW vw_category_insights IS 
'Category-level insights with opportunity scores and recommended actions for cost optimization';

-- ============================================================================
-- VIEW 5: Lifestyle Improvement Plan
-- ============================================================================
-- Purpose: Comprehensive lifestyle change plan per person with prioritized actions
-- Grain: One row per person
-- Usage: SELECT * FROM vw_lifestyle_improvement_plan ORDER BY total_potential_savings DESC;
-- ============================================================================

CREATE OR REPLACE VIEW vw_lifestyle_improvement_plan AS
WITH latest_month AS (
    SELECT MAX(year * 100 + month) as latest_period
    FROM dst_person_analytics
),
person_summary AS (
    SELECT 
        pa.person_name,
        pa.year,
        pa.month,
        pa.total_spending,
        pa.essential_spending,
        pa.discretionary_spending,
        pa.essential_percent,
        pa.discretionary_percent,
        pa.essential_to_discretionary_ratio,
        pa.top_category,
        pa.top_category_spending,
        pa.top_category_percent,
        pa.weekend_spending_percent,
        pa.transaction_count,
        pa.unique_categories_count
    FROM dst_person_analytics pa
    WHERE pa.year * 100 + pa.month = (SELECT latest_period FROM latest_month)
),
category_totals AS (
    SELECT 
        mss.person_name,
        mss.category_name,
        mss.category_group,
        SUM(mss.total_spending) as cat_total
    FROM dst_monthly_spending_summary mss
    WHERE mss.year * 100 + mss.month = (SELECT latest_period FROM latest_month)
      AND mss.category_group = 'Discretionary'
    GROUP BY mss.person_name, mss.category_name, mss.category_group
),
top_categories AS (
    SELECT 
        person_name,
        STRING_AGG(category_name || ' ($' || ROUND(cat_total, 0) || ')', ', ' 
            ORDER BY cat_total DESC) as top_discretionary_categories
    FROM category_totals
    GROUP BY person_name
)
SELECT 
    ps.person_name,
    ps.year,
    ps.month,
    
    -- Current State
    ps.total_spending as current_monthly_spending,
    ps.discretionary_spending as current_discretionary_spending,
    ps.discretionary_percent as current_discretionary_percent,
    ps.essential_to_discretionary_ratio as current_ed_ratio,
    
    -- Financial Health Assessment
    CASE 
        WHEN ps.discretionary_percent < 25 AND ps.essential_percent BETWEEN 30 AND 40 THEN '🌟 EXCELLENT'
        WHEN ps.discretionary_percent < 35 AND ps.essential_percent BETWEEN 25 AND 45 THEN '✅ GOOD'
        WHEN ps.discretionary_percent < 45 THEN '⚡ FAIR'
        WHEN ps.discretionary_percent < 55 THEN '⚠️ POOR'
        ELSE '🚨 CRITICAL'
    END as financial_health_status,
    
    -- Target State (35% discretionary)
    ROUND(ps.total_spending * 0.35, 2) as target_discretionary_spending,
    ROUND(ps.discretionary_spending - (ps.total_spending * 0.35), 2) as excess_discretionary_spending,
    
    -- Savings Potential
    ROUND(GREATEST(0, ps.discretionary_spending - (ps.total_spending * 0.35)), 2) as monthly_savings_potential,
    ROUND(GREATEST(0, ps.discretionary_spending - (ps.total_spending * 0.35)) * 12, 2) as annual_savings_potential,
    
    -- Priority Actions (Top 3)
    CASE 
        WHEN ps.top_category_percent > 40 
            THEN '1️⃣ URGENT: Reduce ' || ps.top_category || ' spending by 30% (save $' || ROUND(ps.top_category_spending * 0.30, 2) || '/mo)'
        WHEN ps.discretionary_percent > 50
            THEN '1️⃣ URGENT: Cut all discretionary by 25% (save $' || ROUND(ps.discretionary_spending * 0.25, 2) || '/mo)'
        WHEN ps.weekend_spending_percent > 40
            THEN '1️⃣ HIGH: Reduce weekend spending (currently ' || ROUND(ps.weekend_spending_percent, 1) || '%)'
        ELSE '1️⃣ LOW: Continue monitoring spending'
    END as action_1_priority,
    
    CASE 
        WHEN ps.discretionary_percent > 50
            THEN '2️⃣ HIGH: Review top 3 categories: ' || tc.top_discretionary_categories
        WHEN ps.transaction_count > 100
            THEN '2️⃣ MEDIUM: Too many transactions (' || ps.transaction_count || '). Consolidate purchases.'
        WHEN ps.weekend_spending_percent > 35
            THEN '2️⃣ MEDIUM: Plan weekend activities better to control impulse spending'
        ELSE '2️⃣ LOW: Maintain current spending discipline'
    END as action_2_priority,
    
    CASE 
        WHEN ps.unique_categories_count > 8
            THEN '3️⃣ MEDIUM: Simplify spending - too diverse (' || ps.unique_categories_count || ' categories)'
        WHEN ps.essential_percent < 25
            THEN '3️⃣ MEDIUM: Increase essential spending proportion (currently ' || ROUND(ps.essential_percent, 1) || '%)'
        ELSE '3️⃣ LOW: Track weekly spending and review monthly'
    END as action_3_priority,
    
    -- Lifestyle Changes
    CASE 
        WHEN ps.top_category = 'Shopping' THEN '🛍️ Implement "24-hour rule" for purchases over $50'
        WHEN ps.top_category = 'Dining' THEN '🍳 Meal prep Sundays + pack lunch 3x/week'
        WHEN ps.top_category = 'Entertainment' THEN '🎬 Swap paid activities for free alternatives'
        WHEN ps.top_category = 'Transport' THEN '🚴 Walk/bike short distances + carpool when possible'
        ELSE '💡 Track every purchase for 30 days'
    END as lifestyle_change_1,
    
    CASE 
        WHEN ps.weekend_spending_percent > 40 THEN '📅 Plan weekend budget on Friday ($X limit)'
        WHEN ps.transaction_count > 100 THEN '🛒 Batch shopping trips (1x/week max)'
        ELSE '💰 Set weekly spending limits and stick to them'
    END as lifestyle_change_2,
    
    '📊 Review spending every Friday + adjust next week accordingly' as lifestyle_change_3,
    
    -- Timeline
    '30 days' as recommended_review_period,
    ROUND(GREATEST(0, ps.discretionary_spending - (ps.total_spending * 0.35)) / 4, 2) as week_1_savings_target,
    
    -- Top Problem Areas
    ps.top_category as biggest_expense_category,
    ps.top_category_spending as biggest_expense_amount,
    tc.top_discretionary_categories as discretionary_categories_list

FROM person_summary ps
LEFT JOIN top_categories tc ON tc.person_name = ps.person_name
ORDER BY monthly_savings_potential DESC;

COMMENT ON VIEW vw_lifestyle_improvement_plan IS 
'Comprehensive lifestyle improvement plan with prioritized actions, savings targets, and specific behavioral changes per person';

-- ============================================================================
-- SUMMARY
-- ============================================================================
-- Views Created:
--   ✅ vw_financial_health_scorecard   - Financial health scores and grades
--   ✅ vw_spending_recommendations     - Personalized cost-saving recommendations
--   ✅ vw_budget_alerts                - Proactive overspending alerts
--   ✅ vw_category_insights            - Category-level opportunities
--   ✅ vw_lifestyle_improvement_plan   - Comprehensive improvement plans
--
-- Key Features:
--   - Actionable insights (what to do, not just what happened)
--   - Personalized recommendations based on actual spending patterns
--   - Clear priority levels (HIGH/MEDIUM/LOW)
--   - Specific savings amounts and targets
--   - Behavioral change suggestions
--
-- Ultimate Goal Achieved:
--   ✅ "What lifestyle choices can improve my financial burden?"
--   → Each view provides specific answers with actionable steps
-- ============================================================================

-- Display success message
DO $$
BEGIN
    RAISE NOTICE '✅ DIS Layer views created successfully!';
    RAISE NOTICE '📊 5 analytical views ready for use';
    RAISE NOTICE '🎯 Financial insights and recommendations are now available!';
    RAISE NOTICE '';
    RAISE NOTICE 'Sample Queries:';
    RAISE NOTICE '  SELECT * FROM vw_financial_health_scorecard;';
    RAISE NOTICE '  SELECT * FROM vw_spending_recommendations WHERE person_name = ''John Tan'';';
    RAISE NOTICE '  SELECT * FROM vw_budget_alerts WHERE alert_severity = ''HIGH'';';
    RAISE NOTICE '  SELECT * FROM vw_category_insights WHERE opportunity_score > 50;';
    RAISE NOTICE '  SELECT * FROM vw_lifestyle_improvement_plan;';
END $$;

