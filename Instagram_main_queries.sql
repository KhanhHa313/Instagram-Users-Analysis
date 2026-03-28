--Create dashboard view for later import to csv
CREATE OR REPLACE VIEW dashboard_vieww AS (

--Convert all engagement metrics to daily base
WITH daily_metrics AS (
    SELECT 
        user_id,
        posts_created_per_week / 7.0 AS daily_posts,
        comments_written_per_day AS daily_comments,
        dms_sent_per_week / 7.0 AS daily_dms,
        likes_given_per_day AS daily_likes,
        stories_viewed_per_day AS daily_stories,
        reels_watched_per_day AS daily_reels
    FROM instagram
),

--Normalise the engagement metrics which were originally on different units format, with max
--units of each activity is the base (denumerator)
normalised_metrics AS (
    SELECT
        user_id,
        daily_posts / NULLIF(MAX(daily_posts) OVER (), 0) AS norm_daily_posts,
        daily_comments / NULLIF(MAX(daily_comments) OVER (), 0) AS norm_daily_comments,
        daily_dms / NULLIF(MAX(daily_dms) OVER (), 0) AS norm_daily_dms,
        daily_likes / NULLIF(MAX(daily_likes) OVER (), 0) AS norm_daily_likes,
        daily_stories / NULLIF(MAX(daily_stories) OVER (), 0) AS norm_daily_stories,
        daily_reels / NULLIF(MAX(daily_reels) OVER (), 0) AS norm_daily_reels
    FROM daily_metrics
),

--Find the engagement score by giving different weight to each user activity 
engagement_score_cte AS (
    SELECT 
        user_id,
        (0.4 * norm_daily_posts + 0.3 * norm_daily_comments + 0.3 * norm_daily_dms) AS active_score,
        (0.5 * norm_daily_likes + 0.3 * norm_daily_stories + 0.2 * norm_daily_reels) AS reactive_score,
        (0.4 * norm_daily_posts + 0.3 * norm_daily_comments + 0.3 * norm_daily_dms) + 
        (0.5 * norm_daily_likes + 0.3 * norm_daily_stories + 0.2 * norm_daily_reels) as total_engagement_score
    FROM normalised_metrics
),
 
 
--Segment the users into groups according to their account sign up year and Instagram growth phase
segmentation_cte AS (
    SELECT 
        user_id, subscription_status, account_creation_year, country,
        CASE
            WHEN account_creation_year BETWEEN 2010 AND 2012 THEN '2010–2012 Early adopters'
            WHEN account_creation_year BETWEEN 2013 AND 2015 THEN '2013–2015 Growth'
            WHEN account_creation_year BETWEEN 2016 AND 2018 THEN '2016–2018 Expansion'
            WHEN account_creation_year BETWEEN 2019 AND 2021 THEN '2019–2021 Monetization'
            WHEN account_creation_year >= 2022 THEN '2022–2025 Mature'
        END AS registration_cohort,
        

        EXTRACT(YEAR FROM NOW()) - account_creation_year AS tenure_years,
        CASE
            WHEN EXTRACT(YEAR FROM NOW()) - account_creation_year < 2 THEN '< 2 years'
            WHEN EXTRACT(YEAR FROM NOW()) - account_creation_year BETWEEN 2 AND 4 THEN '2–4 years'
            WHEN EXTRACT(YEAR FROM NOW()) - account_creation_year BETWEEN 5 AND 7 THEN '5–7 years'
            WHEN EXTRACT(YEAR FROM NOW()) - account_creation_year BETWEEN 8 AND 10 THEN '8–10 years'
            ELSE '10+ years'
        END AS tenure_year_group,

        daily_active_minutes_instagram / 60.0 AS daily_active_hours,

        CASE
            WHEN daily_active_minutes_instagram / 60.0 < 1 THEN '<1 hour'
            WHEN daily_active_minutes_instagram / 60.0 BETWEEN 1 AND 2 THEN '1–2 hours'
            WHEN daily_active_minutes_instagram / 60.0 BETWEEN 2 AND 4 THEN '2–4 hours'
            WHEN daily_active_minutes_instagram / 60.0 BETWEEN 4 AND 6 THEN '4–6 hours'
            WHEN daily_active_minutes_instagram / 60.0 >= 6 THEN '6+ hours'
        END AS daily_active_hours_group,

        CASE
            WHEN age < 27 THEN 'Gen Z'
            WHEN age BETWEEN 27 AND 42 THEN 'Millennials'
            WHEN age BETWEEN 43 AND 58 THEN 'Gen X'
            ELSE 'Boomers'
        END AS generation

    FROM instagram
),

--Find the last day of login with 2026-01-01 as the base date
--As different tenure users have different inactivity phase and all cannot be treated as the same 
--Normalisation is once again used 
inactivity AS (
    SELECT 
        s.user_id,
        s.registration_cohort,
        s.tenure_year_group,
        s.daily_active_hours_group,
        '2026-01-01'::DATE - i.last_login_date AS days_since_last_login,
        ('2026-01-01'::DATE  - i.last_login_date ):: NUMERIC/ NULLIF(s.tenure_years*365,0) AS inactivity_by_tenure
    FROM instagram i
    JOIN segmentation_cte s USING (user_id)
),


--Divide the users into bucket and label them
inactivity_groups AS (
    SELECT 
        MIN(inactivity_by_tenure) AS min_val,
        MAX(inactivity_by_tenure) AS max_val
    FROM inactivity
),

retention_cte as (
    SELECT 
        i.user_id,
        i.registration_cohort,
        i.tenure_year_group,
        i.daily_active_hours_group,
        i.days_since_last_login,
        i.inactivity_by_tenure,
        CASE WIDTH_BUCKET(i.inactivity_by_tenure, g.min_val, g.max_val, 4)
            WHEN 1 THEN ' Recently active'
            WHEN 2 THEN 'Active'
            WHEN 3 THEN 'At risk'
            ELSE 'Likely churned'
        END AS status
    FROM inactivity i
    JOIN inactivity_groups g ON TRUE 
) 
--Combine every ctes created for the dashboard view
   SELECT 
        e.user_id, s.generation, s.country, s.subscription_status, 
        s.registration_cohort, s.tenure_years, s.tenure_year_group, 
        ROUND (e.active_score, 5) as active_score, 
        ROUND (e.reactive_score, 5) as reactive_score, 
        ROUND (e.total_engagement_score,5) as total_engagement_score,
        s.daily_active_hours_group,
        r.days_since_last_login,
        ROUND (r.inactivity_by_tenure, 5) as inactivity_by_tenure,
        r.status
        FROM engagement_score_cte e
        JOIN segmentation_cte s
        USING (user_id)
        JOIN retention_cte  r
        USING (user_id) 
)

----------- RUN IN TERMINAL -----------
-- Import the view to the csv file for the visualisation
-- \copy (SELECT *  FROM dashboard_vieww ORDER BY RANDOM () LIMIT 5000 ) to '/Users/sarahnguyen313/Downloads/dashboard_data_latest.csv' CSV HEADER

-- Import shrinked raw data due to file size for Github
-- \copy (SELECT *  FROM instagram ORDER BY RANDOM () LIMIT 5000 ) to '/Users/sarahnguyen313/Downloads/instagram_raw_sample.csv' CSV HEADER




 