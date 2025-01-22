-- Cities with Average Monthly Levels of CO and SO2 in the 90th Percentile Globally
WITH monthly_averages AS (
    SELECT
        city_id,
        month,
        AVG(CASE WHEN parameter = 'co' THEN value END) AS avg_co,
        AVG(CASE WHEN parameter = 'so2' THEN value END) AS avg_so2
    FROM fact_air_quality
    JOIN dim_date ON DATE_TRUNC('month', fact_air_quality.datetime) = DATE_TRUNC('month', dim_date.datetime)
    WHERE parameter IN ('co', 'so2')
    GROUP BY city_id, month
)
SELECT
    city_id,
    avg_co,
    avg_so2
FROM (
    SELECT
        city_id,
        avg_co,
        avg_so2,
        NTILE(100) OVER (ORDER BY avg_co DESC) AS co_percentile,
        NTILE(100) OVER (ORDER BY avg_so2 DESC) AS so2_percentile
    FROM monthly_averages
)
WHERE co_percentile >= 90 AND so2_percentile >= 90;


-- Top 5 Cities with Highest Daily Average Levels of PM2.5
SELECT
    city_id,
    date_trunc('day', datetime) AS date,
    AVG(value) AS avg_pm25
FROM fact_air_quality
WHERE parameter = 'pm25'
GROUP BY city_id, date
ORDER BY avg_pm25 DESC
LIMIT 5;

-- Top 10 Cities with Highest Hourly Average Levels of PM2.5 and CO & SO2 Statistics

WITH hourly_pm25 AS (
    SELECT
        city_id,
        date_trunc('hour', datetime) AS hour,
        AVG(value) AS avg_pm25
    FROM fact_air_quality
    WHERE parameter = 'pm25'
    GROUP BY city_id, hour
)
SELECT
    h.city_id,
    h.hour,
    h.avg_pm25,
    co_stats.mean_co,
    co_stats.median_co,
    co_stats.mode_co,
    so2_stats.mean_so2,
    so2_stats.median_so2,
    so2_stats.mode_so2
FROM (
    SELECT city_id, hour, avg_pm25
    FROM hourly_pm25
    ORDER BY avg_pm25 DESC
    LIMIT 10
) h
LEFT JOIN (
    SELECT
        city_id,
        date_trunc('hour', datetime) AS hour,
        AVG(value) AS mean_co,
        MEDIAN(value) AS median_co,
        MODE() WITHIN GROUP (ORDER BY value) AS mode_co
    FROM fact_air_quality
    WHERE parameter = 'co'
    GROUP BY city_id, hour
) co_stats ON h.city_id = co_stats.city_id AND h.hour = co_stats.hour
LEFT JOIN (
    SELECT
        city_id,
        date_trunc('hour', datetime) AS hour,
        AVG(value) AS mean_so2,
        MEDIAN(value) AS median_so2,
        MODE() WITHIN GROUP (ORDER BY value) AS mode_so2
    FROM fact_air_quality
    WHERE parameter = 'so2'
    GROUP BY city_id, hour
) so2_stats ON h.city_id = so2_stats.city_id AND h.hour = so2_stats.hour;


-- Air Quality Index for Each Country

WITH country_aqi AS (
    SELECT
        country_id,
        date_trunc('hour', datetime) AS hour,
        AVG(CASE WHEN parameter = 'pm25' THEN value END) AS avg_pm25,
        AVG(CASE WHEN parameter = 'so2' THEN value END) AS avg_so2,
        AVG(CASE WHEN parameter = 'co' THEN value END) AS avg_co
    FROM fact_air_quality
    WHERE parameter IN ('pm25', 'so2', 'co')
    GROUP BY country_id, hour
)
SELECT
    country_id,
    hour,
    CASE
        WHEN (avg_pm25 > 150 OR avg_so2 > 75 OR avg_co > 15) THEN 'High'
        WHEN (avg_pm25 BETWEEN 55 AND 150 OR avg_so2 BETWEEN 35 AND 75 OR avg_co BETWEEN 7 AND 15) THEN 'Moderate'
        ELSE 'Low'
    END AS air_quality_index
FROM country_aqi;


