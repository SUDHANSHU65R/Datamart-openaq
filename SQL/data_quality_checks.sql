-- Data Quality Checks for Air Quality Data Mart

-- Check for null values in key columns
SELECT 
    COUNT(*) AS null_count 
FROM 
    air_quality_measurements 
WHERE 
    city IS NULL OR 
    country IS NULL OR 
    date IS NULL OR 
    co IS NULL OR 
    so2 IS NULL OR 
    pm25 IS NULL;

-- Check for negative values in pollution measurements
SELECT 
    COUNT(*) AS negative_count 
FROM 
    air_quality_measurements 
WHERE 
    co < 0 OR 
    so2 < 0 OR 
    pm25 < 0;

-- Check for duplicate entries based on unique constraints (city, date, and measurement type)
SELECT 
    city, 
    date, 
    COUNT(*) AS duplicate_count 
FROM 
    air_quality_measurements 
GROUP BY 
    city, 
    date 
HAVING 
    COUNT(*) > 1;

-- Generate a report of invalid data entries
SELECT 
    city, 
    country, 
    date, 
    co, 
    so2, 
    pm25 
FROM 
    air_quality_measurements 
WHERE 
    city IS NULL OR 
    country IS NULL OR 
    date IS NULL OR 
    co IS NULL OR 
    so2 IS NULL OR 
    pm25 IS NULL OR 
    co < 0 OR 
    so2 < 0 OR 
    pm25 < 0;