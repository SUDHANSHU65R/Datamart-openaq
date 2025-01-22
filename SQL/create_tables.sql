-- Create or Replace Tables
CREATE OR REPLACE TABLE fact_air_quality (
    measurement_id STRING,
    city_id STRING,
    country_id STRING,
    location STRING,
    datetime TIMESTAMP,
    parameter STRING,
    value FLOAT,
    unit STRING
);

CREATE OR REPLACE TABLE dim_city (
    city_id STRING,
    city_name STRING,
    country_id STRING
);

CREATE OR REPLACE TABLE dim_country (
    country_id STRING,
    country_name STRING
);

CREATE OR REPLACE TABLE dim_date (
    date_id DATE,
    year INT,
    month INT,
    day INT,
    hour INT,
    weekday STRING
);

-- Create Stage for S3 Bucket
CREATE OR REPLACE STAGE openaq_s3_stage
URL='s3://sf-aws1-data-intrgration/Processed-fille-openaq'
STORAGE_INTEGRATION = s3_integration;

-- Load Data
COPY INTO fact_air_quality
FROM @openaq_s3_stage/fact_air_quality
FILE_FORMAT = (TYPE = 'PARQUET');

COPY INTO dim_city
FROM @openaq_s3_stage/dim_city
FILE_FORMAT = (TYPE = 'PARQUET');

COPY INTO dim_country
FROM @openaq_s3_stage/dim_country
FILE_FORMAT = (TYPE = 'PARQUET');

COPY INTO dim_date
FROM @openaq_s3_stage/dim_date
FILE_FORMAT = (TYPE = 'PARQUET');

-- 