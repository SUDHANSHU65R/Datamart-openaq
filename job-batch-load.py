# %%
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import *
import boto3
from pyspark.sql.window import Window


# Initialize Spark session
def init_spark_session():
    """
    Initializes and returns a SparkSession with necessary AWS configurations.
    """
    key = os.environ.get("aws_access_key")
    secret = os.environ.get("aws_secret_key")

    spark = (
        SparkSession.builder.master("yarn")
        .appName("datamart-process")
        .config("spark.dynamicAllocation.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext._jsc.hadoopConfiguration().set(
        "com.amazonaws.services.s3.enableV4", "true"
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A"
    )
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret)

    return spark


# Read data from S3
def read_data_from_s3(spark, s3_data_path):
    """
    Reads data from the specified S3 path into a DataFrame.
    """
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(s3_data_path)
    )


# Clean data by removing null or negative values in the 'value' column
def clean_data(df):
    """
    Cleans the DataFrame by removing rows with null or negative 'value'.
    """
    return df.filter((df["value"].isNotNull()) & (df["value"] >= 0))


# Filter the data for specific parameters
def filter_parameters(df, valid_parameters):
    """
    Filters the DataFrame based on valid parameters (e.g., "co", "so2", "pm25").
    """
    return df.filter(df["parameter"].isin(valid_parameters))


# Handle duplicates in the data
def remove_duplicates(df):
    """
    Removes duplicates based on 'location', 'datetime', and 'parameter'.
    """
    return df.dropDuplicates(["location", "datetime", "parameter"])


# Generate a data quality report for bad data
def generate_data_quality_report(df, valid_parameters):
    """
    Generates a report for bad data where 'value' is null or negative,
    or 'parameter' is not in the valid parameters.
    """
    bad_data_df = df.filter(
        (df["value"].isNull())
        | (df["value"] < 0)
        | (~df["parameter"].isin(valid_parameters))
    )
    bad_data_df.write.mode("overwrite").csv(
        "s3a://sf-aws1-data-intrgration/Processed-fille-openaq/data-quality-reports/2017_bad_data.csv"
    )


# Extract datetime components for dimension tables
def extract_datetime_components(df):
    """
    Extracts various datetime components like 'year', 'month', 'day', 'hour', etc.
    """
    return (
        df.withColumn("date_id", to_date("datetime"))
        .withColumn("year", year("datetime"))
        .withColumn("month", month("datetime"))
        .withColumn("day", dayofmonth("datetime"))
        .withColumn("hour", hour("datetime"))
        .withColumn("weekday", date_format("datetime", "EEEE"))
        .withColumn("city_id", split(col("location"), "-")[2])
        .withColumn("city", split(col("location"), "-")[0])
        .withColumn("country", split(col("location"), "-")[1])
    )


# Prepare Fact Table
def prepare_fact_table(df_clean):
    """
    Prepares the Fact table with necessary columns.
    """
    return df_clean.select(
        monotonically_increasing_id().alias("measurement_id"),
        col("city").alias("city_id"),
        col("country").alias("country_id"),
        "location",
        "datetime",
        "parameter",
        "value",
        "units",
    )


# Prepare Dimension Tables
def prepare_dimension_tables(df_clean):
    """
    Prepares the Dimension tables for city, country, and date.
    """
    dim_city = df_clean.select(
        col("city_id"), "city", col("country").alias("country_id")
    ).distinct()

    dim_country = df_clean.select(
        col("country").alias("country_id"), "country"
    ).distinct()

    dim_date = df_clean.select(
        "date_id", "year", "month", "day", "hour", "weekday"
    ).distinct()

    return dim_city, dim_country, dim_date


# Save data to Parquet files on S3
def save_to_parquet(df, s3_path):
    """
    Saves the DataFrame to the specified S3 path as Parquet files.
    """
    df.write.mode("overwrite").parquet(s3_path)


# %% Main Execution


def main():
    # Initialize Spark session
    spark = init_spark_session()

    # Bucket Path
    s3_data_path = r"s3a://openaq-data-archive/records/csv.gz/locationid=*/year=2017/month=03/location-100-2017*.csv.gz"

    # Read data from S3
    df = read_data_from_s3(spark, s3_data_path)

    # Clean the data
    df_clean = clean_data(df)

    # Filter valid parameters
    valid_parameters = ["co", "so2", "pm25"]
    df_clean = filter_parameters(df_clean, valid_parameters)

    # Handle duplicates
    df_clean = remove_duplicates(df_clean)

    # Generate data quality report
    generate_data_quality_report(df, valid_parameters)

    # Extract datetime components for dimension tables
    df_clean = extract_datetime_components(df_clean)

    # Prepare Fact Table
    fact_air_quality = prepare_fact_table(df_clean)

    # Prepare Dimension Tables
    dim_city, dim_country, dim_date = prepare_dimension_tables(df_clean)

    # Write DataFrames to Parquet files on S3
    save_to_parquet(
        fact_air_quality,
        "s3a://sf-aws1-data-intrgration/Processed-fille-openaq/fact_air_quality",
    )
    save_to_parquet(
        dim_city, "s3a://sf-aws1-data-intrgration/Processed-fille-openaq/dim_city"
    )
    save_to_parquet(
        dim_country, "s3a://sf-aws1-data-intrgration/Processed-fille-openaq/dim_country"
    )
    save_to_parquet(
        dim_date, "s3a://sf-aws1-data-intrgration/Processed-fille-openaq/dim_date"
    )


if __name__ == "__main__":
    main()
