from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.functions import sum
import math
import pandas as pd
import argparse
import pyspark
from pyspark.sql.window import Window

import openpyxl


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Spark Job for Bike Share Data Analysis')
    parser.add_argument('--hdfs_input_path',
                        help='HDFS input directory', required=True, type=str)
    parser.add_argument(
        '--year', help='Partion Year To Process, e.g. 2024', required=True, type=str)
    parser.add_argument(
        '--month', help='Partion Month To Process, e.g. 11', required=True, type=str)
    parser.add_argument(
        '--day', help='Day of File To Process, e.g. 23', required=True, type=str)
    return parser.parse_args()


def execute_pipeline():

    # Parse command line Arguments
    params = parse_arguments()

    # Initialize Spark session
    spark_ctx = pyspark.SparkContext()
    spark_session = SparkSession(spark_ctx)

    # Load bike share data
    bike_trips_df = spark_session.read.format('csv').options(
        header='true',
        delimiter=',',
        inferschema='true',
        nullValue="\\N"
    ).load(params.hdfs_input_path + '/hubway_trips/' + params.year + '/' + params.month + '/hubway_' + params.year + '-' + params.month + '-' + params.day + '.csv')

    # Ensure numerical columns have appropriate data types
    bike_trips_df = bike_trips_df.withColumn(
        "tripduration", col("tripduration").cast("int"))
    bike_trips_df = bike_trips_df.withColumn(
        "start station latitude", col("start station latitude").cast("float"))
    bike_trips_df = bike_trips_df.withColumn(
        "start station longitude", col("start station longitude").cast("float"))
    bike_trips_df = bike_trips_df.withColumn(
        "end station latitude", col("end station latitude").cast("float"))
    bike_trips_df = bike_trips_df.withColumn(
        "end station longitude", col("end station longitude").cast("float"))

    # 1. Calculate Average Trip Duration
    bike_trips_df = bike_trips_df.withColumn(
        "starttime", col("starttime").cast('timestamp'))

    bike_trips_df = bike_trips_df.withColumn('year', year(col('starttime')))
    bike_trips_df = bike_trips_df.withColumn('month', month(col('starttime')))

    avg_duration_monthly = bike_trips_df.groupBy('year', 'month').agg(
        avg('tripduration').alias("avg_tripduration"))
    avg_duration_sorted = avg_duration_monthly.orderBy('year', 'month')
    avg_duration_sorted = avg_duration_sorted.withColumn(
        "avg_tripduration", round(avg_duration_sorted["avg_tripduration"], 1))
    avg_duration_sorted.show()

    # 2. Compute Average Trip Distance
    bike_trips_with_distance = bike_trips_df.withColumn("distance_km", sqrt((col("end station latitude") - col("start station latitude")) ** 2 + (
        # Scale to kilometers
        (col("end station longitude") - col("start station longitude")) * cos(radians(col("start station latitude")))) ** 2) * 111)
    avg_distance_monthly = bike_trips_with_distance.groupBy(
        "month").agg(avg("distance_km").alias("avg_trip_distance_km"))
    avg_distance_monthly = avg_distance_monthly.orderBy("month")
    avg_distance_monthly.show()

    # 3. Usage share by gender (in percent)
    bike_trips_df = bike_trips_df.withColumn(
        "gender", col("gender").cast("int"))
    gender_counts_monthly = bike_trips_df.filter(
        bike_trips_df["gender"] != 2)  # 2 = undefined gender
    gender_counts_monthly = gender_counts_monthly.groupBy(
        "month", "gender").agg(count("*").alias("gender_count_month"))
    total_gender_monthly = gender_counts_monthly.groupBy("month").agg(
        sum("gender_count_month").alias("total_month_gender_count"))
    gender_usage_share = gender_counts_monthly.join(total_gender_monthly, on="month").withColumn("usage_share", round(
        (col("gender_count_month") / col("total_month_gender_count")) * 100, 2)).select("month", "gender", "usage_share")
    gender_usage_share = gender_usage_share.withColumn(
        "gender", when(col("gender") == 0, "Female").otherwise("Male"))
    gender_usage_share = gender_usage_share.orderBy(
        col("month"), col("gender"))
    gender_usage_share.show()

    # Skipped the rest.

    # CREATE KPI EXCEL OUT OF COMPUTED DataFrames

    avg_duration_sorted_pd = avg_duration_sorted.toPandas()
    avg_distance_monthly_pd = avg_distance_monthly.toPandas()
    gender_usage_share_pd = gender_usage_share.toPandas()

    with pd.ExcelWriter("/home/airflow/airflow/python/bike_share_kpi_result.xlsx", engine="openpyxl") as writer:
        avg_duration_sorted_pd.to_excel(
            writer, sheet_name="Average Trip Duration", index=False)
        avg_distance_monthly_pd.to_excel(
            writer, sheet_name="Average Trip Distance", index=False)
        gender_usage_share_pd.to_excel(
            writer, sheet_name="Gender Usage Share", index=False)

    print("xlsx file created")


if __name__ == "__main__":
    execute_pipeline()
