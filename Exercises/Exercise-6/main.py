from pyspark.sql import SparkSession
import os
import sys
from spark_util import create_dir_check_exists, read_zip, transform1, transform2, transform3, transform4, transform5, transform6
from pyspark.sql.functions import *
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    path = 'data'
    # .csv export location
    created_dir = "reports"
    create_dir_check_exists(created_dir)

    df1 = read_zip('data/Divvy_Trips_2019_Q4.zip', sc, spark)
    #df2 = read_zip('data/Divvy_Trips_2020_Q1.zip', sc, spark)

    df1, df_avg_tripduration = transform1(df1, spark, created_dir)
    print(f'*************************** here is the transformed df1 with new cols and converted dates:')
    df1.show()
    print(f'*************************** What is the average trip duration per day?')
    df_avg_tripduration.show(95)

    print(f'***************************What is the average trip duration per day?:\n')
    df_avg_tripduration.show(50)

    print(f'***************************How many trips (trip id) were taken per day?')
    df_trip_count_per_day = transform2(df1, spark, created_dir)
    df_trip_count_per_day.show(60)

    print(f'***************************the most popular starting trip station for each month?')
    df_pop_trip_station = transform3(df1, spark, created_dir)
    df_pop_trip_station.show()
    # ----------------------------------------------------------------------------------------
    print(f'*************************What were the top 3 trip stations each day for the last two weeks?')
    df_top3_stations = transform4(df1, spark, created_dir)
    print(f'4. What were the top 3 trip stations each day for the last two weeks?\n')
    df_top3_stations.show(60)

    df_gender_avgduration = transform5(df1, spark, created_dir)
    print(f'**************************Do Males or Females take longer trips on average? look below:')
    df_gender_avgduration.show()
    print(f'****************************What is the top 10 ages of those that take the longest trips, and shortest?')
    df_top10_ages = transform6(df1, spark, created_dir)
    df_top10_ages.show()

    # print(f'distinct count of age col: {df1.select("age").distinct().count()}')
    # print summary statistics of 2 cols:

    # print(f"Summary for df1: {df1.select('tripduration', 'age').summary().show()}")


if __name__ == "__main__":
    main()

    # there's no straightforward way to read .zip files directly in pyspark.
    # df.write.csv(path) in Spark creates a folder with the name specified in the path and writes data as multiple parts(partitions).
    # we only need a single csv as export, so used df.coalesce(1).write()
    # 1.to run Apache Spark locally, winutils.exe is required for Windows: installed pyspark and winutils.exe but not working
    # 2.set env variables
    # 3.read one .csv file from each .zip file and load onto df
    # 4.give the path to func, it reads the .csv file in .zip file, converts to df and returns df.
    # Q1.convert  start_time column to date type, group by date, calculate avg duration per day
