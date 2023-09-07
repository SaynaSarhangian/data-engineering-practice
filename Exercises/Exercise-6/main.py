from pyspark.sql import SparkSession
import os
import sys
from spark_util import read_zip, transform1, transform3, transform4, transform5, transform6
from pyspark.sql.functions import *
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    path = 'data'
    # .csv export location
    output_path = "reports"
    df1 = read_zip('data/Divvy_Trips_2019_Q4.zip', spark)
    # df2 = read_zip('data/Divvy_Trips_2020_Q1.zip', spark)

    df1, df_avg_tripduration = transform1(df1, spark, output_path)
    # print(f'1.What is the average trip duration per day?:\n')
    # df_avg_tripduration.show(50)
    # df_trip_count_per_day = transform2(df1, spark, output_path)
    # df_pop_trip_station = transform3(df1, spark, output_path)
    # ----------------------------------------------------------------------------------------

    # df_top3_stations = transform4(df1, spark, output_path)
    # print(f'4. What were the top 3 trip stations each day for the last two weeks?\n')
    # df_top3_stations.show(60)

    # df_gender_avgduration = transform5(df1, spark, output_path)

    df_top10_ages = transform6(df1, spark, output_path)
    # print(f'distinct count of age col: {df1.select("age").distinct().count()}')
    # print summary statistics of 2 cols:
    print(f"Summary for df1: {df1.select('tripduration', 'age').summary().show()}")

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

