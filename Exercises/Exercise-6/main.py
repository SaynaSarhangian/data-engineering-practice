from pyspark.sql import SparkSession
import os
import sys
from spark_util import read_zip, transform1, transform3

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
    # df_trip_count_per_day = transform2(df1, spark, output_path)
    df_pop_trip_station= transform3(df1, spark, output_path)
    df_pop_trip_station.show()



if __name__ == "__main__":
    main()
    # there's no straightforward way to read .zip files directly in pyspark.
    # df.write.csv(path) in Spark creates a folder with the name specified in the path and writes data as multiple parts(partitions).
    # we only need a single csv as export, so used df.coalesce(1).write()
    # 1.to run Apache Spark locally, winutils.exe is required for Windows: installed pyspark and winutils.exe
    # 2.set env variables
    # 3.read one .csv file from each .zip file and load onto df
    # 4.give the path to func, it reads the .csv file in .zip file, converts to df and returns df.
    # Q1.convert  start_time column to date type, group by date, calculate avg duration per day

    # print(df1.columns)
    # ['trip_id', 'start_time', 'end_time', 'bikeid', 'tripduration', 'from_station_id', 'from_station_name', 'to_station_id', 'to_station_name', 'usertype', 'gender', 'birthyear']

    # print(df2.columns)
    # ['ride_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_name', 'start_station_id', 'end_station_name', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'member_casual']

    # df1 and df2 have start_station_name|start_station_id|end_station_name|end_station_id in common.
