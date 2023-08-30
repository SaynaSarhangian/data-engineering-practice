from pyspark.sql import SparkSession
import os
import sys
from spark_util import read_zip, transform1

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    path = 'data'
    output_path = "reports"

    df1 = read_zip('data/Divvy_Trips_2019_Q4.zip', spark)
    #df2 = read_zip(path, z_list[1], spark)

    df_avg_tripduration = transform1(df1, spark)
    print('******Showing a preview of the transformed dataframe:...')
    df_avg_tripduration.show(25)
    print('Count of rows:', df_avg_tripduration.count())

    df_avg_tripduration.coalesce(1).write \
        .option("header", True) \
        .option("mode", "overwrite") \
        .csv(output_path)

    #print(df1.trip_id.nunique())
    #print(df1.distinct().count())
    # df_avg_tripduration.printSchema()



if __name__ == "__main__":
    main()
    #Spark creates a folder with the name specified in the path and writes data as multiple parts(partitions)
    # 1.to run Apache Spark locally, winutils.exe is required for Windows: installed pyspark and winutils.exe
    # 2.set env variables
    # 3.read one .csv file from each .zip file and load onto df
    # 4.give the path to func, it reads the .csv file in .zip file, converts to df and returns df.
    # Q1.convert  start_time column to date type, group by date, calculate avg duration per day

    # print(df1.columns)
    # ["b'trip_id", 'start_time', 'end_time', 'bikeid', 'tripduration', 'from_station_id', 'from_station_name', 'to_station_id', 'to_station_name', 'usertype', 'gender', "birthyear'"]

    # print(df2.columns)
    # ["b'ride_id", 'rideable_type', 'started_at', 'ended_at', 'start_station_name', 'start_station_id', 'end_station_name', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng', "member_casual'"]
