import os, shutil
from zipfile import ZipFile
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
# from pyspark.sql import Row
from pyspark import SparkConf, SparkContext


def create_dir_check_exists(created_dir):
    base_dir = "D:/3-repos/data-engineering-practice/Exercises/Exercise-6/"
    if os.path.exists(created_dir):
        shutil.rmtree(created_dir)
    os.makedirs(created_dir)
    print(f'new directory {created_dir} was created!!')


def read_zip(zip_path, sc, spark):
    with ZipFile(zip_path, 'r') as zip:
        csv_file = zip.namelist()[0]  # 'Divvy_Trips_2019_Q4.csv'
        csv_file_bytes = zip.read(csv_file)
        # print(f'type of the read csv file: {type(csv_file_bytes)}')
        csv_file_str = str(csv_file_bytes,
                           'utf-8')  # this converts the file content (byte string starting with b') to string using UTF-8 encoding
        # print(f'type of converted csv file: {type(csv_file_str)}')
        csv_file_lines = csv_file_str.splitlines()  # (list of lines split by \n  num of lines:704056 including the header)
        rdd = sc.parallelize(csv_file_lines)  # passing a list to sc.parallelize()
        # Convert rdd into a spark DataFrame
        df1 = spark.read.csv(rdd, header=True, inferSchema=True)
        return df1


# add cols, calculate avg trip duration per day, return the result and transformed df1, export result to .csv
def transform1(df1, spark, created_dir):
    # add a new col converted_start_time with date type from an existing col to df1
    df1 = df1.withColumn("converted_start_time", to_date(col("start_time"), "yyyy-MM-dd HH:mm:ss"))
    # add age column
    df1 = df1.withColumn('age', (year(current_date()) - col('birthyear')))
    # change data type from string to int
    df1 = df1.withColumn('tripduration', df1.tripduration.cast('int'))
    df1 = df1.na.drop(subset=["birthyear", "tripduration"])

    # sparkSQL to calculate Question1:What is the average trip duration per day?
    df1.createOrReplaceTempView("df1_table")
    query = '''
SELECT \
    converted_start_time, round(avg(tripduration), 2) as avg_tripduration \
FROM df1_table \
group by \
    converted_start_time \
order by \
    converted_start_time 
'''
    # 1.What is the average trip duration per day?
    df_avg_tripduration = spark.sql(query)
    # export df_avg_tripduration to csv:
    df_avg_tripduration.coalesce(1).write\
        .mode('overwrite')\
        .option('header', True) \
        .csv(created_dir)
    return df1, df_avg_tripduration


# 2.How many trips (trip id) were taken per day?
def transform2(df1, spark, output_path):
    df_trip_count_per_day = df1.groupby('converted_start_time').agg({'trip_id': 'count'}).sort('converted_start_time')
    # export to .csv:
    # df_trip_count_per_day.coalesce(1).write \
    #     .option("header", True) \
    #     .option("mode", "overwrite") \
    #     .csv(output_path)
    return df_trip_count_per_day


# 3.the most popular starting station for each month:window func
def transform3(df1, spark, output_path):
    # extract year and month from date to group by them.
    df1 = df1.withColumn('year', year('converted_start_time'))
    df1 = df1.withColumn('month', month('converted_start_time'))

    df_pop_trip_station = df1.groupby('from_station_id', 'from_station_name', 'year',

                                      'month').agg(
        count('trip_id').alias('count_trip_id')).sort(col('count_trip_id').desc())
    # distinct_dates = df_pop_trip_station.select('year', 'month').distinct().collect()
    # print(f'distinct y,m are: {distinct_dates}')

    # to get the max for 'count_trip_id' we need to partition the data
    # for each year,month, order desc then get the first row of each
    window_agg = Window.partitionBy('year', 'month').orderBy(col('count_trip_id').desc())
    df_pop_trip_station = df_pop_trip_station.withColumn('row_number', row_number().over(window_agg)) \
        .withColumn('max', max(col('count_trip_id')).over(window_agg)) \
        .filter(col('row_number') == 1).drop('row_number')
    # print(f'num of rows for this df are: {df_pop_trip_station.count()}')  # 1783
    return df_pop_trip_station


# 4. What were the top 3 trip stations each day for the last two weeks?
def transform4(df1, spark, output_path):
    df_top3 = df1.groupby('from_station_id', 'from_station_name', 'converted_start_time').agg(
        count('trip_id').alias('count_trip_id')).sort(col('converted_start_time').desc())
    window_top3 = Window.partitionBy('converted_start_time').orderBy(
        col('count_trip_id').desc())
    df_top3_stations = df_top3.withColumn('row_number', row_number().over(window_top3)) \
        .withColumn('top3max', max(col('count_trip_id')).over(window_top3)) \
        .filter(col('row_number') <= 3).sort(col('converted_start_time').desc())
    return df_top3_stations


def transform5(df1, spark, output_path):
    # 5.Do Males or Females take longer trips on average?
    df_gender_avgduration = df1.groupby('gender').agg(round(avg('tripduration'), 2).alias('avg_tripduration'))
    return df_gender_avgduration


# 6.What is the top 10 ages of those that take the longest trips, and shortest?
def transform6(df1, spark, output_path):
    window_top10_ages = Window.partitionBy('age').orderBy(col('tripduration').desc())
    df_top10_ages = df1.groupby('age').agg(max('tripduration').alias('max_tripduration')).orderBy(
        col('max_tripduration').desc())
    return df_top10_ages
