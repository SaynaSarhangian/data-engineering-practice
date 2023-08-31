from zipfile import ZipFile
from pyspark.sql.functions import *
from pyspark.sql import functions as F


def read_zip(zip_path, spark):
    with ZipFile(zip_path, 'r') as zip:
        for csv_file in zip.namelist():
            # only consider the required csv file
            if csv_file.startswith('Divvy'):
                csv_data = zip.read(csv_file)
                print(
                    f'extract from .zip is in {type(csv_data)} format. converting to string....')  # this give the content of the file in byte array
                csv_data_str = str(csv_data,
                                   'utf-8')  # this converts the file content (byte string starting with b') to string using UTF-8 encoding
                file_lines = csv_data_str.splitlines()  # this turns the file content to a list of lines (list of strings)
                # Convert CSV data into a DataFrame
                csv_to_df = spark.sparkContext.parallelize(file_lines)  # this reads the lines into a RDD
                print(f'extract type is now {type(csv_to_df)}. converting to dataframe and returning it....')
                df = spark.read.csv(csv_to_df, header=True, inferSchema=True)
                return df


# add a converted date col to df1, give avg trip duration per day, return the result and transformed df1, export result to .csv
def transform1(df1, spark, output_path):
    # add a new col converted_start_time with date type from an existing col to df1
    df1 = df1.withColumn("converted_start_time", to_date(col("start_time"), "yyyy-MM-dd HH:mm:ss"))
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
    df_avg_tripduration = spark.sql(query)
    # export df_avg_tripduration to csv:
    # df_avg_tripduration.coalesce(1).write \
    #     .option("header", True) \
    #     .option("mode", "overwrite") \
    #     .csv(output_path)
    return df1, df_avg_tripduration


# How many trips (trip id) were taken per day?
def transform2(df1, spark, output_path):
    df_trip_count_per_day = df1.groupby('converted_start_time').agg({'trip_id': 'count'}).sort('converted_start_time')
    # export to .csv:
    # df_trip_count_per_day.coalesce(1).write \
    #     .option("header", True) \
    #     .option("mode", "overwrite") \
    #     .csv(output_path)
    return df_trip_count_per_day


# the most popular starting trip station for each month:
def transform3(df1, spark, output_path):
    df_pop_trip_station = df1.groupby('from_station_id', 'from_station_name').agg(
        count('trip_id').alias('count_trip_id')).sort(col('count_trip_id').desc())
    first_row = df_pop_trip_station.collect()[0]
    print(f'***********************The most popular station is {first_row[0]} with name {first_row[1]} and the max trips for it is {first_row[2]}!!!!!!!!!!!!!!!!!!!!!!!!!')
    return df_pop_trip_station
