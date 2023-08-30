from zipfile import ZipFile
import os
import io
import zipfile
from pyspark.sql.functions import to_date, col


def read_zip(zip_path, spark):
    with ZipFile(zip_path, 'r') as zip:
        for csv_file in zip.namelist():
            # only consider the required csv file
            if csv_file.startswith('Divvy'):
                csv_data = zip.read(csv_file)  # this give the content of the file in byte array
                csv_data_str = str(csv_data, 'utf-8')  # this converts the file content from byte array to string
                file_lines = csv_data_str.splitlines()  # this turns the file content to a list of lines (list of strings)
                # Convert CSV data into a DataFrame
                csv_to_df = spark.sparkContext.parallelize(file_lines) # this reads the lines into a RDD
                df = spark.read.csv(csv_to_df, header=True, inferSchema=True)
                print(f'reading csv file {csv_file} into a dataframe')
                return df



# add a converted date col to df1, group by and give avg trip duration per day
def transform1(df1, spark):
    # add a new converted_start_time with date type from an existing col
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
    return df1, df_avg_tripduration


def transform2(df1, spark):
    df_trip_count_per_day = df1.groupby('converted_start_time', b'trip_id').count()
    return df_trip_count_per_day

