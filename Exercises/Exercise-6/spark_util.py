from zipfile import ZipFile
import os

from pyspark.sql.functions import to_date, col


def read_zip(zip_path, spark):
    with ZipFile(zip_path, 'r') as zip:
        for csv_file in zip.namelist():
            # only consider the required csv file
            if csv_file.startswith('Divvy'):
                csv_data = zip.read(csv_file)
                # Convert CSV data into a DataFrame
                csv_to_df = spark.sparkContext.parallelize(csv_data.splitlines())
                df = spark.read.csv(csv_to_df, header=True, inferSchema=True)
                print(f'reading csv file {csv_file} into a dataframe')
                return df


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
    return df_avg_tripduration

def transform2(df1, spark):



