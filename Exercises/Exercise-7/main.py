from pyspark.sql import SparkSession
from spark_utils import read_zip, brand, ranking_joining
from pyspark.sql.functions import col, dense_rank, desc
import sys
import os

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def main():
    zip_path = 'data/hard-drive-2022-01-01-failures.csv.zip'
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    df = read_zip(zip_path, spark)
    df = brand(df, spark)
    df = df.dropDuplicates()
    print(f'number of rows in df :{df.count()}')

    # filter and drop rows with -1 value for 'capacity_bytes'
    df = df.filter(~((col('model') == 'ST12000NM0008') & (col('capacity_bytes') == -1)))
    print(f'number of rows in df after excluding unwanted rows :{df.count()}')

    joined_df = ranking_joining(df, spark)
    joined_df.show(40)
    print(f'number of rows in the final joined dataframe:{joined_df.count()}')

    # number of distinct models:66
    # number of distinct capacity:15  max:18000207937536 and rank=1


if __name__ == "__main__":
    main()
