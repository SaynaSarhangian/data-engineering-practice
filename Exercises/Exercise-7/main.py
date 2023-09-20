from pyspark.sql import SparkSession
#import pyspark.sql.functions as F
from spark_utils import read_zip
from spark_utils import brand
import sys
import os

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def main():
    zip_path = 'data/hard-drive-2022-01-01-failures.csv.zip'
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    df = read_zip(zip_path, spark)
    #df.show(50)
    #df.printSchema()
    df = brand(df, spark)
    df.show(50)



if __name__ == "__main__":
    main()
