from zipfile import ZipFile
# import pyspark.sql.functions as F
from pyspark.sql.functions import when, split


def read_zip(zip_path, spark):
    with ZipFile(zip_path, 'r') as zip:
        for csv_file in zip.namelist():
            csv_data = zip.read(csv_file)
            csv_data_str = str(csv_data, 'utf-8')
            file_lines = csv_data_str.splitlines()  # this turns the file content to a list of lines (list of strings)
            csv_to_df = spark.sparkContext.parallelize(file_lines)  # this reads the lines into a RDD
            print(f'converting the csv file inside a zip file to dataframe and returning it....')
            df = spark.read.csv(csv_to_df, header=True, inferSchema=True)
            return df

    # Q3:Add a new column called brand...


def brand(df, spark):
    df = df.withColumn('brand', when(df.model.contains(' '), (split(df['model'], ' ')).getItem(0))
                       .otherwise('unknown'))
    return df
# (F.split(df('model'), ' ')).getItem(0))
