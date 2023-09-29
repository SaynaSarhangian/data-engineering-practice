from zipfile import ZipFile
from pyspark.sql.functions import when, split
from pyspark.sql.window import Window
from pyspark.sql.functions import col, dense_rank, desc


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


# check if a str column contains a particular character, if not return a default value in the new col
def brand(df, spark):
    df = df.withColumn('brand', when(df.model.contains(' '), (split(df['model'], ' ')).getItem(0))
                       .otherwise('unknown'))
    return df


def df_with_ranking(df, spark):
    df_ranking = df.select('capacity_bytes', 'model')
    # do not partition the data, rank the whole dataset
    partition = Window.orderBy(desc('capacity_bytes'))
    df_ranking = df_ranking.withColumn('storage_ranking', dense_rank().over(partition))
    df_ranking.select('storage_ranking').distinct().show()
    df_ranking_dist = df_ranking.select('model', 'storage_ranking').distinct()
    print(f'showing "model" and "storage_ranking"')
    df_ranking_dist.show(66)
    joined_df = df_ranking_dist.join(df, on='model', how='left_outer')
    return joined_df
