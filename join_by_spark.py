import time

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import functions as func
from pyspark.sql import SparkSession


def _convert_from_str_to_int(x):
    return int(x[2:])


def load_data_into_spark(num_rows):

    ratings_df = pd.read_csv('data/ratings.csv')[['movieId', 'rating']].iloc[0:num_rows].dropna()
    ratings_df_schema = StructType([StructField("movieId", IntegerType(), True), StructField("rating", FloatType(), True)])

    metas_df = pd.read_csv('data/movies_metadata.csv')[['imdb_id', 'title']].dropna()
    metas_df['imdb_id'] = metas_df['imdb_id'].apply(_convert_from_str_to_int)
    metas_df = metas_df.rename(columns={'imdb_id': 'imdbId'})
    metas_df_schema = StructType([StructField("imdbId", IntegerType(), True), StructField("title", StringType(), True)])

    links_df = pd.read_csv('data/links.csv')[['imdbId', 'movieId']].dropna()
    links_df_schema = StructType([StructField("imdbId", IntegerType(), True), StructField("movieId", IntegerType(), True)])

    spark = SparkSession.builder.getOrCreate()

    t1 = spark.createDataFrame(list(map(list, ratings_df.itertuples(index=False))), ratings_df_schema)
    t2 = spark.createDataFrame(links_df.values.tolist(), links_df_schema)
    t3 = spark.createDataFrame(metas_df.values.tolist(), metas_df_schema)

    return t1, t2, t3


def join_df(t1, t2, t3):
    # normal spark join (runs on cluster default partitioning)
    # df = t1.join(t2, t1['movieId']==t2['movieId']).join(t3, t2['imdbId'] == t3['imdbId'])

    # broadcast smaller tables to worker
    df = t1.join(func.broadcast(t2), t1['movieId'] == t2['movieId']).join(func.broadcast(t3),
                                                                          t2['imdbId'] == t3['imdbId'])
    # get partitions
    # df = df.repartition(100)
    # print(df.rdd.getNumPartitions())

    # group by results
    df = df.groupBy('title').agg(func.mean('rating').alias('avg_rating'),
                                 func.count('rating').alias('count_rating')).filter('count_rating > 2').filter('avg_rating > 4.5')

    # show the results
    df.show(10)
    print("Total size:", df.count())


if __name__ == '__main__':

    # num of rows
    num_rows = 10000000 # 10 million

    # Step 1: load data
    t1, t2, t3 = load_data_into_spark(num_rows)

    # Step 2: join dataframes
    start = time.time()
    join_df(t1, t2, t3)
    end = time.time()

    print(f"{end - start} seconds are taken to join the dataframes by Spark.")

    # spark-submit --master local[*] join_by_spark.py