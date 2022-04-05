# pip install psycopg2
import psycopg2
import psycopg2.extras
import pandas as pd
import random
import uuid
import datetime
import time


# Connection
# psql
# CREATE DATABASE movies;
# \c movies
# \dt
# CREATE TABLE metadata (imdbId INT, title TEXT);
# GRANT ALL ON metadata TO admin;
# CREATE TABLE ratings (movieId INT, rating REAL);
# GRANT ALL ON ratings TO admin;
# CREATE TABLE links (imdbId INT, movieId INT);
# GRANT ALL ON links TO admin;
conn = psycopg2.connect(
    host="localhost",
    database="movies",
    user="admin",
    password="admin"
)

conn.autocommit = True


def _convert_from_str_to_int(x):
    return int(x[2:])


def _insert_into_metadata(cursor, rows) -> None:

    query = '''
    INSERT INTO metadata (
        imdbId,
        title
    ) VALUES %s;
    '''

    psycopg2.extras.execute_values(cursor, query, rows)


def _insert_into_ratings(cursor, rows) -> None:

    query = '''
    INSERT INTO ratings (
        movieId,
        rating
    ) VALUES %s;
    '''

    psycopg2.extras.execute_values(cursor, query, rows)


def _insert_into_links(cursor, rows) -> None:

    query = '''
    INSERT INTO links (
        imdbId,
        movieId
    ) VALUES %s;
    '''

    psycopg2.extras.execute_values(cursor, query, rows)


def truncate_tables() -> None:
    with conn.cursor() as cursor:
        query = '''
            TRUNCATE metadata;
            TRUNCATE ratings;
            TRUNCATE links;
        '''

        cursor.execute(query)


def load_data_into_db(num_rows) -> None:

    ratings_df = pd.read_csv('data/ratings.csv')[['movieId', 'rating']].iloc[0:num_rows].dropna()

    metas_df = pd.read_csv('data/movies_metadata.csv')[['imdb_id', 'title']].dropna()
    metas_df['imdb_id'] = metas_df['imdb_id'].apply(_convert_from_str_to_int)
    metas_df = metas_df.rename(columns={'imdb_id': 'imdbId'})

    links_df = pd.read_csv('data/links.csv')[['imdbId', 'movieId']].dropna()

    with conn.cursor() as cursor:
        _insert_into_metadata(cursor, metas_df.values.tolist())
        _insert_into_links(cursor, links_df.values.tolist())
        _insert_into_ratings(cursor, list(map(list, ratings_df.itertuples(index=False))))


def join_tables() -> None:
    with conn.cursor() as cursor:
        query = '''
            SELECT m.title, avg(r.rating) FROM links l, metadata m, ratings r WHERE m.imdbId=l.imdbId and r.movieId=l.movieId GROUP BY m.title HAVING count(r.rating) > 2 and avg(r.rating) > 4.5
        '''

        cursor.execute(query)
        results = cursor.fetchall()
        for idx, result in enumerate(results[0:10]):
            print(idx + 1, result)
        print("Total size:", len(results))


if __name__ == '__main__':

    # Step 1: truncate tables for a clean slate
    truncate_tables()

    # num of rows
    num_rows = 10000000  # 10 million

    # Step 2: load data
    load_data_into_db(num_rows)

    # Step 3: join tables
    start = time.time()
    join_tables()
    end = time.time()

    print(f"{end - start} seconds are taken to join the dataframes by SQL.")

    # python join_by_sql.py
