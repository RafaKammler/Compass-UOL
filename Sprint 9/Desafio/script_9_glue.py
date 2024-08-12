from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, LongType, DateType, TimestampType
import boto3
from pyspark.sql.functions import monotonically_increasing_id, when, concat_ws, col, year, lit, collect_list, row_number, array_sort, coalesce
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import sys


#Aws glue configuration______________________________________________________________________________________________________________________________


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Aws Connection_____________________________________________________________________________________________________________________________________


def aws_connection():
    s3 = boto3.client('s3')
    bucket_name = "datalake-desafio-compassuol-rafael"
    return s3, bucket_name


# Reading files from S3______________________________________________________________________________________________________________________________


def reading_parquet_files(s3, bucket_name, prefixes):

#* Reading the prefixes list and searching for the files in each path
    dataframes = []
    for prefix in prefixes:
        response = s3.list_objects_v2(Bucket = bucket_name, Prefix = prefix)
        parquet_files = [
            f"s3a://{bucket_name}/{content.get('Key')}" 
            for content in response.get("Contents", [])
        ]
        
        #* Saving the files content into a list of DataFrames
        df = spark.read.parquet(*parquet_files)
        print(f"Reading {len(parquet_files)} parquet files from {prefix}")
        dataframes.append(df)


    return dataframes


# Separating the DataFrames list to multiple DataFrames______________________________________________________________________________________________


def separating_csv_dataframes(dataframes):
    
    #* Separates with each index beign one DF
    csv_movies =      dataframes[0]
    csv_series =      dataframes[1]
    csv_movies_cast = dataframes[2]
    csv_series_cast = dataframes[3]
    
    
    return csv_movies, csv_series, csv_movies_cast, csv_series_cast


def separating_tmdb_dataframes(dataframes):
    
    #* Separates with each index beign one DF
    tmdb_movies =        dataframes[4]
    tmdb_series =        dataframes[5]
    tmdb_movies_cast =   dataframes[6]
    tmdb_series_cast =   dataframes[7]
    tmdb_movies_genres = dataframes[8]
    tmdb_series_genres = dataframes[9]
    
    
    return tmdb_movies, tmdb_series, tmdb_movies_cast, tmdb_series_cast, tmdb_movies_genres, tmdb_series_genres


# Padronizing the columns of all the IMDB DataFrames___________________________________________________________________________________________________


def padronizing_csv_dfs(csv_movies, csv_series, csv_movies_cast, csv_series_cast):
    
    #* Filtering movies DF
    csv_movies = csv_movies\
    .select(
    "movie_id",
    col("movie_name")         .alias("name"),
    col("movie_genres")       .alias("genres"),
    col("movie_vote_average") .alias("vote_average"),
    col("movie_vote_count")   .alias("vote_count"),
    col("movie_release_date") .alias("release_date")
    )\
    .dropDuplicates()\
    .orderBy("movie_vote_count", ascending=False)
    
    #* Filtering series DF
    csv_series = csv_series\
    .select(
        "serie_id",
        col("serie_name")           .alias("name"),
        col("serie_genres")         .alias("genres"),
        col("serie_vote_average")   .alias("vote_average"),
        col("serie_vote_count")     .alias("vote_count"),
        col("serie_first_air_date") .alias("release_date")
    )\
    .dropDuplicates()\
    .orderBy("vote_count", ascending=False)
    
    #* Filtering movies_cast DF
    csv_movies_cast = csv_movies_cast\
        .withColumn("cast_gender",  when(csv_movies_cast["cast_gender"] == "actress", "1")
                                   .when(csv_movies_cast["cast_gender"] == "actor", "2")
                                   .otherwise(csv_movies_cast["cast_gender"])
    )\
    .select(
        "movie_id",
        col("cast_name")      .alias("name"),
        col("cast_character") .alias("character"),
        col("cast_gender")    .alias("gender")
    )

    #* Filtering Series_cast DF
    csv_series_cast = csv_series_cast \
    .withColumn("cast_gender",  when(csv_series_cast["cast_gender"] == "actress", "1")
                               .when(csv_series_cast["cast_gender"] == "actor", "2")
                               .otherwise(csv_series_cast["cast_gender"])
    )\
    .select(
        "serie_id",
        col("cast_name")      .alias("name"),
        col("cast_character") .alias("character"),
        col("cast_gender")    .alias("gender")
    )


    return csv_movies, csv_series, csv_movies_cast, csv_series_cast


# Padronizing the columns of all the TMDB DataFrames_________________________________________________________________________________________________


def padronizing_tmdb_dfs(tmdb_movies, tmdb_series, tmdb_movies_cast, tmdb_series_cast, tmdb_movies_genres, tmdb_series_genres):
    
    #* Filtering series DF
    tmdb_series = tmdb_series\
    .withColumn("release_date", year(col("serie_first_air_date")))\
    .select(
        "serie_id",
        col("serie_name")         .alias("name"),
        col("serie_vote_average") .alias("vote_average"),
        col("serie_vote_count")   .alias("vote_count"),
        col("release_date")
    )\
    .dropDuplicates()\
    .orderBy("vote_count", ascending=False)

    #* Filtering movies DF
    tmdb_movies = tmdb_movies\
    .withColumn("release_date", year(col("movie_release_date")))\
    .select(
        "movie_id",
        col("movie_name")         .alias("name"),
        col("movie_vote_average") .alias("vote_average"),
        col("movie_vote_count")   .alias("vote_count"),
        col("release_date")
    )\
    .dropDuplicates()\
    .orderBy("vote_count", ascending=False)
    
    #* Filtering movies_genres DF
    tmdb_movies_genres = tmdb_movies_genres\
        .dropDuplicates()\
        .dropna()\
        .select(
            "movie_id",
            "genre_id",
            "genre_name"
        )
    
    #* Filtering series_genres DF
    tmdb_series_genres = tmdb_series_genres\
        .dropDuplicates()\
        .dropna()\
        .select(
            "serie_id",
            "genre_id",
            "genre_name"
        )

    tmdb_movies_cast = tmdb_movies_cast\
        .select(
            "movie_id",
            col("cast_name")      .alias("name"),
            col("cast_character") .alias("character"),
            col("cast_gender")    .alias("gender")
        )
    
    tmdb_series_cast = tmdb_series_cast\
        .select(
            "serie_id",
            col("cast_name")      .alias("name"),
            col("cast_character") .alias("character"),
            col("cast_gender")    .alias("gender")
        )

    return tmdb_movies, tmdb_series, tmdb_movies_cast, tmdb_series_cast, tmdb_movies_genres, tmdb_series_genres


# Functions to group the common DataFrames together__________________________________________________________________________________________________


def grouping_all_actors(csv_movies_cast, csv_series_cast, tmdb_movies_cast, tmdb_series_cast):
    
    #* Make the dataframes have the same columns
    csv_movies_cast = csv_movies_cast   .withColumn("serie_id", lit(None))
    csv_series_cast = csv_series_cast   .withColumn("movie_id", lit(None))
    tmdb_movies_cast = tmdb_movies_cast .withColumn("serie_id", lit(None))
    tmdb_series_cast = tmdb_series_cast .withColumn("movie_id", lit(None))

    #* Puts the DataFrames together and adds and unique_ids to each register
    all_movies_casts = csv_movies_cast\
        .unionByName(tmdb_movies_cast)

    all_series_casts = csv_series_cast\
                .unionByName(tmdb_series_cast)

    all_casts = all_movies_casts\
        .unionByName(all_series_casts)

    all_casts = all_casts\
        .dropDuplicates()\
        .withColumn("unique_cast_id", row_number().over(Window.orderBy("name")))
    
    all_casts = all_casts\
        .select(
            "unique_cast_id",
            "movie_id",
            "serie_id",
            "name",
            "character",
            "gender"
        )
    

    return all_casts


def grouping_all_movies(csv_movies, tmdb_movies):
    
    #* removes the genres column from movies, beacuse it will be in another table 
    all_movies = csv_movies\
        .drop("genres")\
        .union(tmdb_movies)


    return all_movies


def grouping_all_series(csv_series, tmdb_series):
    
    #* removes the genres column from series, beacuse it will be in another table 
    all_series = csv_series\
        .drop("genres")\
        .union(tmdb_series)
    
    
    return all_series


def grouping_all_genres(tmdb_movies_genres, tmdb_series_genres, csv_movies, csv_series):

    #* Make the dataframes have the same columns
    csv_movies = csv_movies.withColumn("serie_id", lit(None))
    csv_series = csv_series.withColumn("movie_id", lit(None))
    
    #* Group the genres from IMDB into 1 DF
    all_genres_csv = csv_movies.select("movie_id", "serie_id", "genres")\
        .union(csv_series.select("movie_id", "serie_id", "genres"))

    #* Aggregate the genres so that its one genre register per movie, separating the genres with ","
    tmdb_movies_genres = tmdb_movies_genres.groupBy("movie_id")\
        .agg(concat_ws(",", array_sort(collect_list("genre_name"))).alias("genres"))\
        .withColumn("serie_id", lit(None))
        
    tmdb_series_genres = tmdb_series_genres.groupBy("serie_id")\
        .agg(concat_ws(",", array_sort(collect_list("genre_name"))).alias("genres"))\
        .withColumn("movie_id", lit(None))
    
    #* Group the genres of TMDB into 1 DF
    all_genres_tmdb = tmdb_movies_genres.select("movie_id", "serie_id", "genres")\
        .union(tmdb_series_genres.select("movie_id", "serie_id", "genres"))

    #* Group everything together
    all_genres = all_genres_tmdb.union(all_genres_csv)
    
    #* Deduplicate the final DataFrame
    all_genres = all_genres.dropDuplicates(["movie_id", "serie_id", "genres"])
    
    return all_genres


# Function to create all dimensions__________________________________________________________________________________________________________________


def creating_dim_tables(all_casts, all_series, all_movies, all_genres):
    #* Create the movies dimension
    dim_movies = all_movies\
        .select(
            "movie_id",
            "name"
        )\
        .dropDuplicates(["movie_id"])

    #* Create the series Dimension
    dim_series = all_series\
        .select(
            "serie_id",
            "name"
        )\
        .dropDuplicates(["serie_id"])
    
    #* Create an unique_time dimension
    dim_time = all_movies\
        .union(all_series)\
        .orderBy("release_date", ascending=True)\
        .dropDuplicates(["release_date"])\
        .withColumn("unique_time_id", monotonically_increasing_id())\
        .select(
            "unique_time_id",
            "release_date"
        )
    
    #* Create the genres dimension
    window_spec = Window.orderBy("movie_id")
    dim_genres = all_genres\
    .withColumn("unique_genre_id", row_number().over(window_spec))\
    .select(
        "unique_genre_id",
        "movie_id",
        "serie_id",
        "genres"
    )\

    #* Create an cast_dimension
    dim_cast = all_casts\
        .dropDuplicates()\
        .orderBy("unique_cast_id", ascending=True)\
        .select(
            "unique_cast_id",
            "movie_id",
            "serie_id",
            "name",
            "character",
            "gender"
        )\
        
    
    return dim_movies, dim_series, dim_time, dim_cast, dim_genres


# Creating the media_fact table______________________________________________________________________________________________________________________


def creating_fact_table(dim_movies, dim_series, dim_time, dim_cast, all_cast, all_movies, all_series, dim_genres):

    all_movies = all_movies.withColumn("serie_id", lit(None))
    all_series = all_series.withColumn("movie_id", lit(None))
    

    all_media = all_movies.unionByName(all_series)
    

    dim_cast = dim_cast.withColumnRenamed("movie_id", "cast_movie_id").withColumnRenamed("serie_id", "cast_serie_id")
    dim_genres = dim_genres.withColumnRenamed("movie_id", "genre_movie_id").withColumnRenamed("serie_id", "genre_serie_id")
    

    media_fact = all_media.join(dim_time, "release_date", "left")
    

    media_fact = media_fact.join(
        dim_cast,
        (media_fact.movie_id == dim_cast.cast_movie_id) | (media_fact.serie_id == dim_cast.cast_serie_id),
        "left"
    )
    

    media_fact = media_fact.join(
        dim_genres,
        (media_fact.movie_id == dim_genres.genre_movie_id) | (media_fact.serie_id == dim_genres.genre_serie_id),
        "left"
    )
    

    media_fact = media_fact.select(
        "movie_id",
        "serie_id",
        "unique_time_id",
        "unique_cast_id",
        "unique_genre_id",
        "vote_average",
        "vote_count"
    ).orderBy("vote_count", ascending=False)
    
    return media_fact


# Write the files into the Datalake in the S3 Bucket_________________________________________________________________________________________________


def writing_to_s3(df, bucket_name, path):
    df.write.mode("overwrite").parquet(f"s3a://{bucket_name}/{path}")
    
    
# Main function to run all the other funcions________________________________________________________________________________________________________


def main():
    
    #* Connect to AWS and read the files from the bucket
    s3, bucket_name = aws_connection()
    prefixes = [
        'Trusted/CSV/Movies/',
        'Trusted/CSV/Series/',
        'Trusted/CSV/Movies_cast/',
        'Trusted/CSV/Series_cast/',
        'Trusted/TMDB/Movies/',
        'Trusted/TMDB/Series/',
        'Trusted/TMDB/Movies_Cast/',
        'Trusted/TMDB/Series_Cast/',
        'Trusted/TMDB/Movie_genders/',
        'Trusted/TMDB/Series_Genders/'
    ]
    dataframes = reading_parquet_files(s3, bucket_name, prefixes)

    #* Separate all the DFs from the list
    csv_movies, csv_series, csv_movies_cast, csv_series_cast = separating_csv_dataframes(dataframes)
    tmdb_movies, tmdb_series, tmdb_movies_cast, tmdb_series_cast, tmdb_movies_genders, tmdb_series_genders = separating_tmdb_dataframes(dataframes)
    
    #* Padronizing all the DFs
    csv_movies_padronized, csv_series_padronized, csv_movies_cast_padronized, csv_series_cast_padronized,  = padronizing_csv_dfs(csv_movies, csv_series, csv_movies_cast, csv_series_cast)
    tmdb_movies_padronized, tmdb_series_padronized, tmdb_movies_cast_padronized, tmdb_series_cast_padronized, tmdb_movies_genders_padronized, tmdb_series_genders_padronized = padronizing_tmdb_dfs(tmdb_movies, tmdb_series, tmdb_movies_cast, tmdb_series_cast, tmdb_movies_genders, tmdb_series_genders)

    #* Grouping all the dataframes
    all_cast =   grouping_all_actors(csv_movies_cast_padronized, csv_series_cast_padronized, tmdb_movies_cast_padronized, tmdb_series_cast_padronized)
    all_movies = grouping_all_movies(csv_movies_padronized, tmdb_movies_padronized)
    all_series = grouping_all_series(csv_series_padronized, tmdb_series_padronized)
    all_genres = grouping_all_genres(tmdb_movies_genders_padronized, tmdb_series_genders_padronized, csv_movies_padronized, csv_series_padronized)

    #* Creating all the dimensions and fact tables
    dim_movies, dim_series, dim_time, dim_cast, dim_genres = creating_dim_tables(all_cast, all_series, all_movies, all_genres)
    fact_table = creating_fact_table(dim_movies, dim_series, dim_time, dim_cast, all_cast,all_movies,all_series,dim_genres)

    #* Defining the paths and writing the DFs to the Bucket
    paths = [
        "Refined/dim_movies/",
        "Refined/dim_series/",
        "Refined/dim_time/",
        "Refined/dim_cast/",
        "Refined/dim_genres/",
        "Refined/fact_table/"        
    ]
    
    dataframes = [
        dim_movies, 
        dim_series, 
        dim_time, 
        dim_cast, 
        dim_genres, 
        fact_table
    ]

    for df, path in zip(dataframes, paths):
        writing_to_s3(df, bucket_name, path)
    
    
if __name__ == "__main__":
    main()