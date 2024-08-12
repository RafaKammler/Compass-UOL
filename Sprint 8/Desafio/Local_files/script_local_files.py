import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from pyspark.sql.functions import col, collect_list, struct


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH_CSV_MOVIES', 'S3_INPUT_PATH_CSV_SERIES', 'S3_BASE_OUTPUT_PATH'])


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# AWS Connection___________________________________________________________________________________________________________________________


def aws_connection():
    s3 = boto3.client('s3')
    bucket_name = "datalake-desafio-compassuol-rafael"
    return s3, bucket_name


# Read CSV Files___________________________________________________________________________________________________________________________


def read_csv_file(s3, bucket_name):

#* Read Movie Files
    response_movies = s3.list_objects_v2(Bucket = bucket_name, Prefix = args['S3_INPUT_PATH_CSV_MOVIES'])
    csv_movies_files_content = spark.read\
        .option("header", "true")\
        .option("sep", "|")\
        .csv([
        f"s3a://{bucket_name}/{content.get('Key')}" 
        for content in response_movies.get("Contents")
        ])

#* Read Series Files
    response_series = s3.list_objects_v2(Bucket = bucket_name, Prefix = args['S3_INPUT_PATH_CSV_SERIES'])
    csv_series_files_content = spark.read\
        .option("header", "true")\
        .option("sep", "|")\
        .csv([
        f"s3a://{bucket_name}/{content.get('Key')}" 
        for content in response_series.get("Contents")
        ])


    return csv_movies_files_content, csv_series_files_content


# Selecting Columns________________________________________________________________________________________________________________________


def selecting_columns_series(csv_series):

#* Selecting Columns from Series and Series Count
    csv_series_correct = csv_series.select(
        col("id"),
        col("tituloOriginal"),
        col("tituloPincipal"),
        col("genero"),
        col("anoLancamento"),
        col("notaMedia"),
        col("numeroVotos")    
    )

    csv_series_cast = csv_series.select(
        col("id"),
        col("nomeArtista"),
        col("personagem"),
        col("anoNascimento"),
        col("generoArtista") 
    ).where(col("genero").contains("Crime"))


    return csv_series_correct, csv_series_cast

def selecting_columns_movies(csv_movies):

#* Selecting Columns from Movies and Movies Cast
    csv_movies_correct = csv_movies.select(
        col("id"),
        col("tituloOriginal"),
        col("tituloPincipal"),
        col("genero"),
        col("anoLancamento"),
        col("notaMedia"),
        col("numeroVotos")    
    )

    csv_movies_cast = csv_movies.select(
        col("id"),
        col("nomeArtista"),
        col("personagem"),
        col("anoNascimento"),
        col("generoArtista") 
    ).where(col("genero").contains("Crime"))


    return csv_movies_correct, csv_movies_cast


# Cleaning and Organizing Columns__________________________________________________________________________________________________________
def renaming_and_casting_movies(csv_movies, csv_movies_cast_original):

#* Renaming and Casting Columns from Movies and Movies Cast
    csv_movies = csv_movies.na.replace("\\N", None)
    csv_movies_cast_original = csv_movies_cast_original.na.replace("\\N", None)
    
    csv_movies_filtered = csv_movies\
        .withColumnRenamed("id",             "movie_id")\
        .withColumnRenamed("tituloOriginal", "movie_original_name")\
        .withColumnRenamed("tituloPincipal", "movie_name")\
        .withColumnRenamed("genero",         "movie_genres")\
        .withColumnRenamed("anoLancamento",  "movie_release_date")\
        .withColumnRenamed("notaMedia",      "movie_vote_average")\
        .withColumnRenamed("numeroVotos",    "movie_vote_count")\
        .withColumn("movie_release_date",    col("movie_release_date") .cast("int"))\
        .withColumn("movie_vote_average",    col("movie_vote_average") .cast("float"))\
        .withColumn("movie_vote_count",      col("movie_vote_count")   .cast("int")
        )\
        .where(col('movie_genres').contains('Crime') & col("movie_id").isNotNull())\
        .dropna()\
        .dropDuplicates()\
        .orderBy(col("movie_id"))

    csv_movies_cast = csv_movies_cast_original\
        .withColumnRenamed("id",            "movie_id")\
        .withColumnRenamed("nomeArtista",   "cast_name")\
        .withColumnRenamed("personagem",    "cast_character")\
        .withColumnRenamed("anoNascimento", "cast_birthdate")\
        .withColumnRenamed("generoArtista", "cast_gender")\
        .withColumn("cast_birthdate",       col("cast_birthdate").cast("int")
        )\
        .where(col("movie_id").isNotNull() & col("cast_name").isNotNull())\
        .dropna()\
        .dropDuplicates()\
        .orderBy(col("movie_id"))


    return csv_movies_filtered, csv_movies_cast


def renaming_and_casting_series(csv_series, csv_series_cast_original):

#* Renaming and Casting Columns from Series and Series Cast
    csv_series = csv_series.na.replace("\\N", None)
    csv_series_cast_original = csv_series_cast_original.na.replace("\\N", None)

    csv_series_filtered = csv_series\
        .withColumnRenamed("id",             "serie_id")\
        .withColumnRenamed("tituloOriginal", "serie_original_name")\
        .withColumnRenamed("tituloPincipal", "serie_name")\
        .withColumnRenamed("genero",         "serie_genres")\
        .withColumnRenamed("anoLancamento",  "serie_first_air_date")\
        .withColumnRenamed("notaMedia",      "serie_vote_average")\
        .withColumnRenamed("numeroVotos",    "serie_vote_count")\
        .withColumn("serie_first_air_date",  col("serie_first_air_date") .cast("int"))\
        .withColumn("serie_vote_average",    col("serie_vote_average")   .cast("float"))\
        .withColumn("serie_vote_count",      col("serie_vote_count")     .cast("int")
        )\
        .where(col('serie_genres').contains('Crime') & col("serie_id").isNotNull())\
        .dropna()\
        .dropDuplicates()\
        .orderBy(col("serie_id"))

    csv_series_cast = csv_series_cast_original\
        .withColumnRenamed("id",            "serie_id")\
        .withColumnRenamed("nomeArtista",   "cast_name")\
        .withColumnRenamed("personagem",    "cast_character")\
        .withColumnRenamed("anoNascimento", "cast_birthdate")\
        .withColumnRenamed("generoArtista", "cast_gender")\
        .withColumn("cast_birthdate",       col("cast_birthdate").cast("int")
        )\
        .where(col("serie_id").isNotNull() & col("cast_name").isNotNull())\
        .dropna()\
        .dropDuplicates()\
        .orderBy(col("serie_id"))


    return csv_series_filtered, csv_series_cast


# Writing to S3 as Parquet_________________________________________________________________________________________________________________


def write_to_parquet(df, bucket_name, base_output_path, partition_name):
    df.write.mode("overwrite").parquet(f"s3a://{bucket_name}/{base_output_path}/{partition_name}")


# Main Function____________________________________________________________________________________________________________________________


def main():

#* Reading the S3 Files
    s3, bucket_name = aws_connection()
    csv_movies, csv_series = read_csv_file(s3, bucket_name)

#* Organizing the Dataframes
    csv_movies_correct, csv_movies_cast = selecting_columns_movies(csv_movies)
    csv_series_correct, csv_series_cast = selecting_columns_series(csv_series)
    csv_movies_filtered, csv_movies_cast = renaming_and_casting_movies(csv_movies_correct, csv_movies_cast)
    csv_series_filtered, csv_series_cast = renaming_and_casting_series(csv_series_correct, csv_series_cast)

#* Writing the Dataframes to S3
    base_output_path = args['S3_BASE_OUTPUT_PATH']
    write_to_parquet(csv_movies_filtered, bucket_name, base_output_path, "Movies")
    write_to_parquet(csv_movies_cast,bucket_name, base_output_path, "Movies_cast")
    write_to_parquet(csv_series_filtered,bucket_name, base_output_path, "Series")
    write_to_parquet(csv_series_cast,bucket_name, base_output_path, "Series_cast")



if __name__ == "__main__":
    main()

job.commit()
