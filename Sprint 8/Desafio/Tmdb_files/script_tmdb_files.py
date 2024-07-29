import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DateType, FloatType
import re


# Initialize Glue job______________________________________________________________________________________________________________________


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH_TMDB_SERIES', 'S3_INPUT_PATH_TMDB_MOVIES', 'S3_BASE_OUTPUT'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Connect with AWS_________________________________________________________________________________________________________________________


def aws_connection():
    s3 = boto3.client('s3')
    bucket_name = "datalake-desafio-compassuol-rafael"
    return s3, bucket_name


# Define Original Schemas__________________________________________________________________________________________________________________


def schemas():
    schema_tmdb_series = StructType([
        StructField("id", IntegerType()),
        StructField("original_name", StringType()),
        StructField("name", StringType()),
        StructField("origin_country", StringType()),
        StructField("genre_ids", StringType()),
        StructField("first_air_date", DateType()),
        StructField("popularity", FloatType()),
        StructField("vote_average", FloatType()),
        StructField("vote_count", IntegerType()),
        StructField("cast", ArrayType(StructType([
            StructField("id", IntegerType()),
            StructField("nome", StringType()),
            StructField("personagem", StringType()),
            StructField("data_nascimento", DateType()),
            StructField("sexo_ator", IntegerType())
        ])))
    ])

    schema_tmdb_filmes = StructType([
        StructField("id", IntegerType()),
        StructField("original_title", StringType()),
        StructField("title", StringType()),
        StructField("genre_ids", StringType()),
        StructField("release_date", DateType()),
        StructField("popularity", FloatType()),
        StructField("vote_average", FloatType()),
        StructField("vote_count", IntegerType()),
        StructField("cast", ArrayType(StructType([
            StructField("id", IntegerType()),
            StructField("nome", StringType()),
            StructField("personagem", StringType()),
            StructField("data_nascimento", DateType()),
            StructField("sexo_ator", IntegerType())
        ])))
    ])


    return schema_tmdb_series, schema_tmdb_filmes


# Read JSON files from S3__________________________________________________________________________________________________________________


def read_json_tmdb_files(bucket_name, s3, schema_tmdb_series, schema_tmdb_filmes):

#* Read Series files
    response_series = s3.list_objects_v2(Bucket=bucket_name, Prefix=args['S3_INPUT_PATH_TMDB_SERIES'])
    json_series_files_content = spark.read.option("multiline", "true").schema(schema_tmdb_series).json([
        f"s3a://{bucket_name}/{content.get('Key')}"
        for content in response_series.get("Contents", [])
    ])

#* Read Movies files
    response_movies = s3.list_objects_v2(Bucket=bucket_name, Prefix=args['S3_INPUT_PATH_TMDB_MOVIES'])
    json_movies_files_content = spark.read.option("multiline", "true").schema(schema_tmdb_filmes).json([
        f"s3a://{bucket_name}/{content.get('Key')}"
        for content in response_movies.get("Contents", [])
    ])


    return json_series_files_content, json_movies_files_content


# Filter and Group Series Dataframes_______________________________________________________________________________________________________


def filter_tmdb_series_dataframe(json_series_files_content):

#* Explode Dataframes
    df_series_exploded = json_series_files_content.withColumn("cast", explode("cast"))

#* Filtering Series Dataframes
    df_series_filtered = df_series_exploded.select(
        col("id").alias("serie_id"),
        col("original_name").alias("serie_original_name"),
        col("name").alias("serie_name"),
        col("origin_country").alias("serie_origin_country"),
        col("genre_ids").alias("serie_genre_ids"),
        col("first_air_date").alias("serie_first_air_date"),
        col("popularity").alias("serie_popularity"),
        col("vote_average").alias("serie_vote_average"),
        col("vote_count").alias("serie_vote_count"),
    )

    df_series_cast = df_series_exploded.select(
        col("id").alias("serie_id"),
        col("cast.id").alias("cast_id"),
        col("cast.nome").alias("cast_name"),
        col("cast.personagem").alias("cast_character"),
        col("cast.data_nascimento").alias("cast_birthdate"),
        col("cast.sexo_ator").alias("cast_gender"))

    df_series_filtered = df_series_filtered.where(
        (col("serie_id").isNotNull())
    ).dropna().dropDuplicates().orderBy(col("serie_id"))

    df_series_cast_filtered = df_series_cast.where(
        (col("serie_id").isNotNull()) &
        (col("cast_id").isNotNull()) &
        (col("cast_gender").isin([1, 2]))
    ).dropna().dropDuplicates().orderBy(col("serie_id"))


    return df_series_filtered, df_series_cast_filtered


# Filter and group Movies Dataframes_______________________________________________________________________________________________________


def filter_tmdb__movies_dataframe(json_movies_files_content):


#* Explode Dataframes
    df_movies_exploded = json_movies_files_content.withColumn("cast", explode("cast"))

#* Filtering Movies Dataframes
    df_movies_filtered = df_movies_exploded.select(
        col("id").alias("movie_id"),
        col("original_title").alias("movie_original_name"),
        col("title").alias("movie_name"),
        col("genre_ids").alias("movie_genres"),
        col("release_date").alias("movie_release_date"),
        col("popularity").alias("movie_popularity"),
        col("vote_average").alias("movie_vote_average"),
        col("vote_count").alias("movie_vote_count"))

    df_movies_cast = df_movies_exploded.select(
        col("id").alias("movie_id"),
        col("cast.id").alias("cast_id"),
        col("cast.nome").alias("cast_name"),
        col("cast.personagem").alias("cast_character"),
        col("cast.data_nascimento").alias("cast_birthdate"),
        col("cast.sexo_ator").alias("cast_gender"))

    df_movies_filtered = df_movies_filtered.where(
        (col("movie_id").isNotNull())
    ).dropna().dropDuplicates().orderBy(col("movie_id"))

    df_movies_cast_filtered = df_movies_cast.where(
        (col("movie_id").isNotNull()) &
        (col("cast_id").isNotNull()) &
        (col("cast_gender").isin([1, 2]))
    ).dropna().dropDuplicates().orderBy(col("movie_id"))


    return df_movies_filtered, df_movies_cast_filtered


# Write Partitioned Parquet________________________________________________________________________________________________________________


def write_partitioned_parquet(df, base_output_path, partition_name, input_file_path):

#* Extract Date from File Path
    match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', input_file_path)
    year, month, day = match.groups()
    output_path = f"{base_output_path}/{partition_name}/{year}/{month}/{day}/"

#* Write Parquet
    df.write.mode("overwrite").parquet(output_path)


# Main_____________________________________________________________________________________________________________________________________


def main():

#* Connect with AWS
    s3, bucket_name = aws_connection()
    schema_tmdb_series, schema_tmdb_movies = schemas()

#* Read JSON files from S3
    json_series_files_content, json_movies_files_content = read_json_tmdb_files(bucket_name, s3, schema_tmdb_series, schema_tmdb_movies)
    df_series_filtered, df_series_cast_filtered = filter_tmdb_series_dataframe(json_series_files_content)
    df_movies_filtered, df_movies_cast_filtered = filter_tmdb__movies_dataframe(json_movies_files_content)

#* Write Series and Series Cast DataFrames
    base_output_path = args['S3_BASE_OUTPUT']
    response_series, response_movies = s3.list_objects_v2(Bucket=bucket_name, Prefix=args['S3_INPUT_PATH_TMDB_SERIES'])
    series_files = [f"s3a://{bucket_name}/{content.get('Key')}" for content in response_series.get("Contents", [])]
    for file_path in series_files:
        write_partitioned_parquet(df_series_filtered, base_output_path, "Series", file_path)
        write_partitioned_parquet(df_series_cast_filtered, base_output_path, "Series_Cast", file_path)

#* Write Movies and Movies Cast DataFrames
    response_movies = s3.list_objects_v2(Bucket=bucket_name, Prefix=args['S3_INPUT_PATH_TMDB_MOVIES'])
    movies_files = [f"s3a://{bucket_name}/{content.get('Key')}" for content in response_movies.get("Contents", [])]
    for file_path in movies_files:
        write_partitioned_parquet(df_movies_filtered, base_output_path, "Movies", file_path)
        write_partitioned_parquet(df_movies_cast_filtered, base_output_path, "Movies_Cast", file_path)

main()
job.commit()