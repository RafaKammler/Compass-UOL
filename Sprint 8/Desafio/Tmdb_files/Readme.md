# Desafio

A segunda parte do desafio foi a transformação dos dados que foram previamente adquiridos da API do TMDB. Esta etapa realiza basicamente a mesma coisa que a primeira, alterando somente alguns detalhes de leitura por se tratarem de informações com estruturas e tipos de arquivo distintos.

## Parte 1

Assim como na primeira transformação, realizada com os dados provenientes dos arquivos locais, a primeira parte deste script também realiza a conexão com o S3 bucket, por meio do `boto3`, e a atribuição do conteúdo dos arquivos a DataFrames do `PySpark`. Como os arquivos estavam no formato `.json`, foi necessária a criação de um schema que definisse quais eram as colunas a serem lidas e o tipo do conteúdo presente nelas. Esse schema é atribuído a uma variável da seguinte maneira:


```py
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
```

Isso teve de ser realizado para ambos os arquivos, com pequenas alterações nos nomes das chaves, que são diferentes para filmes e séries.

Com isso, foi possível ler o conteúdo dos arquivos de maneira semelhante à anteriormente, onde os arquivos são listados por meio do `list_objects_v2` e têm seus conteúdos atribuídos a um DataFrame por meio do `spark.read`, organizado da seguinte maneira:

```py
    response_series = s3.list_objects_v2(Bucket=bucket_name, Prefix=args['S3_INPUT_PATH_TMDB_SERIES'])
    json_series_files_content = spark.read.option("multiline", "true").schema(schema_tmdb_series).json([
        f"s3a://{bucket_name}/{content.get('Key')}"
        for content in response_series.get("Contents", [])
    ])
```

## Parte 2 

Com o conteúdo dos arquivos já salvo em DataFrames, a próxima etapa é o tratamento dos dados, por meio da seleção das colunas que podem se tornar úteis futuramente. Já aproveitarei a parte da seleção para renomear as colunas, padronizando-as com as colunas dos arquivos locais. Antes dessa seleção, foi necessária a separação da coluna `cast`, pois se tratava de diversas listas de dicionários, o que não era exatamente do meu agrado, pois não sei exatamente como lidar com isso utilizando `SQL`. Então, para poupar trabalho futuramente, resolvi separar a coluna `cast` com o método `explode` e transformá-la em um novo DataFrame. Toda essa seção foi construída da seguinte maneira:

```py
df_series_exploded = json_series_files_content.withColumn("cast", explode("cast"))

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
```

Isso foi realizado tanto para filmes quanto para séries. Mas esse não é a fim da seção de filtragem. Mais uma tarefa foi inserida na mesma função, que funciona limpando os dados, ou seja, removendo tudo que é indesejado, como por exemplo dados nulos e duplicados, retirando também os registros com IDs nulos ou que contém um gênero de ator inválido, além de ordenar os registros por ID de série/filme. Tudo isso resultou no seguinte código:

```py
    df_series_filtered = df_series_filtered.where(
        (col("serie_id").isNotNull())
    ).dropna().dropDuplicates().orderBy(col("serie_id"))

    df_series_cast_filtered = df_series_cast.where(
        (col("serie_id").isNotNull()) &
        (col("cast_id").isNotNull()) &
        (col("cast_gender").isin([1, 2]))
    ).dropna().dropDuplicates().orderBy(col("serie_id"))
```

Isso também foi recriado para os DataFrames relacionados a filmes, sofrendo alterações nas colunas a serem afetadas.

## Parte 3

A parte final do script tem a função de escrever os DataFrames do `PySpark` para `Parquet` e armazená-los na camada Trusted do S3 Bucket. Mas esse armazenamento tinha de ser realizado de maneira particionada, essa divisão ocorrendo por data de envio dos arquivos originais para o bucket. Lembrando que essa data estava no caminho do arquivo dentro do Bucket, ou seja, para isso foi necessário extrair o trecho que armazenava a data. Isso foi realizado com o uso da ferramenta `re`, mais especificamente a função `search`, que buscava no caminho dos arquivos um padrão que fosse igual a uma data no formato utilizado (YYYY/MM/DD). Após isso, utilizei a função `groups` para atribuir o valor de ano, mês e dia separadamente e, por fim, adicionar esses valores ao caminho de escrita dos arquivos. Com isso pronto, só restava o uso da função `write` para salvar os arquivos, e a função completa ficou da seguinte maneira:

```py
def write_partitioned_parquet(df, base_output_path, partition_name, input_file_path):
    match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', input_file_path)
    year, month, day = match.groups()

    output_path = f"{base_output_path}/{partition_name}/{year}/{month}/{day}/"

    df.write.mode("overwrite").parquet(output_path)
```

Como essa era a última tarefa do script, restava somente a função principal para que todas as funções fossem executadas como planejado. A função foi estruturada da seguinte maneira:

```py
def main():

    s3, bucket_name = aws_connection()
    schema_tmdb_series, schema_tmdb_movies = schemas()

    json_series_files_content, json_movies_files_content = read_json_tmdb_files(bucket_name, s3, schema_tmdb_series, schema_tmdb_movies)
    df_series_filtered, df_series_cast_filtered = filter_tmdb_series_dataframe(json_series_files_content)
    df_movies_filtered, df_movies_cast_filtered = filter_tmdb__movies_dataframe(json_movies_files_content)

    base_output_path = args['S3_BASE_OUTPUT']
    response_series = s3.list_objects_v2(Bucket=bucket_name, Prefix=args['S3_INPUT_PATH_TMDB_SERIES'])
    series_files = [f"s3a://{bucket_name}/{content.get('Key')}" for content in response_series.get("Contents", [])]
    for file_path in series_files:
        write_partitioned_parquet(df_series_filtered, base_output_path, "Series", file_path)
        write_partitioned_parquet(df_series_cast_filtered, base_output_path, "Series_Cast", file_path)

    response_movies = s3.list_objects_v2(Bucket=bucket_name, Prefix=args['S3_INPUT_PATH_TMDB_MOVIES'])
    movies_files = [f"s3a://{bucket_name}/{content.get('Key')}" for content in response_movies.get("Contents", [])]
    for file_path in movies_files:
        write_partitioned_parquet(df_movies_filtered, base_output_path, "Movies", file_path)
        write_partitioned_parquet(df_movies_cast_filtered, base_output_path, "Movies_Cast", file_path)
```
