# Desafio

O desafio foi separado em dois trechos, sendo cada um deles um script que tem como intuito enviar uma parte dos dados da camada Raw para a camada Trusted, sendo esta primeira parte a do envio dos dados locais, ou seja, os que estavam em formato CSV. Para cada uma dessas partes foi necessária a criação de um AWS Glue Job, com parâmetros diferentes.

## Parte 1 - Leitura dos dados

O primeiro trecho do script tem como função ler todos os dados que estão localizados no AWS S3 Bucket. Para inicialmente fazer a leitura desses objetos, é necessária a conexão com a AWS, por meio da biblioteca boto3, o que é extremamente facilitado dentro de um ambiente Glue, já que as variáveis de ambiente AWS já estão localizadas internamente. Para isso, criei a seguinte função:

```py
def aws_connection():
    s3 = boto3.client('s3')
    bucket_name = "datalake-desafio-compassuol-rafael"
    return s3, bucket_name
```

Após essa conexão, foi necessário realizar a leitura dos objetos por meio da função `list_objects_v2`, que lista todos os arquivos que estão dentro de um caminho S3 e seus conteúdos. Para que o conteúdo de todos esses arquivos seja armazenado dentro de um DataFrame Spark, é necessário o uso da função:

```py
    response_movies = s3.list_objects_v2(Bucket = bucket_name, Prefix = args['S3_INPUT_PATH_CSV_MOVIES'])
    csv_movies_files_content = spark.read\
        .option("header", "true")\
        .option("sep", "|")\
        .csv([
        f"s3a://{bucket_name}/{content.get('Key')}" 
        for content in response_movies.get("Contents")
        ])
```

Essa função lê o conteúdo de todos os CSVs, definindo a existência de um cabeçalho e o separador das colunas como `|`, além de iterar sobre todos os arquivos para resgatar somente os seus conteúdos, ignorando o resto das informações que são adquiridas pelo `list_objects`. Essa função é utilizada duas vezes, mudando os parâmetros de caminho, para que os dados das séries também sejam lidos.

## Parte 2 - Selecionando colunas

A próxima parte consiste na seleção das colunas que futuramente serão utilizadas em pesquisas. Com o uso do `select`, foi possível realizar essa filtragem sem muitas dificuldades, resultando na redução da quantidade de informações dos novos DataFrames, removendo todas as informações julgadas inúteis para as pesquisas. Essa seleção ocorreu de maneira diferente para cada um dos DataFrames, mas segue um exemplo de como foi feita para um deles:

```py
    csv_series_correct = csv_series.select(
        col("id"),
        col("tituloOriginal"),
        col("tituloPincipal"),
        col("genero"),
        col("anoLancamento"),
        col("notaMedia"),
        col("numeroVotos")    
    )
```

## Parte 3 - Organizando os Dataframes

Para a melhor organização e padronização dos DataFrames, realizei algumas mudanças, limpezas e transformações nas colunas, que são divididas em três etapas:

### Etapa 1 - Renomeação das colunas

A primeira etapa para padronizar todos os DataFrames foi a renomeação das colunas. Para isso, utilizei o comando `withColumnRenamed`, que recebe primeiramente o nome original da coluna e após isso o `alias` para ela. Isso foi realizado para todas as colunas, da seguinte maneira:

```py
.withColumnRenamed("id",             "movie_id")\
.withColumnRenamed("tituloOriginal", "movie_original_name")\
.withColumnRenamed("tituloPincipal", "movie_name")\
.withColumnRenamed("genero",         "movie_genres")\
.withColumnRenamed("anoLancamento",  "movie_release_date")\
.withColumnRenamed("notaMedia",      "movie_vote_average")\
.withColumnRenamed("numeroVotos",    "movie_vote_count")\
```

### Etapa 2 - Tipagem das colunas

Após isso, transformei as colunas que estavam todas em string para seu respectivo tipo correto. Apesar de serem poucas colunas que precisavam dessa alteração, ainda era uma tarefa necessária, já que se fosse realizada alguma pesquisa nas tabelas que estivesse relacionada ao valor numérico das colunas, não funcionaria devido à tipagem string. Para isso, utilizei a função `withColumn`, definindo qual a coluna a ser alterada e usando o `cast` para definir o tipo dos dados presentes na coluna. Isso também foi realizado em todos os DataFrames, e a tipagem de colunas segue o seguinte exemplo para todas as situações:

```py
.withColumn("serie_first_air_date",  col("serie_first_air_date") .cast("int"))\
.withColumn("serie_vote_average",    col("serie_vote_average")   .cast("float"))\
.withColumn("serie_vote_count",      col("serie_vote_count")     .cast("int"))
```

### Etapa 3 - Filtragem dos dados

A terceira etapa dessa organização consiste na filtragem dos dados para remoção de registros nulos, duplicados e ordenação das colunas, que foi a mesma para todos os DataFrames, sendo ordenados por ID do filme/série, da seguinte maneira:

```py
.where(col("serie_id").isNotNull() & col("cast_name").isNotNull())\
.dropna()\
.dropDuplicates()\
.orderBy(col("serie_id"))
```

## Parte 4

Por fim, o último trecho é bem simples e tem como função escrever os DataFrames que resultam das filtragens anteriores dentro da camada Trusted no Datalake utilizado nas sprints anteriores, que está localizado no S3 Bucket, no formato parquet. Para isso, foi utilizado somente um comando que recebe o conteúdo de cada um dos arquivos e o caminho dos mesmos por meio da função main:

```py
def write_to_parquet(df, bucket_name, base_output_path, partition_name):
    df.write.mode("overwrite").parquet(f"s3a://{bucket_name}/{base_output_path}/{partition_name}")
```

## Execução

Para utilizar todas as funções de maneira correta, garantindo que a execução ocorra como desejada, utilizei uma função principal, a main, que ficou organizada da seguinte maneira:

```py
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

```