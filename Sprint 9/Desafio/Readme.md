# Desafio

## [Código completo](./script_9_glue.py)

### [Script novo sprint 07](/GitHub/Sprint%207/Desafio/lambda/lambda_function_v2.py)

### [Script novo Sprint 08](/GitHub/Sprint%208/Desafio/Tmdb_files/script_tmdb_files_v2.py)

## Realização do Desafio

O desafio da Sprint 09 consistia na criação da camada *refined* por meio de um script Python utilizando a ferramenta PySpark para ser executado no ambiente Glue da AWS. Para a criação dessa nova camada do DataLake, que está sendo construída desde a Sprint 06, era necessário utilizar os dados presentes na camada *trusted*, que foram salvos na sprint passada no formato Parquet.

### Antecedentes

Para que o desafio da Sprint 09 fosse realizado de maneira correta, analisei que precisaria refazer parcialmente desafios anteriores, mais especificamente das Sprints 07 e 08. Essas novas versões dos scripts trouxeram uma maior quantidade de informações a serem extraídas, além de dados que não eram requisitados anteriormente, o que acabou alterando a formatação dos arquivos `.json` da camada *raw*, acarretando na mudança de como estava organizado o script da Sprint 08.

---

## Estrutura e Funcionamento do Script

### Etapa 1

O script, primeiramente, inicia o job Glue e se conecta ao S3 por meio do Boto3, da mesma maneira que já foi realizado nas últimas sprints, já que os arquivos são extraídos e enviados para o DataLake posicionado no S3 Bucket.

A leitura desses arquivos é realizada em duas partes, sendo uma delas a listagem de todos os arquivos presentes no bucket por meio do `list_objects_v2`, arquivos estes que são salvos em uma lista que é usada na segunda parte, da seguinte maneira:

```python
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
parquet_files = [
f"s3a://{bucket_name}/{content.get('Key')}" 
for content in response.get("Contents", [])
]
```

Com a lista que resulta dessa etapa, é possível chegar à segunda parte, que consiste na extração somente do conteúdo de cada um dos arquivos, salvando todos esses conteúdos dentro de outra lista de DataFrames, o que acontece da seguinte maneira:

```python
dataframes = []

df = spark.read.parquet(*parquet_files)
dataframes.append(df)
```

Os DataFrames localizados dentro dessa lista devem ser acessados de alguma maneira. Para isso, acesso-os pelos seus índices, que são equivalentes à ordem em que foram salvos (ordem essa que é definida na função principal). Para isso, utilizo uma *list comprehension* simples, como esta: `csv_movies = dataframes[0]`.

---
### Etapa 2

A segunda etapa do script baseia-se na padronização dos dados, deixando todos os DataFrames semelhantes com as mesmas colunas, para auxiliar futuras uniões de DFs. Isso foi feito com todos os DataFrames originários das funções anteriores, utilizando principalmente as funções `withColumn`, `select`, `orderBy`, `dropDuplicates`, `dropna` e `alias`. A estrutura de cada um dos grupos de DFs ficou da seguinte maneira:

- **Filmes:** `movie_id`, `name`, `genres`, `vote_average`, `vote_count`, `release_date`.  
Ordenação: `vote_count`, Desc
- **Séries:** `serie_id`, `name`, `genres`, `vote_average`, `vote_count`, `release_date`.  
Ordenação: `vote_count`, Desc
- **Atores_Filmes:** `movie_id`, `name`, `character`, `gender`
- **Atores_Séries:** `serie_id`, `name`, `character`, `gender`
- **Gêneros_Filmes_TMDB:** `movie_id`, `genre_id`, `genre_name`
- **Gêneros_Séries_TMDB:** `serie_id`, `genre_id`, `genre_name`

A estrutura utilizada para padronizar os DFs era semelhante à seguinte:

```python
csv_movies = csv_movies\
.select(
    "movie_id",
    col("movie_name").alias("name"),
    col("movie_genres").alias("genres"),
    col("movie_vote_average").alias("vote_average"),
    col("movie_vote_count").alias("vote_count"),
    col("movie_release_date").alias("release_date")
)\
.dropDuplicates()\
.orderBy("movie_vote_count", ascending=False)
```

Além disso, utilizei uma função para transformar o gênero de todos os atores em valores numéricos, sendo 1 = Feminino e 2 = Masculino, da seguinte maneira:

```python
.withColumn("cast_gender",  
        when(csv_movies_cast["cast_gender"] == "actress", "1")
        .when(csv_movies_cast["cast_gender"] == "actor", "2")
        .otherwise(csv_movies_cast["cast_gender"]))
```

---

### Etapa 3

Na próxima etapa, foi realizado o agrupamento dos DFs semelhantes para facilitar ainda mais o futuro dimensionamento das tabelas, criando 4 tabelas a partir das 10 anteriores: uma de filmes, uma de séries, uma de gêneros e uma de atores. No entanto, nesta etapa, foram necessárias várias etapas adicionais em tabelas específicas, conforme descrito abaixo:

#### Tabela Atores

Na tabela dos atores, foi necessária a adição da coluna `id`, que não estava presente, para que os DataFrames possuíssem as mesmas colunas com os mesmos nomes. Para isso, utilizei `withColumn` e `lit(None)` da seguinte maneira:

```python
csv_movies_cast = csv_movies_cast   .withColumn("serie_id", lit(None))
csv_series_cast = csv_series_cast   .withColumn("movie_id", lit(None))
tmdb_movies_cast = tmdb_movies_cast .withColumn("serie_id", lit(None))
tmdb_series_cast = tmdb_series_cast .withColumn("movie_id", lit(None))
```

Após isso, uni os DataFrames por nome por meio do `unionByName` e adicionei a coluna `unique_cast_id`, atribuindo um valor por meio da função `row_number`, que deve ser utilizada da seguinte maneira:

```python
.withColumn("unique_cast_id", row_number().over(Window.orderBy("name")))
```

#### Tabelas de Filmes e Séries

Nas tabelas de filmes e séries, o trabalho foi muito menor, sendo necessário apenas remover as colunas de gênero por meio do `drop` e uni-las por meio do `union`.


#### Tabela Gêneros

A tabela de gêneros foi uma das mais complexas de se construir, pois envolveu o agrupamento e padronização dos gêneros de filmes e séries provenientes de diferentes fontes de dados. O objetivo era criar uma tabela unificada contendo os gêneros associados a cada filme e série, independentemente da origem dos dados.

Para isso, o seguinte processo foi realizado:


- Assim como nas etapas anteriores, foi necessário padronizar as colunas dos DataFrames. Para que os DataFrames `csv_movies` e `csv_series` tivessem as mesmas colunas (`movie_id` e `serie_id`), as colunas ausentes foram adicionadas com valor `None` usando `withColumn` e `lit(None)`.

```python
csv_movies = csv_movies.withColumn("serie_id", lit(None))
csv_series = csv_series.withColumn("movie_id", lit(None))
```


- Em seguida, os gêneros dos filmes e séries provenientes dos DataFrames `csv_movies` e `csv_series` foram agrupados em um único DataFrame chamado `all_genres_csv`. Isso foi feito selecionando as colunas relevantes (`movie_id`, `serie_id`, e `genres`) e unindo os DataFrames por meio do método `union`.

```python
all_genres_csv = csv_movies.select("movie_id", "serie_id", "genres")\
    .union(csv_series.select("movie_id", "serie_id", "genres"))
```


- Para os gêneros provenientes do TMDB, foi realizado um agrupamento para consolidar todos os gêneros de um mesmo filme ou série em um único registro. Isso foi feito usando as funções `groupBy`, `agg`, `concat_ws`, `array_sort`, e `collect_list`. As colunas ausentes foram novamente adicionadas com valor `None`.

```python
tmdb_movies_genres = tmdb_movies_genres.groupBy("movie_id")\
    .agg(concat_ws(",", array_sort(collect_list("genre_name"))).alias("genres"))\
    .withColumn("serie_id", lit(None))
    
tmdb_series_genres = tmdb_series_genres.groupBy("serie_id")\
    .agg(concat_ws(",", array_sort(collect_list("genre_name"))).alias("genres"))\
    .withColumn("movie_id", lit(None))
```


- Após a agregação dos gêneros, os DataFrames `tmdb_movies_genres` e `tmdb_series_genres` foram unidos em um único DataFrame chamado `all_genres_tmdb`.

```python
all_genres_tmdb = tmdb_movies_genres.select("movie_id", "serie_id", "genres")\
    .union(tmdb_series_genres.select("movie_id", "serie_id", "genres"))
```


- Finalmente, todos os gêneros, tanto do CSV quanto do TMDB, foram unificados em um único DataFrame `all_genres`. Para garantir que não houvesse registros duplicados, foi utilizado o método `dropDuplicates` com base nas colunas `movie_id`, `serie_id`, e `genres`.

```python
all_genres = all_genres_tmdb.union(all_genres_csv)
all_genres = all_genres.dropDuplicates(["movie_id", "serie_id", "genres"])
```

### Etapa 4

Na quarta etapa do script, foi realizada a criação das tabelas de dimensão, que são essenciais para a construção do DataWarehouse. Essas tabelas permitem uma organização eficiente dos dados, facilitando futuras consultas e análises. A criação dessas tabelas foi feita com base nos DataFrames processados nas etapas anteriores.

#### Dimensão de Filmes

A tabela de dimensão de filmes (`dim_movies`) foi criada a partir do DataFrame `all_movies`. Esta tabela contém apenas os campos `movie_id` e `name`, representando a identificação e o nome dos filmes. Foram removidas duplicatas para garantir que cada filme apareça apenas uma vez.

```python
dim_movies = all_movies\
.select("movie_id", "name")\
.dropDuplicates(["movie_id"])
```

#### Dimensão de Séries

De forma semelhante à dimensão de filmes, a dimensão de séries (`dim_series`) foi criada a partir do DataFrame `all_series`. Esta tabela contém os campos `serie_id` e `name`, garantindo que cada série seja identificada unicamente.

```python
dim_series = all_series\
.select("serie_id", "name")\
.dropDuplicates(["serie_id"])
```

#### Dimensão de Tempo

A dimensão de tempo (`dim_time`) foi criada unificando os DataFrames de filmes e séries. Essa tabela é organizada pela data de lançamento (`release_date`), garantindo a ausência de duplicatas para cada data. Além disso, foi criada uma coluna adicional, `unique_time_id`, que gera um identificador único para cada data de lançamento.

```python
dim_time = all_movies\
.union(all_series)\
.orderBy("release_date", ascending=True)\
.dropDuplicates(["release_date"])\
.withColumn("unique_time_id", monotonically_increasing_id())\
.select("unique_time_id", "release_date")
```

#### Dimensão de Gêneros

Para a dimensão de gêneros (`dim_genres`), o script agrupa os gêneros de filmes e séries em um único DataFrame. Foi criada uma coluna `unique_genre_id` usando a função `row_number` para fornecer um identificador único para cada combinação de gênero e obra (filme ou série).

```python
window_spec = Window.orderBy("movie_id")
dim_genres = all_genres\
.withColumn("unique_genre_id", row_number().over(window_spec))\
.select("unique_genre_id", "movie_id", "serie_id", "genres")
```

#### Dimensão de Elenco

Por fim, a dimensão de elenco (`dim_cast`) foi criada com base no DataFrame `all_casts`, contendo informações sobre o elenco, como identificador único (`unique_cast_id`), `movie_id`, `serie_id`, `name`, `character` e `gender`. As duplicatas foram removidas e os registros foram ordenados pelo identificador único do elenco.

```python
dim_cast = all_casts\
.dropDuplicates()\
.orderBy("unique_cast_id", ascending=True)\
.select("unique_cast_id", "movie_id", "serie_id", "name", "character", "gender")
```

### Etapa 5

Na quinta etapa do script, foi realizada a criação da tabela fato, uma parte crucial na construção do DataWarehouse. A tabela fato (`media_fact`) é onde os dados das dimensões previamente criadas são combinados para fornecer uma visão integrada e analítica dos filmes e séries. Abaixo está uma explicação detalhada sobre como essa tabela foi criada:

#### Unificação dos DataFrames de Filmes e Séries

Primeiramente, os DataFrames de filmes (`all_movies`) e séries (`all_series`) foram unificados em um único DataFrame (`all_media`). Para garantir a integridade dos dados durante essa unificação, foi necessário adicionar colunas nulas (`serie_id` para filmes e `movie_id` para séries) para que ambos os DataFrames tivessem a mesma estrutura.

```python
all_movies = all_movies.withColumn("serie_id", lit(None))
all_series = all_series.withColumn("movie_id", lit(None))

all_media = all_movies.unionByName(all_series)
```

#### Renomeação das Colunas nas Dimensões de Elenco e Gêneros

Antes de realizar as junções, as colunas das tabelas de dimensão de elenco (`dim_cast`) e gêneros (`dim_genres`) foram renomeadas para evitar conflitos durante as operações de junção. Especificamente, as colunas `movie_id` e `serie_id` foram renomeadas para `cast_movie_id`, `cast_serie_id`, `genre_movie_id`, e `genre_serie_id`, conforme a dimensão correspondente.

```python
dim_cast = dim_cast.withColumnRenamed("movie_id", "cast_movie_id").withColumnRenamed("serie_id", "cast_serie_id")
dim_genres = dim_genres.withColumnRenamed("movie_id", "genre_movie_id").withColumnRenamed("serie_id", "genre_serie_id")
```

#### Junção com a Dimensão de Tempo

O primeiro passo na criação da tabela fato foi realizar a junção do DataFrame unificado `all_media` com a dimensão de tempo (`dim_time`). Esta junção foi feita utilizando a coluna `release_date`, garantindo que cada filme ou série fosse associado com o identificador único de tempo (`unique_time_id`).

```python
media_fact = all_media.join(dim_time, "release_date", "left")
```

#### Junção com a Dimensão de Elenco

Em seguida, a tabela fato foi enriquecida com os dados da dimensão de elenco (`dim_cast`). A junção foi realizada utilizando tanto `movie_id` quanto `serie_id`, associando o elenco correspondente a cada filme ou série. A junção foi feita de forma que, se não houvesse correspondência direta (por exemplo, se um filme não tivesse um elenco listado), o resultado ainda manteria os dados existentes na tabela fato.

```python
media_fact = media_fact.join(
dim_cast,
(media_fact.movie_id == dim_cast.cast_movie_id) | (media_fact.serie_id == dim_cast.cast_serie_id),
"left"
)
```

#### Junção com a Dimensão de Gêneros

A última junção foi realizada com a dimensão de gêneros (`dim_genres`), novamente utilizando tanto `movie_id` quanto `serie_id`. Esta operação associou os gêneros correspondentes a cada entrada na tabela fato, completando a estrutura necessária para análises complexas.

```python
media_fact = media_fact.join(
dim_genres,
(media_fact.movie_id == dim_genres.genre_movie_id) | (media_fact.serie_id == dim_genres.genre_serie_id),
"left"
)
```

#### Seleção Final das Colunas

Depois de todas as junções, a tabela fato foi refinada para incluir apenas as colunas mais relevantes para análise: `movie_id`, `serie_id`, `unique_time_id`, `unique_cast_id`, `unique_genre_id`, `vote_average`, e `vote_count`. Finalmente, a tabela foi ordenada pelo número de votos (`vote_count`) em ordem decrescente, facilitando a identificação dos filmes e séries mais populares.

```python
media_fact = media_fact.select(
"movie_id",
"serie_id",
"unique_time_id",
"unique_cast_id",
"unique_genre_id",
"vote_average",
"vote_count"
).orderBy("vote_count", ascending=False)
```

### Etapa 6

A última etapa do script envolve a escrita dos DataFrames refinados de volta ao Amazon S3 e a construção da função principal (`main`). Esta etapa conclui o processo de criação da camada *refined* do DataLake, garantindo que todos os dados estejam devidamente processados, estruturados e armazenados para futuras análises.

#### Escrita dos DataFrames no S3

Para salvar os DataFrames refinados, foi criada uma função chamada `writing_to_s3`. Essa função recebe três parâmetros: o DataFrame a ser salvo (`df`), o nome do bucket S3 (`bucket_name`), e o caminho (`path`) onde o arquivo Parquet será salvo dentro do bucket.

```python
def writing_to_s3(df, bucket_name, path):
df.write.mode("overwrite").parquet(f"s3a://{bucket_name}/{path}")
```

A função utiliza o método `write` do PySpark para salvar os arquivos no formato Parquet, substituindo qualquer arquivo existente no destino especificado. 

#### Construção da Função Principal (`main`)

A função `main` é o coração do script, orquestrando todas as funções previamente definidas para realizar a tarefa de criação da camada *refined*. Ela é responsável por executar cada etapa do processo, desde a conexão com a AWS até a escrita dos dados refinados no S3.


A função se conecta à AWS utilizando a função `aws_connection` e, em seguida, lê os arquivos Parquet armazenados no S3 usando a função `reading_parquet_files`. Os prefixos que indicam os caminhos dos arquivos dentro do bucket são definidos em uma lista e passados para a função de leitura.

```python
s3, bucket_name = aws_connection()
prefixes = [
    'Trusted/CSV/Movies/',
    'Trusted/CSV/Series/',
    ...
]
dataframes = reading_parquet_files(s3, bucket_name, prefixes)
```

Os DataFrames lidos são separados conforme sua origem (CSV ou TMDB) utilizando as funções `separating_csv_dataframes` e `separating_tmdb_dataframes`.

```python
csv_movies, csv_series, csv_movies_cast, csv_series_cast = separating_csv_dataframes(dataframes)
tmdb_movies, tmdb_series, tmdb_movies_cast, tmdb_series_cast, tmdb_movies_genders, tmdb_series_genders = separating_tmdb_dataframes(dataframes)
```


As funções de padronização (`padronizing_csv_dfs` e `padronizing_tmdb_dfs`) são chamadas para garantir que todos os DataFrames tenham uma estrutura consistente, facilitando as etapas subsequentes de agrupamento e união.

```python
csv_movies_padronized, csv_series_padronized, csv_movies_cast_padronized, csv_series_cast_padronized = padronizing_csv_dfs(csv_movies, csv_series, csv_movies_cast, csv_series_cast)
tmdb_movies_padronized, tmdb_series_padronized, tmdb_movies_cast_padronized, tmdb_series_cast_padronized, tmdb_movies_genders_padronized, tmdb_series_genders_padronized = padronizing_tmdb_dfs(tmdb_movies, tmdb_series, tmdb_movies_cast, tmdb_series_cast, tmdb_movies_genders, tmdb_series_genders)
```


Os DataFrames padronizados são agrupados em conjuntos específicos para filmes, séries, elenco, e gêneros usando as funções `grouping_all_actors`, `grouping_all_movies`, `grouping_all_series`, e `grouping_all_genres`.

```python
all_cast = grouping_all_actors(csv_movies_cast_padronized, csv_series_cast_padronized, tmdb_movies_cast_padronized, tmdb_series_cast_padronized)
all_movies = grouping_all_movies(csv_movies_padronized, tmdb_movies_padronized)
all_series = grouping_all_series(csv_series_padronized, tmdb_series_padronized)
all_genres = grouping_all_genres(tmdb_movies_genders_padronized, tmdb_series_genders_padronized, csv_movies_padronized, csv_series_padronized)
```


As funções `creating_dim_tables` e `creating_fact_table` são então chamadas para gerar as tabelas dimensão e a tabela fato que compõem a camada *refined*.

```python
dim_movies, dim_series, dim_time, dim_cast, dim_genres = creating_dim_tables(all_cast, all_series, all_movies, all_genres)
fact_table = creating_fact_table(dim_movies, dim_series, dim_time, dim_cast, all_cast, all_movies, all_series, dim_genres)
```


Por fim, os DataFrames são escritos no S3. Para isso, são definidos os caminhos onde cada DataFrame deve ser salvo e a função `writing_to_s3` é chamada em um loop para salvar cada um deles.

```python
paths = [
    "Refined/dim_movies/",
    "Refined/dim_series/",
    "Refined/dim_time/",
    "Refined/dim_cast/",
    "Refined/dim_genres/",
    "Refined/fact_table/"
]

dataframes = [dim_movies, dim_series, dim_time, dim_cast, dim_genres, fact_table]

for df, path in zip(dataframes, paths):
    writing_to_s3(df, bucket_name, path)
```
