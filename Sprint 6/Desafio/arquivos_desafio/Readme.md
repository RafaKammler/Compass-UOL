# Desafio

O desafio da Sprint 06 se baseava no upload de dois datasets .csv para dentro de um AWS S3 Bucket por meio de um script python, utilizando a biblioteca `boto3`. E após a criação do script foi solicitada a criação de um container docker com volume, que tivesse como função armazenar os arquivos csv e executar o processo python.

## Parte 1

### Conexão com a AWS

A primeira etapa do meu script foi a conexão com o cliente AWS para permitir o a manipulação de arquivos dentro do bucket S3, onde apesar que eu poderia somente adicionar as minhas credenciais ao script, por questões de segurança das chaves de acesso da aws, resolvi criar um arquivo .env para atribuir todas as informações necessárias para login como variáveis de sistema no container, e para realizar a leitura dessas variáveis de sistema e atribuição delas à conexão S3, utilizei da biblioteca `os`, mais especificamente o comando `getenv`, da seguinte maneira:

```py
aws_access_key_id =        os.getenv('aws_access_key_id'),
aws_secret_access_key =    os.getenv('aws_secret_access_key'),
aws_session_token =        os.getenv('aws_session_token')
```

Com a conexão realizada foi possivel realizar a primeira solicitação do desafio, que era a criação de um bucket para armazenar os dados, para isso foi possivel utilizar a função `create_bucket` do `boto3`

### Leitura do arquivo

A próxima etapa que realizei do desafio foi a criação de uma função que realiza a leitura dos arquivos `movies.csv` e `series.csv` que nos foram passados, além disso foi necessária a tradução dos arquivos para bytes para que o conteúdo possa ser usado por uma função posterior, tradução essa que foi feita por meio do `encode`.

### Definição dos caminhos

A próxima etapa do desafio foi definir o caminho em que os arquivos seriam salvos dentro do S3, onde como o caminho do arquivo de filmes e de séries eram muito semelhantes, foi possivel reaproveitar parte do código, sendo o código:

```py
camada_armazenamento =        'Raw'
origem_dados =                'Local'
formato_dados =               'CSV'
especificacao_dados_series =  'Series'
nome_arquivo_series =         'series.csv'

caminho_arquivo_series = f'{camada_armazenamento}/{origem_dados}/{formato_dados}/{especificacao_dados_series}/{ano_atual}/{mes_atual}/{dia_atual}/{nome_arquivo_series}'
```

E para o arquivo de filmes são realizadas alterações somente nas variaveis `especificacao_dados_series` e `nome_arquivo_series` e em abas tudo isso ocorre dentro de uma função que retorna o caminho do arquivo.


### Upload dos arquivos

As ultimas duas funções do arquivo que são diretamente relacionadas ao desafio são as que realizam o upload dos arquivos para o S3, funções essas que recebem como parametros os caminhos dos arquivos e o conteudo em bytes, para que possam ser utilizados em conjunto com o nome do bucket pela função `put_object` para realizar o upload de arquivos para o bucket, função essa que no caso do arquivo filmes deve ser utilizada da seguinte maneira:

```py
    s3_client.put_object(Bucket = nome_bucket, Key = caminho_arquivo_filmes, Body = filmes_csv_bytes)
```

Por fim para que todas as funções do script sejam executadas com sucesso utilizei de uma função chamada main, que chama todas as outras.

## Parte 2

Com a finalização do script python resta somente uma tarefa do desafio, a containerização de tudo, por meio do docker