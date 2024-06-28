# Desafio

O desafio da Sprint 06 se baseava no upload de dois datasets .csv para dentro de um AWS S3 Bucket por meio de um script Python, utilizando a biblioteca `boto3`. Após a criação do script, foi solicitada a criação de um container Docker com volume, que tivesse como função armazenar os arquivos .csv e executar o script Python.

## Parte 1

### Conexão com a AWS

A primeira etapa do meu script foi a conexão com o cliente AWS para permitir a manipulação de arquivos dentro do bucket S3. Embora eu pudesse simplesmente adicionar minhas credenciais ao script, por questões de segurança das chaves de acesso da AWS, resolvi criar um arquivo .env para atribuir todas as informações necessárias para login como variáveis de sistema no container. Para realizar a leitura dessas variáveis de sistema e atribuição delas à conexão S3, utilizei a biblioteca `os`, mais especificamente o comando `getenv`, da seguinte maneira:

```py
aws_access_key_id =        os.getenv('aws_access_key_id'),
aws_secret_access_key =    os.getenv('aws_secret_access_key'),
aws_session_token =        os.getenv('aws_session_token')
```

Com a conexão realizada, foi possível atender à primeira solicitação do desafio, que era a criação de um bucket para armazenar os dados. Para isso, utilizei a função `create_bucket` do `boto3`.

### Leitura do arquivo

A próxima etapa que realizei do desafio foi a criação de uma função que realiza a leitura dos arquivos `movies.csv` e `series.csv` que nos foram passados. Esses arquivos se encontram no diretório `/data` dentro do volume do container. Além disso, foi necessária a tradução dos arquivos para bytes para que o conteúdo possa ser usado por uma função posterior. Essa tradução foi feita por meio do `encode`.


### Definição dos caminhos

A próxima etapa do desafio foi definir o caminho em que os arquivos seriam salvos dentro do S3. Como o caminho do arquivo de filmes e de séries era muito semelhante, foi possível reaproveitar parte do código, sendo o código:


```py
camada_armazenamento =        'Raw'
origem_dados =                'Local'
formato_dados =               'CSV'
especificacao_dados_series =  'Series'
nome_arquivo_series =         'series.csv'

caminho_arquivo_series = f'{camada_armazenamento}/{origem_dados}/{formato_dados}/{especificacao_dados_series}/{ano_atual}/{mes_atual}/{dia_atual}/{nome_arquivo_series}'
```

E para o arquivo de filmes, são realizadas alterações somente nas variáveis `especificacao_dados_series` e `nome_arquivo_series`. Tudo isso ocorre dentro de uma função que retorna o caminho do arquivo.


### Upload dos arquivos

As últimas duas funções do arquivo que são diretamente relacionadas ao desafio são as que realizam o upload dos arquivos para o S3. Essas funções recebem como parâmetros os caminhos dos arquivos e o conteúdo em bytes, para que possam ser utilizados em conjunto com o nome do bucket pela função `put_object` para realizar o upload de arquivos para o bucket. A função, no caso do arquivo de filmes, deve ser utilizada da seguinte maneira:

```py
    s3_client.put_object(Bucket = nome_bucket, Key = caminho_arquivo_filmes, Body = filmes_csv_bytes)
```

Por fim para que todas as funções do script sejam executadas com sucesso utilizei de uma função chamada main, que chama todas as outras.

## Parte 2

Com a finalização do script Python, resta somente uma tarefa do desafio: a containerização de tudo por meio do Docker. O primeiro passo para a containerização foi a criação de um Dockerfile que execute o script Python. Esse Dockerfile define o diretório de trabalho, o volume a ser usado, copia todos os arquivos necessários, instala todas as bibliotecas listadas no arquivo `requirements.txt` e, por fim, executa o script que realiza o upload dos arquivos.

Mas para a criação do volume e uso do arquivo `.env` com as credenciais, foi necessária a criação de um docker-compose. Este realiza inicialmente a criação do volume para armazenar os arquivos CSV a serem usados pelo script. A criação desse volume envolve duas etapas, sendo a primeira a criação do `Dockerfile.init`, que transfere os arquivos locais para dentro do volume, feita da seguinte maneira:

```Dockerfile
FROM busybox

COPY ./csv_files /data
```

Após isso, é necessária a integração com o arquivo `docker-compose` por meio da criação de um serviço que constrói a imagem usando o Dockerfile anterior e define em qual volume e diretório serão salvos os arquivos.

Após a criação bem-sucedida do volume, foi necessário criar o serviço que executa o script Python. Isso envolve construir a imagem de upload dos arquivos, montar os volumes necessários para o funcionamento da imagem, definir as variáveis de ambiente do container e, por fim, o nome do container a ser criado, tudo da seguinte maneira:

```yml
    build: ./file_upload/
    volumes: 
      - "./file_upload:/app"
      - csv_files:/data
    environment:
      - aws_access_key_id=${aws_access_key_id}
      - aws_secret_access_key=${aws_secret_access_key}
      - aws_session_token=${aws_session_token}
    container_name: data_raw_upload
```


Com todas as etapas realizadas, é possível executar todos os scripts do desafio com o simples comando `docker-compose up`.