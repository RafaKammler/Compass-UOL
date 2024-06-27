# Desafio

O desafio da Sprint 06 se baseava no upload de dois datasets .csv para dentro de um AWS S3 Bucket por meio de um script python, utilizando a biblioteca `boto3`. E após a criação do script foi solicitada a criação de um container docker com volume, que tivesse como função armazenar os arquivos csv e executar o processo python.

## Parte 1

Leitura dos arquivos csv, onde para isso uitlizei da biblioteca pandas com o comando `read_csv` para leitura e o comando `to_csv` para transferir isso como arquivo csv novo que seria enviado para o S3. Além da utilização do `encode('utf-8')` para converter o conteúdo do arquivo em bytes, que é a unica maneira que o comando `put_object` que será usado posteriormente aceita conteúdo

## Parte 2

### Credenciais

Na segunda etapa foi solicitado o uso do `boto3` para conexão e upload para a AWS, onde para essa etapa foi necessária a importação das credenciais AWS para o script, onde para evitar inserir as credenciais AWS dentro do script python, principalmente por questões de segurança, resolvi criar um arquivo .json que tem como conteudo as credenciais, arquivo esse que não será exportado para lugar nenhum, com o intuito de manter as credenciais em segredo, e para a leitura desse arquivo por parte do script eu utilizei a seguinte sequencia de comandos:

```py

    with open("aws_credentials.json") as f:
        credenciais = json.load(f)

    for chave, valor in credenciais.items():
        credenciais_dict[chave] = valor

    s3_client = boto3.client('s3', 
                        region_name =              'us-east-1',
                        aws_access_key_id =        credenciais_dict['aws_access_key_id'],
                        aws_secret_access_key =    credenciais_dict['aws_secret_access_key'],
                        aws_session_token =        credenciais_dict['aws_session_token'])

```

Que primeiramente utiliza o `json.load()` para a leitura do arquivo das credenciais, depois salva os conjuntos chave, valor dentro de uma variavel, e por fim atribui esses valores as credenciais AWS solicitadas pelo boto3

## Parte 3

Com a conexão bem sucedida com a AWS, o restante tem um nivel menor de dificuldade, já que para a terceira etapa foi necessária somente a criação do bucket pelo comando `create_bucket`

Depois da criação do bucket tive que definir o caminho do arquivo, que teve que ser definido como solicitado pelo desafio, onde para isso tive que inicialmente utilizar:

```py
    nome_bucket =                 'datalake-desafio-compassuol-rafael'
    camada_armazenamento =        'Raw'
    origem_dados =                'Local'
    formato_dados =               'CSV'
    especificacao_dados_filmes =  'Movies'
    especificacao_dados_series =  'Series'
    nome_arquivo_filmes =         'movies.csv'
    nome_arquivo_series =         'series.csv'
    ano_atual = datetime.datetime.now().strftime('%Y')
    mes_atual = datetime.datetime.now().strftime('%m')  
    dia_atual = datetime.datetime.now().strftime('%d')  
```

Sendo tudo isso utilizado para definir as váriaveis do caminho, váriaveis essas que são utilizadas da seguinte maneira:

```py
    caminho_arquivo_filmes = f'{camada_armazenamento}/{origem_dados}/{formato_dados}/{especificacao_dados_filmes}/{ano_atual}/{mes_atual}/{dia_atual}/{nome_arquivo_filmes}'
    caminho_arquivo_series = f'{camada_armazenamento}/{origem_dados}/{formato_dados}/{especificacao_dados_series}/{ano_atual}/{mes_atual}/{dia_atual}/{nome_arquivo_series}'


    s3_client.put_object(Bucket = nome_bucket, Key = caminho_arquivo_filmes, Body = filmes_csv_bytes)
    print(f"Arquivo {nome_arquivo_filmes} enviado com sucesso")
    ''
    s3_client.put_object(Bucket = nome_bucket, Key = caminho_arquivo_series, Body = series_csv_bytes)
    print(f"Arquivo {nome_arquivo_series} enviado com sucesso")
```

Funcionando com a utilização das variáveis definidas anteriormente como o nome do bucket e caminho do arquivo, além do conteúdo que foi anteriormente traduzido para bytes como conteúdo do mesmo, tudo isso por meio do comando `put_object`

## Parte 4