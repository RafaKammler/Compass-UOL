import boto3
import pandas as pd
import datetime
import json


def data_upload():
# definição das variaveis do caminho do arquivo

    nome_bucket =                 'datalake-desafio-compassuol-rafael'
    camada_armazenamento =        'Raw'
    origem_dados =                'Local'
    formato_dados =               'CSV'
    especificacao_dados_filmes =  'Movies'
    especificacao_dados_series =  'Series'
    nome_arquivo_filmes =         'movies.csv'
    nome_arquivo_series =         'series.csv'
    credenciais_dict =            {}


#leitura das credenciais AWS

    with open("aws_credentials.json") as f:
        credenciais = json.load(f)


# gravação das credenciais em um dicionario

    for chave, valor in credenciais.items():
        credenciais_dict[chave] = valor


#definição das variáveis de data

    ano_atual = datetime.datetime.now().strftime('%Y')
    mes_atual = datetime.datetime.now().strftime('%m')  
    dia_atual = datetime.datetime.now().strftime('%d')  




# conexão com o S3

    s3_client = boto3.client('s3', 
                        region_name =              'us-east-1',
                        aws_access_key_id =        credenciais_dict['AWS_ACCESS_KEY_ID'],
                        aws_secret_access_key =    credenciais_dict['AWS_SECRET_ACCESS_KEY'],
                        aws_session_token =        credenciais_dict['AWS_SESSION_TOKEN'])
    s3_client.create_bucket(Bucket = nome_bucket)


# Leitura e conversão dos arquivos csv

    conteudo_filmes = pd.read_csv('/data/movies.csv', sep='|')
    conteudo_series = pd.read_csv('/data/series.csv', sep='|')

    filmes_csv_string = conteudo_filmes.to_csv(index=False, sep='|')
    series_csv_string = conteudo_series.to_csv(index=False, sep='|')


# Tradução do csv para bytes

    filmes_csv_bytes = filmes_csv_string.encode('utf-8')
    series_csv_bytes = series_csv_string.encode('utf-8')


# Caminho do arquivo no S3

    caminho_arquivo_filmes = f'{camada_armazenamento}/{origem_dados}/{formato_dados}/{especificacao_dados_filmes}/{ano_atual}/{mes_atual}/{dia_atual}/{nome_arquivo_filmes}'
    caminho_arquivo_series = f'{camada_armazenamento}/{origem_dados}/{formato_dados}/{especificacao_dados_series}/{ano_atual}/{mes_atual}/{dia_atual}/{nome_arquivo_series}'


# Upload dos arquivos

    s3_client.put_object(Bucket = nome_bucket, Key = caminho_arquivo_filmes, Body = filmes_csv_bytes)
    print(f"Arquivo {nome_arquivo_filmes} enviado com sucesso")
    ''
    s3_client.put_object(Bucket = nome_bucket, Key = caminho_arquivo_series, Body = series_csv_bytes)
    print(f"Arquivo {nome_arquivo_series} enviado com sucesso")

data_upload()

