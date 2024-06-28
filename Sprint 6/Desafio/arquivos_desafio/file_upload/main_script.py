import boto3
import pandas as pd
import datetime
import os

# Conexão com a AWS
s3_client = boto3.client('s3', 
                    region_name =              'us-east-1',
                    aws_access_key_id =        os.getenv('aws_access_key_id'),
                    aws_secret_access_key =    os.getenv('aws_secret_access_key'),
                    aws_session_token =        os.getenv('aws_session_token'))
nome_bucket =                                  'datalake-desafio-compassuol-rafael'

# Data do sitema
ano_atual = datetime.datetime.now().strftime('%Y')
mes_atual = datetime.datetime.now().strftime('%m')  
dia_atual = datetime.datetime.now().strftime('%d')  

# Criação do bucket
s3_client.create_bucket(Bucket = nome_bucket)


def leitura_do_arquivo():

# lê e retorna o conteúdo dos arquivos em bytes
    conteudo_filmes = pd.read_csv('/data/movies.csv', sep='|')
    conteudo_series = pd.read_csv('/data/series.csv', sep='|')


    filmes_csv_string = conteudo_filmes.to_csv(index=False, sep='|')
    series_csv_string = conteudo_series.to_csv(index=False, sep='|')


    filmes_csv_bytes = filmes_csv_string.encode('utf-8')
    series_csv_bytes = series_csv_string.encode('utf-8')
    return filmes_csv_bytes, series_csv_bytes


def definicao_caminho_filme():

    # Define e retorna o caminho do arquivo de filmes
    camada_armazenamento =        'Raw'
    origem_dados =                'Local'
    formato_dados =               'CSV'
    especificacao_dados_filmes =  'Movies'
    nome_arquivo_filmes =         'movies.csv'
    
    
    caminho_arquivo_filmes = f'{camada_armazenamento}/{origem_dados}/{formato_dados}/{especificacao_dados_filmes}/{ano_atual}/{mes_atual}/{dia_atual}/{nome_arquivo_filmes}'
    return caminho_arquivo_filmes


def definicao_caminho_series():

    # Define e retorna do arquivo de series
    camada_armazenamento =        'Raw'
    origem_dados =                'Local'
    formato_dados =               'CSV'
    especificacao_dados_series =  'Series'
    nome_arquivo_series =         'series.csv'

    caminho_arquivo_series = f'{camada_armazenamento}/{origem_dados}/{formato_dados}/{especificacao_dados_series}/{ano_atual}/{mes_atual}/{dia_atual}/{nome_arquivo_series}'
    return caminho_arquivo_series


def file_upload_filmes(caminho_arquivo_filmes, filmes_csv_bytes):

# Envia o arquivo de filmes para o bucket
    s3_client.put_object(Bucket = nome_bucket, Key = caminho_arquivo_filmes, Body = filmes_csv_bytes)
    print("Arquivo filmes enviado com sucesso")


def file_upload_series(caminho_arquivo_series, series_csv_bytes):
# Envia o arquivo de series para o bucket
    s3_client.put_object(Bucket = nome_bucket, Key = caminho_arquivo_series, Body = series_csv_bytes)
    print("Arquivo series enviado com sucesso")


def main():
    filmes_csv_bytes, series_csv_bytes = leitura_do_arquivo()
    caminho_arquivo_filmes = definicao_caminho_filme()
    caminho_arquivo_series = definicao_caminho_series()
    file_upload_filmes(caminho_arquivo_filmes, filmes_csv_bytes)
    file_upload_series(caminho_arquivo_series, series_csv_bytes)

main()
