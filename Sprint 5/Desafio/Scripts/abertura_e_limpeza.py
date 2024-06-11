import boto3
import pandas as pd
import os

def abertura_e_limpeza():


# Definição do bucket e arquivos
    s3_bucket = 'desafio-compass-uol'
    arquivo = 'databases-sujas/salas-de-exibicao-e-complexos.csv'
    arquivo_local = '/tmp/arquivo_local.csv'
    arquivo_corrigido = 'salas-de-exibicao-e-complexos-corrigido.csv'


# Conexão com o s3
    s3 = boto3.client('s3')
    s3.download_file(s3_bucket, arquivo, arquivo_local)


    # abre o arquivo e trata de maneira simples
    conteudo_arquivo = pd.read_csv(arquivo_local, sep=';')
    conteudo_arquivo = conteudo_arquivo.dropna(how='all', subset=['REGISTRO_SALA', 'NOME_SALA']).drop_duplicates()
    conteudo_arquivo = conteudo_arquivo.fillna(0)


    # arrumando datas
    colunas_datas = ['DATA_SITUACAO_SALA', 'DATA_INICIO_FUNCIONAMENTO_SALA', 'DATA_SITUACAO_COMPLEXO']
    for coluna in colunas_datas:
        conteudo_arquivo[coluna] = pd.to_datetime(conteudo_arquivo[coluna], format='%d/%m/%Y', errors='coerce')


# Mandando o arquivo corrigido para o s3
    conteudo_arquivo.to_csv(arquivo_corrigido, sep=';', index=False)
    s3.upload_file(arquivo_corrigido, s3_bucket, 'databases-limpas/salas-de-exibicao-e-complexos-corrigido.csv')


    os.remove(arquivo_local)
    os.remove(arquivo_corrigido)


if __name__ == '__main__':
    abertura_e_limpeza()