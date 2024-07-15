import boto3
import json
import requests
import datetime
import os

# Configuração do cliente S3
s3 = boto3.client('s3', 
                  region_name='us-east-1',
                  aws_access_key_id=os.getenv('aws_access_key_id'),
                  aws_secret_access_key=os.getenv('aws_secret_access_key'),
                  aws_session_token=os.getenv('aws_session_token'))
nome_bucket = 'datalake-desafio-compassuol-rafael'

# Definição da data atual
ano_atual = datetime.datetime.now().strftime('%Y')
mes_atual = datetime.datetime.now().strftime('%m')
dia_atual = datetime.datetime.now().strftime('%d')

# Leitura do token TMDB
headers = json.load(open('credential.json', 'r'))
cache = {}

# Função para encontrar informações de atores principais de cada filme e série
def info_atores_principais(tmdb_id, tipo_dado, headers, cache):
    cache_key = f"{tipo_dado}_{tmdb_id}"
    if cache_key in cache:
        return cache[cache_key]

    url_info_atores = f"https://api.themoviedb.org/3/{tipo_dado}/{tmdb_id}/credits?language=en-US"
    resposta_api_atores = requests.get(url_info_atores, headers=headers)
    if resposta_api_atores.status_code != 200:
        return []
    informacoes_cast = resposta_api_atores.json()["cast"][:3]
    info_atores = []

# Resgata as informações dos atores principais, além de checar se já existem em cache
    for ator in informacoes_cast:
        cache_ator = f"actor_{ator['id']}"
        if cache_ator in cache:
            detalhes_atores = cache[cache_ator]
        else:
            url_detalhe = f"https://api.themoviedb.org/3/person/{ator['id']}?language=en-US"
            response_atores = requests.get(url_detalhe, headers=headers)
            detalhes_atores = {}
            if response_atores.status_code == 200:
                detalhes_atores = response_atores.json()
                cache[cache_ator] = detalhes_atores

# Adiciona as informações dos atores principais a lista
        info_atores.append({
            "id": ator["id"],
            "nome": ator["name"],
            "personagem": ator["character"],
            "data_nascimento": detalhes_atores.get("birthday"),
            "sexo_ator": detalhes_atores.get("gender")
        })
    cache[cache_key] = info_atores
    return info_atores

# Função para realizar a requisição e upload dos dados de filmes e séries
def request_e_upload_dados(qtd_arquivos):
    # loop para definir a quantidade de arquivos a serem salvos
    for j in range(1, qtd_arquivos + 1):
        conteudo_filmes = []
        conteudo_series = []

        # loop para realizar a requisição de 5 páginas de filmes e séries por arquivo, para que não ultrapassem 100 registros por arquivo
        for i in range(1, 6):
            # requisições da API para encontrar filmes e séries do gênero crime, ordenados por quantidade de votos
            url_filme = f"https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=en-US&with_genres=80&sort_by=vote_count.desc&page={(j-1)*5 + i}&release_date.lte={ano_atual}-{mes_atual}-{dia_atual}"
            url_serie = f"https://api.themoviedb.org/3/discover/tv?include_adult=false&include_video=false&language=en-US&with_genres=80&sort_by=vote_count.desc&page={(j-1)*5 + i}&first_air_date.lte={ano_atual}-{mes_atual}-{dia_atual}"
            response_filmes = requests.get(url_filme, headers=headers)
            response_series = requests.get(url_serie, headers=headers)

            # Verificações para verificar se a requisição foi bem sucedida
            if response_filmes.status_code == 200:
                filmes = response_filmes.json()["results"]
                # loop que utiliza a função anterior para encontrar informações dos atores principais de cada filme
                for filme in filmes:
                    informacoes_cast = info_atores_principais(filme["id"], "movie", headers, cache)
                    filme["cast"] = informacoes_cast
                conteudo_filmes.extend(filmes)

            # realiza a mesma verificação para as séries
            if response_series.status_code == 200:
                series = response_series.json()["results"]
                for serie in series:
                    cast_info = info_atores_principais(serie["id"], "tv", headers, cache)
                    serie["cast"] = cast_info
                conteudo_series.extend(series)

        # define o caminho dentro do S3
        caminho_arquivo_series = f'Raw/TMDB/JSON/Series/{ano_atual}/{mes_atual}/{dia_atual}/top_series_crime_{j}.json'
        caminho_arquivo_filmes = f'Raw/TMDB/JSON/Movies/{ano_atual}/{mes_atual}/{dia_atual}/top_filmes_crime_{j}.json'

        # salva os arquivos no S3
        s3.put_object(Body=json.dumps(conteudo_series, indent=4, ensure_ascii=False).encode('utf-8'), Bucket=nome_bucket, Key=caminho_arquivo_series)
        s3.put_object(Body=json.dumps(conteudo_filmes, indent=4, ensure_ascii=False).encode('utf-8'), Bucket=nome_bucket, Key=caminho_arquivo_filmes)

    # imprime mensagem de sucesso
    print('Arquivos Movies e Series salvos com sucesso!')

# Função principal para executar o código no lambda
def lambda_handler(event, context):
    request_e_upload_dados(30)
    return {
        'statusCode': 200,
        'statusMessage': 'OK'
    }
