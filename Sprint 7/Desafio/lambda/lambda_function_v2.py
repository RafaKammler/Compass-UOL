import boto3
import json
import requests
import datetime
import os


# Initialize AWS S3 client___________________________________________________________________________________________________________________________


s3 = boto3.client('s3', 
                  region_name='us-east-1',
                  aws_access_key_id=os.getenv('aws_access_key_id'),
                  aws_secret_access_key=os.getenv('aws_secret_access_key'),
                  aws_session_token=os.getenv('aws_session_token'))
bucket_name = 'datalake-desafio-compassuol-rafael'


# Define global variables____________________________________________________________________________________________________________________________


ano_atual = datetime.datetime.now().strftime('%Y')
mes_atual = datetime.datetime.now().strftime('%m')
dia_atual = datetime.datetime.now().strftime('%d')

#* Load headers from json file
headers = json.load(open('credential.json', 'r'))


# Find the movies____________________________________________________________________________________________________________________________________


def fetch_data(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("results", [])
    else:
        print(f"Failed to fetch data from {url}, status code: {response.status_code}")
        return []


# Find the details of the movies/series______________________________________________________________________________________________________________


def fetch_details(item_id, item_type, headers):
    if item_type == 'movie':
        url = f"https://api.themoviedb.org/3/movie/{item_id}?append_to_response=credits&language=en-US"
    elif item_type == 'tv':
        url = f"https://api.themoviedb.org/3/tv/{item_id}?append_to_response=credits%2Cexternal_ids&language=en-US"
    else:
        print(f"Unknown item type: {item_type}")
        return None

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch details for {item_id}, status code: {response.status_code}")
        return None


# Save the data to S3_______________________________________________________________________________________________________________________________


def save_to_s3(data, path, bucket_name):
    s3.put_object(Body=json.dumps(data).encode('utf-8'), Bucket=bucket_name, Key=path)


# Changes the pages that are being saved_____________________________________________________________________________________________________________


def process_page(page_num, headers, ano_atual, mes_atual, dia_atual, bucket_name):
    all_the_movies = []
    all_the_series = []

    for i in range(1, 6):
        url_filme = f"https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=en-US&with_genres=80&sort_by=vote_count.desc&page={(page_num-1)*5 + i}&release_date.lte={ano_atual}-{mes_atual}-{dia_atual}"
        url_serie = f"https://api.themoviedb.org/3/discover/tv?include_adult=false&include_video=false&language=en-US&with_genres=80&sort_by=vote_count.desc&page={(page_num-1)*5 + i}&first_air_date.lte={ano_atual}-{mes_atual}-{dia_atual}"

        filmes = fetch_data(url_filme, headers)
        series = fetch_data(url_serie, headers)

        for filme in filmes:
            details = fetch_details(filme["id"], 'movie', headers)
            if details:
                all_the_movies.append(details)

        for serie in series:
            details = fetch_details(serie["id"], 'tv', headers)
            if details:
                all_the_series.append(details)

    path_series = f'Raw/TMDB/JSON/Series/{ano_atual}/{mes_atual}/{dia_atual}/top_series_crime_{page_num}.json'
    path_movies = f'Raw/TMDB/JSON/Movies/{ano_atual}/{mes_atual}/{dia_atual}/top_filmes_crime_{page_num}.json'

    save_to_s3(all_the_movies, path_movies, bucket_name)
    save_to_s3(all_the_series, path_series, bucket_name)


# Main function to use the rest of the functions_____________________________________________________________________________________________________


def request_upload_dados(qtd_paginas, headers, ano_atual, mes_atual, dia_atual, bucket_name):
    for j in range(1, qtd_paginas + 1):
        process_page(j, headers, ano_atual, mes_atual, dia_atual, bucket_name)


def lambda_handler(event, context):
    request_upload_dados(30, headers, ano_atual, mes_atual, dia_atual, bucket_name)
    return {
        'statusCode': 200,
        'body': json.dumps('Data processing completed successfully!')
    }
