# Parte 4

## Script utilizado para a limpeza do dataset

### Conexão com a AWS

Para a limpeza do dataset eu utilizei uma sequência de comandos Python com o auxílio da biblioteca `pandas`, mas o primeiro passo deveria ser a conexão com o bucket S3 onde eu havia previamente inserido o dataset. Para essa conexão, inicialmente defini as variáveis `S3_bucket`, `arquivo`, `arquivo_local` e `arquivo_corrigido` para conseguir fazer o download, análise e upload para o bucket. Após isso, realizei a conexão por meio dos comandos:

```python
s3 = boto3.client('s3')
s3.download_file(s3_bucket, arquivo, arquivo_local)
```
que conecta-se ao servidor AWS, baixa o arquivo e o salva em um arquivo local.

### Limpeza dos dados

Para o tratamento dos dados do dataset, utilizei diversas funções da biblioteca `pandas`. Inicialmente, li o arquivo e depois utilizei os comandos:

```python
conteudo_arquivo = conteudo_arquivo.dropna(how='all', subset=['REGISTRO_SALA', 'NOME_SALA']).drop_duplicates()
conteudo_arquivo = conteudo_arquivo.fillna(0)
```

Esses comandos tinham como função a limpeza das linhas nulas, exclusão das duplicadas, exclusão das linhas que continham as colunas "REGISTRO_SALA" e "NOME_SALA" nulas por tratarem de salas inexistentes e a substituição de todos os valores numéricos nulos para 0.

Além disso, tive que utilizar uma sequência de comandos para padronização de todas as datas do database. Para isso, foi inicialmente necessário definir quais são as tabelas que contêm datas e depois formatar todas para YYYY/MM/DD. Tudo isso foi feito por meio dos comandos:

```python
colunas_datas = ['DATA_SITUACAO_SALA', 'DATA_INICIO_FUNCIONAMENTO_SALA', 'DATA_SITUACAO_COMPLEXO']
for coluna in colunas_datas:
     conteudo_arquivo[coluna] = pd.to_datetime(conteudo_arquivo[coluna], format='%d/%m/%Y', errors='coerce')
```

E por fim, a última parte do script tinha como função a devolução do arquivo CSV com as correções e tratamentos para dentro do bucket S3 na AWS, por meio dos comandos:

python

```python
conteudo_arquivo.to_csv(arquivo_corrigido, sep=';', index=False)
s3.upload_file(arquivo_corrigido, s3_bucket, 'databases-limpas/salas-de-exibicao-e-complexos-corrigido.csv')
```
Além da remoção de todos os arquivos locais que sobraram na máquina, tudo isso por meio da biblioteca `os`.

## Script utilizado para a execução do S3_Select por meio de python

### Conexão com a AWS

A conexão com a AWS nessa etapa é semalhante a anterior, mas desta vez não é necessaria a definição de um arquivo temporário local e aqui devo definir o arquivo tratado para ser utilizado, além de que a conexão com o bucket ocorre da mesma maneira por meio do `boto3`

### Utilização do S3_Select

Para conseguir utilizar um script sql dentro do S3_select por meio do python do python tive que utilizar da formatação solicitada na documentação da AWS, a qual seria:

```Python
solicitação = s3.select_object_content(
    Bucket=s3_bucket,
    Key=arquivo,
    ExpressionType='SQL',
    Expression=comandos_sql,
    InputSerialization={
        'CSV': {
            'FileHeaderInfo': 'USE',  
            'RecordDelimiter': '\n', 
            'FieldDelimiter': ';'  
        }
    },
    OutputSerialization={
        'JSON': {
            'RecordDelimiter': '\n'
        }
    }
)
```

Onde cada linha tem como função, respectivamente:
- Definição de qual será o bucket usado
- Definir qual sera o arquivo utilizado
- Definir qual o tipo de comandos que serão recebidos
- Definir quais serão os comandos recebidos, que nessa situação são comandos_sql importados de um arquivo externo
- Definir como está organizado o dataset de entrada, que também contem uma lista de comandos, sendo eles respectivamente:
     - Instrução para que a primeira linha seja utilizada como cabeçalho
     - Definir que cada linha nova é um registro novo
     - Determinar qual o caractere que separa os campos, sendo nesse contexto o ";"
- E por fim definir como será o dataset de saida, para o qual utilizei o tipo de arquivo JSON, somente com fins de organização

Finalmente para que os dados pudessem ser lidos foi necessaria a criação de uma outra função para decodificação , que seria:

```python
for evento in solicitação['Payload']:
     if 'Records' in evento:
          registros = evento['Records']['Payload'].decode('utf-8')
          print(registros)
```

Que realiza uma iteração na solicitação do S3 para checar se existe o valor "Records", caso ele exista, realiza uma decodificação dos valores para utf-8, já que caso contrario sairiam em binário, o que dificultaria a leitura.
