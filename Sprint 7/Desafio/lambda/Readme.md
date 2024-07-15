# funcionamento no Lambda

Após a definição da API e de como seria realizado seu uso, era necessária a captação dos dados originários dessa API por meio de um Script Python, tudo isso dentro do AWS Lambda, dados esses que deveriam posteriormente ser armazenados em arquivos .json de no máximo 100 registros cada, dentro da RAW zone do mesmo S3 bucket utilizado na sprint anterior.

## Bibliotecas necessárias

O script que foi criado para poder ser utilizado dentro do AWS Lambda tem como bibliotecas importadas as seguintes: `boto3|json|requests|datetime|os` as quais para o funcionamento do script deveriam ser inclusas na AWS, mas como a `boto3` é padrão do ambiente lambda e as bibliotecas `datetime, os e json` são padrão do python, foi necessária somente a adição da biblioteca requests, adição essa feita por meio de layers, que consistem no upload de bibliotecas no formato .zip para serem utilizadas pelo script.

## Script python

O script python está separado em diversos trechos, sendo so 3 principais a conexão com a AWS e TMDB, a requisição dos dados de atores que fazem parte de cada filme e a requisção do resto dos dados dos filmes e upload desses dados para o S3

### Conexão com a AWS e TMDB

Para a conexão com o TMDB utilizei um arquivo json com o código de autentificação que é lido pelo código e tem seus devidos valores atribuidos no código, já para a conexão com a AWS usei o metodo padrão de variaveis de ambiente, que já está disponivel de maneira integrada no Lambda

### Requisição das informações dos atores

O próximo trecho do código tem como propósito receber as informações de ID, nome, nome do personagem, sexo e data de nascimento de cada ator, para isso desenvolvi a função "info_atores_principais". Essa função é estruturada da maneira com que inicialmente seja iniciado um sistema de cache para evitar requisições repetidas para a API e melhorar a eficiencia do código, sistema esse que será utilizado diversas vezes no restante do código.

Caso não existam informações no cache da aplicação, é iniciada a chamada para a API com o intuito de receber como resposta o tipo de dado para cada um  dos ids de filmes que serão passados para a função posteriormente, além de checar se a resposta da API é bem sucedida e selecionar somente os 3 primeiros membros de cada elenco, já que minha pesquisa se tratava principalmente dos personagens principais, sendo eles os 3 principais, para isso é utilizado o trecho de código:

```py
url_info_atores = f"https://api.themoviedb.org/3/{tipo_dado}{tmdb_id}/credits?language=en-US"
resposta_api_atores = requests.get(url_info_atores, headers=headers)
    if resposta_api_atores.status_code != 200:
        return []
    informacoes_cast = resposta_api_atores.json()["cast"][:3]
    info_atores = []
```

Na próxima etapa é utilizado outro link de API para solicitação de informações que não estavam presentes na pesquisa anterior, como por exemplo a data de nascimento dos atores