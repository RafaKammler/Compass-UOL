# funcionamento no Lambda

Após a definição da API e de como seria realizado seu uso, era necessária a captação dos dados originários dessa API por meio de um script Python, tudo isso dentro do AWS Lambda. Dados esses que deveriam, posteriormente, ser armazenados em arquivos .json de no máximo 100 registros cada, dentro da zona RAW do mesmo bucket S3 utilizado na sprint anterior.

## Bibliotecas necessárias

O script criado para ser utilizado dentro do AWS Lambda tem como bibliotecas importadas as seguintes: `boto3`, `json`, `requests`, `datetime` e `os`. Para o funcionamento do script, apenas a biblioteca requests precisou ser adicionada via layers, pois as bibliotecas `boto3`, `datetime`, `os` e `json` já estão disponíveis por padrão no ambiente Lambda.

## Script python

O script Python está dividido em diversas partes, sendo as três principais: a conexão com a AWS e TMDB, a requisição dos dados de atores que fazem parte de cada filme, e a requisição do resto dos dados dos filmes e upload desses dados para o S3.

### Conexão com a AWS e TMDB

Para a conexão com o TMDB, utilizei um arquivo JSON com o código de autenticação, que é lido pelo código e tem seus valores atribuídos no código. Já para a conexão com a AWS, usei o método padrão de variáveis de ambiente, que já está disponível de maneira integrada no Lambda.

### Requisição das informações dos atores

O próximo trecho do código tem como propósito receber as informações de ID, nome, nome do personagem, sexo e data de nascimento de cada ator. Para isso, desenvolvi a função `info_atores_principais`. Esta função é estruturada de forma que inicialmente um sistema de cache seja iniciado para evitar requisições repetidas à API e melhorar a eficiência do código. Esse sistema será utilizado diversas vezes no restante do código.

Caso não existam informações no cache da aplicação, é iniciada a chamada para a API com o intuito de receber como resposta o tipo de dado para cada um dos IDs de filmes que serão passados para a função posteriormente. Além disso, é verificado se a resposta da API é bem-sucedida e são selecionados apenas os três primeiros membros de cada elenco, já que minha pesquisa se tratava principalmente dos personagens principais, sendo eles os três principais. Para isso, é utilizado o seguinte trecho de código:
```py
url_info_atores = f"https://api.themoviedb.org/3/{tipo_dado}{tmdb_id}/credits?language=en-US"
resposta_api_atores = requests.get(url_info_atores, headers=headers)
    if resposta_api_atores.status_code != 200:
        return []
    informacoes_cast = resposta_api_atores.json()["cast"][:3]
    info_atores = []
```

Na próxima etapa, é utilizado outro link de API para solicitação de informações que não estavam presentes na pesquisa anterior, como a data de nascimento e o gênero dos atores, já que essas informações não estavam disponíveis na pesquisa anterior. Esta segunda requisição é extremamente semelhante à primeira, também utilizando o sistema de cache e a verificação de resposta do servidor, e os detalhes adquiridos com essa solicitação são armazenados dentro de um dicionário Python.

Por fim, é feita a adição das informações obtidas na lista `info_atores` criada com a primeira requisição por meio do método `.append`, da seguinte forma:
```py
info_atores.append({
            "id": ator["id"],
            "nome": ator["name"],
            "personagem": ator["character"],
            "data_nascimento": detalhes_atores.get("birthday"),
            "sexo_ator": detalhes_atores.get("gender")
        })
```

### Restante das informações dos filmes e séries

Dando sequência ao código, a próxima etapa é mais simples e consiste na obtenção de todos os detalhes dos filmes e séries de gênero crime lançados até o dia atual. Além disso, é necessário transferir todos os dados para arquivos `.json` de até 100 registros cada. Para controlar esses registros e a quantidade de arquivos que serão criados, utilizei dois laços de repetição `for` que limitam a quantidade de páginas do TMDb para 5, já que o conteúdo da API é separado em páginas com 20 registros cada, além de definir a quantidade de arquivos que serão extraídos. E para evitar a repetição do conteúdo dos arquivos, é necessária a fórmula: `(j-1)*5 + i`, que ao ser inserida no link para representar a página a ser extraída resulta na não repetição do conteúdo.

O funcionamento das requisições aqui é extremamente semelhante ao dos atores, onde é feita a solicitação, checagem para ver se a resposta da API é positiva, e transformação desses resultados para `.json` para que possa ser extraído o ID de cada filme para ser utilizado pela função `info_atores_principais`. Por fim, adicionamos o conteúdo retornado ao conteúdo dos filmes e séries, conforme feito da seguinte maneira:

```py
esponse_filmes = requests.get(url_filme, headers=headers)
            response_series = requests.get(url_serie, headers=headers)

            # Verificações para verificar se a requisição foi bem sucedida
            if response_filmes.status_code == 200:
                filmes = response_filmes.json()["results"]
                # loop que utiliza a função anterior para encontrar informações dos atores principais de cada filme
                for filme in filmes:
                    informacoes_cast = info_atores_principais(filme["id"], "movie", headers, cache)
                    filme["cast"] = informacoes_cast
                conteudo_filmes.extend(filmes)
```

Esse processo é realizado para ambos, os filmes e as séries.

Após isso, o conteúdo dos arquivos `.json` está pronto para ser enviado para o bucket S3. Então, resta apenas definir o caminho do arquivo e o upload por meio do `put_object`, os quais são realizados por meio da sequência de comandos:

```py
        # define o caminho dentro do S3
        caminho_arquivo_series = f'Raw/TMDB/JSON/Series/{ano_atual}/{mes_atual}/{dia_atual}/top_series_crime_{j}.json'
        caminho_arquivo_filmes = f'Raw/TMDB/JSON/Movies/{ano_atual}/{mes_atual}/{dia_atual}/top_filmes_crime_{j}.json'

        # salva os arquivos no S3
        s3.put_object(Body=json.dumps(conteudo_series, indent=4, ensure_ascii=False).encode('utf-8'), Bucket=nome_bucket, Key=caminho_arquivo_series)
        s3.put_object(Body=json.dumps(conteudo_filmes, indent=4, ensure_ascii=False).encode('utf-8'), Bucket=nome_bucket, Key=caminho_arquivo_filmes)
```