# API Request

O desafio consiste basicamente em realizar requisições para uma API. Apesar de ainda não serem tratadas, essas requisições devem ser organizadas de alguma maneira, e essa organização será explicada na seção a seguir.

## Filtros de API

Devido à minha escolha de utilizar a biblioteca `requests`, os filtros utilizados são adicionados diretamente ao link das requisições. Um detalhe é que a vasta maioria dos links está posicionada em f-strings, que serão explicadas posteriormente. Os links, no final, ficaram da seguinte maneira:

```py
url_info_atores = f"https://api.themoviedb.org/3/{tipo_dado}/{tmdb_id}/credits?language=en-US"
```

- Este é o link que procura pelas informações principais dos atores, sem muitos filtros além da definição da linguagem como `en-US`.

```py
url_detalhe = f"https://api.themoviedb.org/3/person/{ator['id']}?language=en-US"
```

- Este é o link que procura por detalhes adicionais dos atores encontrados pelo último link, tendo também como filtro somente a linguagem como `en-US`.

Ambos os links anteriores foram utilizados porque minhas perguntas estavam diretamente relacionadas ao elenco dos filmes e informações relacionadas aos atores.

```py
url_filme = f"https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=en-US&with_genres=80&sort_by=vote_count.desc&page={(j-1)*5 + i}&release_date.lte={ano_atual}-{mes_atual}-{dia_atual}"

url_serie = f"https://api.themoviedb.org/3/discover/tv?include_adult=false&include_video=false&language=en-US&with_genres=80&sort_by=vote_count.desc&page={(j-1)*5 + i}&first_air_date.lte={ano_atual}-{mes_atual}-{dia_atual}"
```

- Esses dois últimos links são os principais, pois puxam todas as informações dos filmes/séries da API. Aqui já são usados mais filtros, sendo eles:
    - Não incluir filmes adultos
    - Não incluir vídeos
    - Linguagem das informações sendo en-US
    - Somente filmes do gênero crime
    - Organizado por contagem de votos de maneira decrescente
    - Definição da página como uma fórmula matemática que funcionará dentro do código
    - E definindo a data máxima de lançamento como o dia atual da execução do script
