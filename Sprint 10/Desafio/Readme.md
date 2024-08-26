# Desafio

O desafio da sprint 10, que também é o desafio final do PB, foi totalmente diferente do que fizemos até o momento. Até a sprint 09, estávamos envolvidos com a organização, estruturação e modelagem dos dados, mas esta etapa final teve como objetivo testar nossa habilidade na visualização de dados, utilizando a plataforma QuickSight para a criação de um dashboard que deveria responder perguntas definidas por nós mesmos, empregando conceitos visuais e textuais para um bom storytelling.

## Etapa 1 - Perguntas

As perguntas deviam ser escolhidas por nós e podiam ser alteradas ao longo das sprints. Devido a isso, resolvi modificar minhas perguntas diversas vezes durante o desafio final. No fim, minhas perguntas foram as seguintes:

- Como a popularidade de filmes/séries de crime variou ao longo dos anos?
- Quais são os filmes/séries de crime com maior popularidade?
- Quais são os atores com mais aparições em filmes de crime?
- Qual a proporção de aparição de atrizes em séries do gênero "Crime" antes e depois dos anos 2000? Qual a diferença entre essa proporção e a proporção em filmes do mesmo gênero?
- Quais são as combinações de gêneros ligadas ao crime mais populares?

## Etapa 2 - Dataset

A primeira coisa a ser feita após definir as perguntas foi exportar os dados do Athena para o QuickSight e organizar todas as colunas, garantindo que todos os dados necessários estivessem corretamente conectados.

Dentro do QuickSight, após a importação das tabelas, utilizei os IDs presentes tanto na tabela fato quanto nas dimensões para realizar um `left join`, consolidando todos os dados em uma grande tabela que contém as informações de todas as outras tabelas.

Além disso, utilizei a função de criação de colunas calculadas do próprio QuickSight para adicionar outra coluna que não estava presente: a coluna de popularidade. Embora essa coluna existisse nos dados provenientes do TMDB, ela não estava presente nos dados do IMDB. Para evitar a exclusão dos dados do IMDB, resolvi criar minha própria coluna de popularidade, que foi calculada da seguinte maneira:

```sql
{vote_average} * log({vote_count})
```

Nesse cálculo, a média dos votos é multiplicada pelo logaritmo na base 10 da quantidade de votos. O logaritmo foi utilizado para evitar que a quantidade de votos ofuscasse a média dos votos, garantindo um melhor equilíbrio entre os dois valores.

## Etapa 3 - Criação do Dashboard

O dashboard que criei foi dividido em vários grupos, cada um dedicado a uma questão específica, com exceção do terceiro grupo, que aborda duas perguntas.

### Grupo 1 - Popularidade ao Longo dos Anos

O primeiro agrupamento é bem simples: ele apresenta o dashboard e aborda a popularidade dos filmes e séries de crime ao longo dos anos. Para essa questão, utilizei um gráfico de linha, onde o eixo X representa a data de lançamento dos filmes, e o eixo Y exibe a média de popularidade dos filmes e séries lançados em cada ano.

### Grupo 2 - Filmes e Séries Mais Populares

A segunda seção do dashboard também é bastante simples, apresentando os filmes e séries com maior popularidade atualmente. Para visualizar essas informações, optei por utilizar gráficos de barras verticais — um para filmes e outro para séries. Nos gráficos, os nomes dos filmes e séries compõem o eixo X, enquanto a popularidade máxima é representada no eixo Y.

### Grupo 3 - Estatísticas dos Atores

O terceiro espaço do meu dashboard foi reservado para exibir todas as informações relacionadas aos atores. Inicialmente, são apresentados os 10 atores/atrizes que mais atuaram em filmes ou séries. Para essa visualização, utilizei um gráfico de barras horizontais, onde o eixo Y mostra os nomes dos atores, e o eixo X representa a quantidade de aparições de cada ator.

Além disso, neste mesmo espaço, também apresentei a proporção de atores em relação a atrizes em filmes e séries. Para isso, utilizei quatro gráficos de rosca, que mostram a proporção em filmes antes dos anos 2000, em séries antes dos anos 2000, em filmes depois dos anos 2000, e em séries depois dos anos 2000.

### Grupo 4 - Combinações de Gêneros Mais Populares

Na última seção do dashboard, decidi apresentar os dados relacionados às combinações de gêneros mais populares. Para isso, utilizei dois gráficos de dispersão. No eixo X, coloquei a média da popularidade, e no eixo Y, a quantidade de aparições. Além disso, a terceira informação representada é a identificação desses gêneros, com cada um sendo representado por um círculo, cuja posição define a popularidade e a quantidade de aparições.

## Informações Complementares

Além dos gráficos explicados anteriormente, para melhorar o entendimento do dashboard pelos leitores, também decidi adicionar textos e imagens para contextualizar e complementar cada seção do dashboard. Isso ajudou a fornecer uma narrativa mais coesa e a facilitar a interpretação dos dados apresentados.