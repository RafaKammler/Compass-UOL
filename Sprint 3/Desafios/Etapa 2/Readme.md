# Etapa 2

## Desenvolvimento

Já na etapa dois é realizado todo o processo de análise dos dados e criação de gráficos. Essa etapa é separada em 8 partes, cada uma com uma tarefa diferente, sendo elas:

### Parte 1

Na parte 1 eu utilizei diversos scripts para o uso de bibliotecas, leitura do arquivo e tratamento dos dados, que foram os seguintes:

```python
# importando as bibliotecas
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker

# abertura do arquivo
df = pd.read_csv('googleplaystore.csv')

#definição da palheta de cores
sns.set_palette("pastel")
cores = sns.color_palette()
```

Utilizados para a importação das bibliotecas e leitura do arquivo CSV, além da definição da paleta de cores que será utilizada em todos os gráficos.

```python
# formatacao dos numeros dos graficos
def formatacao_sem_notacao(value, pos=None):
    units = [(1000, 'K+'), (1000000, 'M+'), (1000000000, 'B+')]
    if value == 0:
        return '0'
    for threshold, unit in units:
        if value < threshold:
            return f"{value:,.0f}{unit}"
        value /= threshold
    return f"{value:,.0f}B+"
```

Que foi utilizado para formatar os números que aparecem nos gráficos

```python
df = df.drop_duplicates()

df['Rating'] = pd.to_numeric(df['Rating'], errors='# Display the resultcoerce')
df = df[df['Rating'] <= 5]

df['Installs'] = df['Installs'].str.replace(',', '')
df['Installs'] = df['Installs'].str.replace('+', '')
df['Installs'] = pd.to_numeric(df['Installs'], errors='coerce')

df['Price'] = df['Price'].str.replace('$', '')
df['Price'] = pd.to_numeric(df['Price'], errors='coerce')

df['Reviews'] = pd.to_numeric(df['Reviews'], errors='coerce')

df['Size'] = df['Size'].str.replace('M', '')
df['Size'] = pd.to_numeric(df['Size'], errors='coerce')
```

Foram utilizadas para a realização das seguintes tarefas, ordenadas:

1. Remoção de linhas duplicadas
2. Transformação da coluna "Rating" para o tipo numérico
3. Transformação da coluna "Installs" para o tipo numérico
4. Transformação da coluna "Price" para o tipo numérico
5. Transformação da coluna "Reviews" para o tipo numérico
6. Transformação da coluna "Size" para o tiṕo numérico

### Parte 2

Criação de um gráfico de barras com os top 5 apps por quantidade de instalações, por meio do script:

```python

# Definição dos apps mais baixados
MAIS_VENDIDOS = df.nlargest(5, 'Installs')
MAIS_VENDIDOS['Quantidade de downloads'] = MAIS_VENDIDOS['Installs']

# Criação do gráfico
ax = MAIS_VENDIDOS.plot.bar(x='App', y='Quantidade de downloads', color=cores, width=0.7)
ax.set_title('Top 5 apps mais baixados')
plt.legend(prop={'size': 8})
plt.xticks(fontsize=9)
plt.gca().yaxis.set_major_formatter(ticker.FuncFormatter(formatacao_sem_notacao))
plt.show()
```

Que inicialmente encontra quais são os aplicativos mais baixados e depois cria o gráfico.

### Parte 3

Na parte 3 foi solicitada a criação de um gráfico de pizza mostrando as categorias e sua respectiva frequência no dataset.

```python
# Definição da frequencia de cada categoria
frequencia = df['Category'].value_counts()

# Criação do gráfico
patches, texts, autotexts = plt.pie(frequencia, autopct='%1.1f%%', textprops={'fontsize': 5}, colors = cores, pctdistance=1.1)
plt.legend(patches, frequencia.index, loc="center left", bbox_to_anchor=(1, 0, 0.5, 1), prop={'size': 5})
plt.ylabel('') 
plt.xlabel('')
plt.title('Frequência de aplicativos por categoria')
plt.show()
```

Onde primeiramente é calculada a frequência de cada categoria e após isso é criado o gráfico com essa informação.

### Parte 4

O objetivo da quarta etapa era encontrar o app mais caro do dataset, para isso utilizei o script:

```python
# Encontrando o mais caro
MAIS_CARO = df.nlargest(1, 'Price')

# Organizando o Output
MAIS_CARO['Nome do aplicativo'] = MAIS_CARO['App']
MAIS_CARO['Preço'] = '$' + MAIS_CARO['Price'].astype(str)
display(MAIS_CARO[['Nome do aplicativo', 'Preço']])
```

Que encontra o app mais caro e depois imprime seu nome e seu preço.

### Parte 5

Na parte 5 é solicitada a quantidade de apps com a classificação "Mature 17+" no database, que é encontrado da seguinde maneira:

```python
# encontrando a quantia de apps +17 
QUANTIAMAIOR17 = df['Content Rating'].value_counts().get('Mature 17+', 0)

print("Existem", QUANTIAMAIOR17, "aplicativos com conteúdo +17 na PlayStore")
```

Que primeiramente encontra a quantidade e depois a imprime.

### Parte 6

A etapa 6 solicitou que fossem encontrados os 10 apps com maior quantidade de reviews, que foram encontrados da seguinte maneira:

```python
# Encontrando os 10 apps com mais reviews
TOP10MAISREVIEWS = df.drop_duplicates('App').nlargest(10, 'Reviews')

# Organizando o Output
TOP10MAISREVIEWS['Nome do aplicativo'] = TOP10MAISREVIEWS['App']
display(TOP10MAISREVIEWS[['Nome do aplicativo', 'Reviews']])

```

Que inicialmente encontra os aplicativos e depois imprime a tabela.

### Parte 7

Na parte 7 nos foi solicitado a criação de 2 cálculos de nossa escolha, sendo um deles em formato de lista e o outro em formato de matriz. Para essa etapa, eu resolvi calcular qual o maior app disponível no dataset por meio do script:

```python
# Encontrando o mais caro
MAIS_CARO = df.nlargest(1, 'Price')

# Organizando o Output
MAIS_CARO['Nome do aplicativo'] = MAIS_CARO['App']
MAIS_CARO['Preço'] = '$' + MAIS_CARO['Price'].astype(str)
display(MAIS_CARO[['Nome do aplicativo', 'Preço']])
```

Que encontra qual o maior e depois imprime seu nome e tamanho.

E também criei a lista dos 10 maiores ratings entre os apps classificados em "Mature 17+", por meio da sequência de códigos:

```python
# Encontrando os apps +17 com melhores ratings
df_filtrado_mais17 = df[df['Content Rating'] == 'Mature 17+']
MAIORES10RATINGS = df_filtrado_mais17.nlargest(10, 'Rating')

# Organizando o Output
MAIORES10RATINGS['Avaliações'] = MAIORES10RATINGS['Rating']
MAIORES10RATINGS['Nome dos aplicativos'] = MAIORES10RATINGS["App"]
MAIORES10RATINGS = MAIORES10RATINGS.sort_values('Nome dos aplicativos')
display(MAIORES10RATINGS[['Nome dos aplicativos', 'Avaliações']])
```

Que separa os dados desejados e depois os imprime.

### Parte 8

Por fim, na parte 8 foi solicitada a criação de dois gráficos, também de nossa escolha.

Para o primeiro gráfico, eu resolvi utilizar o modelo de gráfico de linhas, tendo como conteúdo a quantidade de reviews para cada nível de avaliação.

```python
# Criação do gráfico
df_grouped = df.groupby('Rating')['Reviews'].mean().reset_index()
df_grouped.plot(x='Rating', y='Reviews', kind='line', color = '#662E9B' )
plt.title('Reviews por cada avaliação')
plt.xlabel('Avaliações')
plt.ylabel('Quantidade de reviews')
plt.gca().yaxis.set_major_formatter(ticker.FuncFormatter(formatacao_sem_notacao))
plt.grid(True)
plt.show()
```

Como segundo gráfico, eu resolvi criar um no modelo de gráfico de caixas, que tinha como conteúdo a média de avaliação por categoria, entre as 10 categorias mais frequentes, isso por meio do código:

```python
# Encontrando categorias mais frequentes
frequencia = df['Category'].value_counts().nlargest(10)

# Criação do gráfico
plt.figure(figsize=(12, 8))
sns.boxplot(x='Category', y='Rating', data=df[df['Category'].isin(frequencia.index)], palette= cores)
plt.xticks(rotation=90, fontsize = 8)  
plt.title('Avaliações por categoria, nas 10 categorias mais frequentes')
plt.xlabel('Categoria', fontsize = 15)
plt.ylabel('Avaliações', fontsize = 15)
plt.show()
```
