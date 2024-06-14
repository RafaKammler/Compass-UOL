# Parte 5

## Scripts SQL para S3_Select

Na parte 5, nos foi solicitado o uso do S3_Select para a análise dos dados do dataset, e para essa análise foi requisitado o uso de diversas funções e sintaxes do SQL disponíveis para o S3, tudo isso tendo que ser utilizado dentro da menor quantidade de consultas possíveis, sendo a quantidade ideal apenas 1 consulta.

- O primeiro requisito do desafio era a criação de uma cláusula que filtrasse dados por meio de pelo menos 2 operadores lógicos. Para isso, eu utilizei os seguintes comandos:

```sql
WHERE DATA_INICIO_FUNCIONAMENTO_SALA <> '' AND 
NOT ASSENTOS_SALA = '0'
```

Isso retira todas as salas que não têm data de abertura, permitindo que minha pesquisa seja feita somente em salas já abertas. Além disso, remove também todas as salas que contêm 0 assentos por se tratarem de salas inexistentes. Para isso, utilizei os operadores lógicos `AND` e `NOT`.

O próximo requisito era o uso de pelo menos duas funções de agregação, que foram utilizadas nos seguintes scripts:


```sql
COUNT(*) AS "Nº total de salas já abertas",
```
Esse comando conta a quantidade de linhas do database, e como cada linha equivale a 1 sala, ele conta a quantidade de salas, utilizando a função de agregação `COUNT`.

```sql
SUM(
        CASE 
            WHEN (CAST(s."ASSENTOS_CADEIRANTES" AS FLOAT) >= 1 AND CAST(s."ASSENTOS_SALA" AS FLOAT) > 200) THEN 1 
            ELSE 0 
        END
    ) AS "Nº de salas já abertas com mais de 200 assentos, com assentos para cadeirantes",
```
Nessa terceira parte do script, uso novamente uma função de agregação, sendo ela o `SUM`, cumprindo o segundo requisito da quinta etapa. Além disso, utilizo também o `CASE WHEN`, que é uma função condicional, o que resulta na resolução da terceira parte também. Além disso, esse trecho também utiliza o `CAST`, que é uma função de conversão como é pedido na quarta etapa.

Esse script tem a função de contar quantas salas que têm mais de 200 assentos possuem pelo menos 1 assento para cadeirantes.

Para a quinta etapa, era necessário utilizar uma função de data, que eu utilizei no seguinte script:

sql


```sql
SUM(
            CASE 
                WHEN(DATE_DIFF(year, CAST(s."DATA_INICIO_FUNCIONAMENTO_SALA" AS TIMESTAMP), UTCNOW()) > 31) THEN 1 
                ELSE 0 
            END
        ) AS "Nº de salas com mais de 30 anos de funcionamento" ,
```

Nesse script, que tinha como função contar a quantidade de salas que foram abertas há mais de 30 anos, utilizei duas funções de data, sendo elas `DATE_DIFF` e `UTCNOW`, além de todas as outras funções utilizadas, que englobam funções condicionais e funções de agregação.

Por fim, foi solicitado o uso de uma função de string, que foi utilizada no seguinte comando:
```sql
SUM(
        CASE
        WHEN CHAR_LENGTH(s."NOME_SALA") > 25 THEN 1
        ELSE 0
        END
    ) AS "Nº de salas com nome maior que 25 caracteres"
```

Aqui, a função de string `CHAR_LENGTH` tem a função de contar os caracteres de todos os nomes, e as funções de agregação e condicional contam quantos nomes têm mais de 25 caracteres.