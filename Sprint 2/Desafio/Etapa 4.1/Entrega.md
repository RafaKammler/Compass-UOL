# Etapa 4.1

## Normalização do banco de dados

A primeira etapa do desafio proposto foi a aplicação das três primeiras formas normais de bancos de dados à tabela que nos foi passada, juntamente com a elaboração de um desenho que explicasse o relacionamento, para isso foram usados os seguintes métodos:

### Primeira norma

Na verdade, a tabela que nos foi fornecida já estava na primeira forma normal, uma vez que não apresentava repetição de linhas idênticas, pois sempre haviam diferenças, por mais pequenas que fossem, entre as linhas.

### Segunda norma

Para fazer com que a tabela entre no modelo da segunda forma normal foi necessaria a criação de diversas outras tabelas que fazem com que os produtos não chave dependam totalmente da chave primaria, e para isso eu utilizei o comando:

```sql
CREATE TABLE nome AS(
    coluna1 TIPO_DE_DADO
    coluna2 TIPO_DE_DADO
    ...
);
```

Após diversos usos 