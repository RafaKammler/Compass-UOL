# Etapa 4.1

## Normalização do banco de dados

A primeira etapa do desafio proposto foi a aplicação das três primeiras formas normais de bancos de dados à tabela que nos foi passada, juntamente com a elaboração de um desenho que explicasse o relacionamento, para realização do desafio utilizei o software DBeaver, e segui os seguintes passos:

### Primeira forma normal

Na verdade, a tabela que nos foi fornecida já estava na primeira forma normal, uma vez que não apresentava repetição de linhas idênticas, pois sempre haviam diferenças, por mais pequenas que fossem, entre as linhas.

### Segunda forma normal

Para garantir que a tabela esteja em conformidade com a Segunda Forma Normal (2NF), foi necessário criar várias outras tabelas, de modo que os atributos não chave dependam totalmente da chave primária. Isso foi alcançado ao utilizar os comandos:

```sql
-- Criação da tabela enderecos
CREATE TABLE enderecos(
cidadeCliente VARCHAR PRIMARY KEY,
estadoCliente VARCHAR,
paisCliente VARCHAR
);
-- Criação da tabela clientes
CREATE TABLE clientes(
idCliente INTEGER PRIMARY KEY,
nomeCliente VARCHAR,
cidadeCliente VARCHAR,
FOREIGN KEY (cidadeCliente) REFERENCES enderecos(cidadeCliente)
);
-- Criação da tabela combustivel
CREATE TABLE combustivel(
idcombustivel INTEGER PRIMARY KEY,
tipoCombustivel VARCHAR
);
-- Criação da tabela carro
CREATE TABLE carro(
idCarro INTEGER PRIMARY KEY,
modeloCarro VARCHAR,
classiCarro VARCHAR,
marcaCarro VARCHAR,
kmCarro INTEGER,
anoCarro INTEGER,
idcombustivel INTEGER,
FOREIGN KEY (idcombustivel) REFERENCES combustivel(idcombustivel)
);
-- Criação da tabela vendedores
CREATE TABLE vendedores(
idVendedor INTEGER PRIMARY KEY,
nomeVendedor VARCHAR,
sexoVendedor SMALLINT,
estadoVendedor VARCHAR
);
-- Criação da tabela info-locacao
CREATE TABLE info_locacao(
idLocacao INTEGER PRIMARY KEY,
dataLocacao DATE,
horaLocacao TIME,
dataEntrega DATE,
horaEntrega TIME,
kmCarro INTEGER,
qtdDiaria INTEGER,
vlrDiaria DOUBLE
);
-- Criação da tabela locacao
CREATE TABLE locacao(
 idLocacao INTEGER,
    idCliente INTEGER,
    idCarro INTEGER,
    idVendedor INTEGER,
    FOREIGN KEY (idLocacao) REFERENCES info_locacao(idLocacao),
    FOREIGN KEY (idCliente) REFERENCES clientes(idCliente),
    FOREIGN KEY (idCarro) REFERENCES carro(idCarro),
    FOREIGN KEY (idVendedor) REFERENCES vendedores(idVendedor)
);
```

Nesse código, cada comando "CREATE TABLE" cria uma tabela, onde cada item dentro dos parênteses representa uma coluna da tabela, e é seguida pelo tipo de dado correspondente, os quais seriam:

- Integer = Número inteiro
- Date = Data
- Time = Horario
- Varchar = Texto

Além de fazer uso de Primary Keys e Foreign Keys para interligar as tabelas entre si e utilizei uma função do próprio DBeaver para realizar a cardinalidade automaticamente.

### Terceira forma normal

Ao realizar o processo anterior para transformar a tabela para a segunda forma normal, acabei, acidentalmente, também convertendo-a também para a Terceira Forma. Assim, não foi necessário realizar nenhuma mudança nesta terceira etapa.

### Transferência dos dados

Para que as tabelas não ficassem vazias, foi necessaria a realização de diversos comandos, sendo eles:

```sql
-- passando os dados pra tabela enderecos
INSERT INTO enderecos (cidadeCliente, estadoCliente, paisCliente)
SELECT DISTINCT cidadeCliente, estadoCliente, paisCliente
FROM tb_locacao;

-- passando os dados pra tabela clientes
INSERT INTO clientes (idCliente, nomeCliente, cidadeCliente)
SELECT DISTINCT idCliente, nomeCliente, cidadeCliente
FROM tb_locacao;

-- passando os dados pra tabela combustvel
INSERT INTO combustivel (idcombustivel, tipoCombustivel)
SELECT DISTINCT idcombustivel, tipoCombustivel
FROM tb_locacao;

-- passando os dados pra tabela carro
INSERT INTO carro (idCarro, modeloCarro, classiCarro, marcaCarro, kmCarro, anoCarro, idcombustivel)
SELECT 
    idCarro,
    modeloCarro,
    classiCarro,
    marcaCarro,
 	kmCarro,
    anoCarro,
    idcombustivel
FROM (
    SELECT 
        idCarro,
        modeloCarro,
        classiCarro,
        marcaCarro,
        kmCarro,
        anoCarro,
        idcombustivel,
        ROW_NUMBER() OVER (PARTITION BY idCarro ORDER BY kmCarro DESC) AS row_num
    FROM tb_locacao
) AS subquery
WHERE row_num = 1;

-- passando os dados pra tabela vendedores

INSERT INTO vendedores (idVendedor, nomeVendedor, sexoVendedor, estadoVendedor)
SELECT DISTINCT idVendedor, nomeVendedor, sexoVendedor, estadoVendedor
FROM tb_locacao;
-- passando os dados pra tabela info_locacao

INSERT INTO info_locacao (idLocacao, dataLocacao, horaLocacao, dataEntrega, horaEntrega, kmCarro, qtdDiaria, vlrDiaria)
SELECT DISTINCT idLocacao, 
  DATE(CONCAT(SUBSTR(dataLocacao, 1, 4), '-', SUBSTR(dataLocacao, 5, 2), '-', SUBSTR(dataLocacao, 7))) AS dataLocacao,
  CASE WHEN horaLocacao LIKE '_:__' THEN TIME(CONCAT('0', horaLocacao)) ELSE TIME(horaLocacao) END AS hora,
  DATE(CONCAT(SUBSTR(dataEntrega, 1, 4), '-', SUBSTR(dataEntrega, 5, 2), '-', SUBSTR(dataEntrega, 7))) AS dataEntrega,  
  CASE WHEN horaEntrega LIKE '_:__' THEN TIME(CONCAT('0', horaEntrega)) ELSE TIME(horaEntrega) END AS horaEntrega, 
  kmCarro, qtdDiaria, vlrDiaria
FROM tb_locacao;

-- passando os dados pra tabela locacao
INSERT INTO locacao (idLocacao, idCliente, idCarro, idVendedor)
SELECT DISTINCT idLocacao, idCliente, idCarro, idVendedor
FROM tb_locacao;
```

Com o uso do comando "Insert into", foram realizadas as inserções de dados necessarias para que as tabelas fossem preenchidas, já o "Select distinct" foi utilizado para selecionar os dados que seriam transferidos para as tableas, mas além disso foram usados alguns comandos a mais, para realizar a correção de alguns dados que ficariam fora do padrão, sendo eles:

```sql
DATE(CONCAT(SUBSTR(dataLocacao, 1, 4), '-', SUBSTR(dataLocacao, 5, 2), '-', SUBSTR(dataLocacao, 7))) AS dataLocacao,
  CASE WHEN horaLocacao LIKE '_:__' THEN TIME(CONCAT('0', horaLocacao)) ELSE TIME(horaLocacao) END AS hora,
  DATE(CONCAT(SUBSTR(dataEntrega, 1, 4), '-', SUBSTR(dataEntrega, 5, 2), '-', SUBSTR(dataEntrega, 7))) AS dataEntrega,  
  CASE WHEN horaEntrega LIKE '_:__' THEN TIME(CONCAT('0', horaEntrega)) ELSE TIME(horaEntrega) END AS horaEntrega,
```

Que foi usado para realizar a correção do formato das datas e dos horários, e também:

```sql
FROM (
    SELECT 
        idCarro,
        modeloCarro,
        classiCarro,
        marcaCarro,
        kmCarro,
        anoCarro,
        idcombustivel,
        ROW_NUMBER() OVER (PARTITION BY idCarro ORDER BY kmCarro DESC) AS row_num
    FROM tb_locacao
) AS subquery
WHERE row_num = 1;
```

Que teve como função selecionar a maior kilometragem, ou seja, a mais recente de cada carro para possibilitar o uso da coluna kmCarro na tabela Carros
