
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
  DATE(CONCAT(SUBSTR(dataEntrega, 1, 4), '-', SUBSTR(dataEntrega, 5, 2), '-', SUBSTR(dataEntrega, 7))) AS dataEntrega,  -- Add dataEntrega correction here
  CASE WHEN horaEntrega LIKE '_:__' THEN TIME(CONCAT('0', horaEntrega)) ELSE TIME(horaEntrega) END AS horaEntrega, -- Add horaEntrega correction here
  kmCarro, qtdDiaria, vlrDiaria
FROM tb_locacao;
-- passando os dados pra tabela locacao
INSERT INTO locacao (idLocacao, idCliente, idCarro, idVendedor)
SELECT DISTINCT idLocacao, idCliente, idCarro, idVendedor
FROM tb_locacao;
-- comandos para consultar as tabelas
-- SELECT * FROM locacao;

-- SELECT * FROM info_locacao;

-- SELECT * FROM vendedores;

-- SELECT * FROM clientes;

-- SELECT * FROM enderecos;

-- SELECT * FROM combustivel;

-- SELECT * FROM carro;
-- Criação as dimenções e a tabela fato
CREATE VIEW dim_vendedores AS
SELECT 
	ven.idVendedor as Codigo,
	ven.nomeVendedor as Nome,
	ven.estadoVendedor as Estado,
	ven.SexoVendedor as Sexo
FROM vendedores as ven;

CREATE VIEW dim_carros AS
SELECT 
    car.idCarro AS Codigo,
    car.modeloCarro AS Modelo,
    car.marcaCarro AS Marca,
    car.anoCarro AS Ano,
    car.kmCarro as "KM rodados",
    com.tipoCombustivel AS Combustivel,
    car.classiCarro as chassi
FROM carro AS car
LEFT JOIN combustivel AS com
ON com.idcombustivel = car.idcombustivel;

CREATE VIEW dim_clientes AS
SELECT 
	cli.idCliente as Codigo,
	cli.nomeCliente as Nome,
	cli.cidadeCliente as Cidade,
	en.estadoCliente as Estado,
	en.paisCliente as Pais
FROM clientes as cli
LEFT JOIN enderecos as en
on en.cidadeCliente = cli.cidadeCliente;

CREATE VIEW fato_locacao AS
SELECT
	loc.idLocacao as "Codigo da locacao",
	loc.idCliente as "Codigo do cliente",
	loc.idCarro as "Codigo do carro",
	loc.idVendedor as "Codigo do vendedor",
	inf.dataLocacao as "Dia da locacao",
	inf.horaLocacao as "Hora da locacao",
	inf.dataEntrega as "Data de entrega",
	inf.horaEntrega as "Horario de entrega",
	inf.qtdDiaria as "Quantidade de dias",
	inf.vlrDiaria as "Valor da diaria"
from locacao loc
left join info_locacao as inf
on inf.idlocacao = loc.idlocacao;

CREATE VIEW dim_tempo AS
SELECT DISTINCT
	dataLocacao as data,
	strftime('%Y', dataLocacao) as ano,
	strftime('%m', dataLocacao) as mes,
	strftime('%W', dataLocacao) as semana,
	strftime('%d', dataLocacao) as dia
from info_locacao;
-- comandos para consultar as views

-- SELECT * FROM dim_vendedores;

-- SELECT * FROM dim_carros;

-- SELECT * FROM dim_clientes; 

-- SELECT * FROM fato_locacao;

-- SELECT * FROM dim_tempo;