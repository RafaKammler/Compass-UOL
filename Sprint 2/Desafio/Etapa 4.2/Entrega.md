# Etapa 4.2

## Dimensionamento do banco de dados

Nessa etapa do desafio, o objetivo era aplicar os conceitos de modelagem dimensional que nos foi passado, tendo como intuito tornar o modelo relacionado normalizado, que foi criado anteriormente, em um modelo dimensional.

### Criação das Views

Para o dimensionamento do banco nos foi recomendado o uso de views para as dimensões e fatos, que foram criadas seguindo os comandos:

```sql
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
```

Esse código cria as views de dimensões e fatos, sendo elas:

- Dim_Vendedores, que contém todos os dados relacionados aos vendedores
- Dim_carros, que tem todas as informações relacionadas aos carros
- Dim_Clientes, que possui tudo que está diretamente relacionado aos clientes
- Dim_tempo, que  contém o ano, o mês, a semana e o dia das datas de locação
- Fato_Locacao, que é a tabela fato do meu dimensionamento, que contém a maior parte dos dados da locação e os IDs dos clientes, carros e vendedores.

E por fim para interligação das tabelas eu utilizei o Canva, já que por serem views elas não podem ser ligadas por meio de primary e foreign keys no proprio DBeaver.
