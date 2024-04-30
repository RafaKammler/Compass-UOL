-- listar o código e o nome do vendedor com maior número de vendas (contagem)
SELECT tbvendedor.cdvdd, tbvendedor.nmvdd
FROM tbvendedor
JOIN tbvendas ON tbvendas.cdvdd = tbvendedor.cdvdd
WHERE tbvendas.status = 'Concluído'
GROUP BY tbvendedor.cdvdd, tbvendedor.nmvdd
ORDER BY COUNT(tbvendas.cdvdd) DESC
LIMIT 1;

-- listar o código e nome do produto mais vendido entre as datas de 2014-02-03 até 2018-02-02
select tbvendas.cdpro, tbvendas.nmpro
from tbvendas
where status = 'Concluído' and tbvendas.dtven >= '2014-02-03' and tbvendas.dtven <= '2018-02-02'
group by cdpro
order by count(*) desc
limit 1

-- calcule a comissão de todos os vendedores
--considerando todas as vendas armazenadas na base de dados com status concluído.
SELECT   tbvendedor.nmvdd AS vendedor, 
    ROUND(SUM(tbvendas.qtd * tbvendas.vrunt), 2) AS valor_total_vendas, 
    ROUND(SUM(tbvendas.qtd * tbvendas.vrunt * tbvendedor.perccomissao / 100.00), 2) AS comissao
FROM tbvendas
JOIN tbvendedor ON tbvendas.cdvdd = tbvendedor.cdvdd
WHERE tbvendas.status = 'Concluído'
GROUP BY tbvendedor.nmvdd
ORDER BY comissao DESC;

-- listar o código e nome cliente com maior gasto na loja
select tbvendas.cdcli, tbvendas.nmcli,
round(sum(tbvendas.qtd * tbvendas.vrunt), 2) as gasto
from tbvendas
where status = 'Concluído'
group by tbvendas.cdcli, tbvendas.nmcli
order by gasto desc
limit 1;

-- listar código, nome e data de nascimento dos dependentes do vendedor com menor valor total bruto em vendas (não sendo zero)
select td.cddep, td.nmdep, td.dtnasc,
    SUM(tvv.qtd * tvv.vrunt) as valor_total_vendas
from TBVENDAS tvv
inner join TBVENDEDOR tv on tvv.cdvdd = tv.cdvdd
inner join TBDEPENDENTE td on tv.cdvdd = td.cdvdd
where tvv.status = 'Concluído'
group by td.cddep, td.nmdep, td.dtnasc
having tvv.cdvdd = (
        select tv2.cdvdd
        from TBVENDEDOR tv2
        inner join TBVENDAS tvv2 on tv2.cdvdd = tvv2.cdvdd
        where tvv2.status = 'Concluído'
        group by tv2.cdvdd
        order by SUM(tvv2.qtd * tvv2.vrunt) asc
        limit 1
    )
order by valor_total_vendas;

-- listar os 10 produtos menos vendidos pelos canais de E-Commerce ou Matriz
select T.cdpro,
    T.nmcanalvendas,
    T.nmpro,
    sum(T.qtd) as quantidade_vendas
from tbvendas T
where status = 'Concluído' 
group by T.nmcanalvendas, 
    T.nmcanalvendas,
    T.nmpro
order by quantidade_vendas asc
limit 10

-- listar o gasto médio por estado da federação
select estado,
   round(avg(qtd * vrunt), 2) as gastomedio
from tbvendas
where status = 'Concluído'
group by
    estado
order by 
    gastomedio desc

-- listar os códigos das vendas identificadas como deletadas
select cdven
from tbvendas
where deletado > 0
order by cdven

-- listar a quantidade média vendida de cada produto agrupado por estado da federação
select T.estado,
    T.nmpro,
    round(avg(T.qtd), 4) as quantidade_media
from tbvendas T
where status = 'Concluído'
group by T.estado, T.nmpro
order by T.estado, nmpro