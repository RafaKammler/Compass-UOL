SELECT 
    sub.decada,
    sub.nome,
    sub.sexo,
    sub.total_por_decada
FROM (
    SELECT 
        nome,
        sexo,
        (ano / 10) * 10 AS decada,
        SUM(total) AS total_por_decada,
        ROW_NUMBER() OVER (PARTITION BY (ano / 10) * 10 ORDER BY SUM(total) DESC) AS posicao
    FROM 
        meubanco.tabela1
    WHERE 
        ano >= 1950
    GROUP BY 
        nome, sexo, (ano / 10) * 10
) AS sub
WHERE 
    sub.posicao <= 3
ORDER BY 
    sub.decada, sub.posicao;