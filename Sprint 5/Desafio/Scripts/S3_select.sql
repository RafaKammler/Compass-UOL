SELECT 
        COUNT(*) AS "Nº total de salas já abertas", 
        SUM(
            CASE 
                WHEN (CAST(s."ASSENTOS_CADEIRANTES" AS FLOAT) >= 1 AND CAST(s."ASSENTOS_SALA" AS FLOAT) > 200) THEN 1 
                ELSE 0 
            END
        ) AS "Nº de salas já abertas com mais de 200 assentos, com assentos para cadeirantes", 
        SUM(
            CASE 
                WHEN(DATE_DIFF(year, CAST(s."DATA_INICIO_FUNCIONAMENTO_SALA" AS TIMESTAMP), UTCNOW()) > 31) THEN 1 
                ELSE 0 
            END
        ) AS "Nº de salas com mais de 30 anos de funcionamento" ,
        SUM(
            CASE
            WHEN CHAR_LENGTH(s."NOME_SALA") > 25 THEN 1
            ELSE 0
            END
        ) AS "Nº de salas com nome maior que 25 caracteres"
    FROM S3Object s
    WHERE DATA_INICIO_FUNCIONAMENTO_SALA <> '' AND 
    NOT ASSENTOS_SALA = '0'