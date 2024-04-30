-- listar todos os livros publicados após 2014
SELECT cod, titulo, autor, editora, valor, publicacao, edicao, idioma
FROM livro 
WHERE publicacao >= '2015-01-01'
ORDER BY cod ASC;
-- listar os 10 livros mais caros
SELECT titulo, valor
FROM livro
ORDER BY valor DESC
LIMIT 10;
-- listar as 5 editoras com mais livros na biblioteca
SELECT COUNT(LIVRO.cod) AS quantidade, EDITORA.nome, ENDERECO.estado, ENDERECO.cidade
FROM LIVRO
JOIN EDITORA ON LIVRO.editora = EDITORA.codeditora
JOIN ENDERECO ON EDITORA.endereco = ENDERECO.codendereco
GROUP BY EDITORA.nome, ENDERECO.estado, ENDERECO.cidade
ORDER BY quantidade DESC
LIMIT 5;
-- listar a quantidade de livros publicada por cada autor.
SELECT A.nome AS nome, A.codautor, A.nascimento, COUNT(L.cod) AS quantidade
FROM 
    AUTOR A
LEFT JOIN 
    LIVRO as L
    ON A.codautor = L.autor
GROUP BY A.nome, A.codautor, A.nascimento
ORDER BY A.nome;
-- listar o nome dos autores que publicaram livros através de editoras NÃO situadas na região sul do Brasil
SELECT DISTINCT AUTOR.nome
FROM AUTOR
JOIN LIVRO ON AUTOR.codautor = LIVRO.autor
JOIN EDITORA ON LIVRO.editora = EDITORA.codeditora
JOIN ENDERECO ON EDITORA.endereco = ENDERECO.codendereco
WHERE ENDERECO.estado NOT IN ('RIO GRANDE DO SUL', 'PARANÁ')
ORDER BY AUTOR.nome ASC;
-- listar o autor com maior número de livros publicados
SELECT autor.codautor, autor.nome, COUNT(livro.cod) AS quantidade_publicacoes
FROM autor
JOIN livro ON autor.codautor = livro.autor
GROUP BY autor.codautor, autor.nome
ORDER BY quantidade_publicacoes DESC
LIMIT 1;
-- listar o nome dos autores com nenhuma publicação
select nome
from autor
left join livro on autor.codautor = livro.autor
where livro.cod is null
order by nome ASC;