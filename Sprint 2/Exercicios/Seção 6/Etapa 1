SELECT 
	liv.cod AS CodLivro,
	liv.titulo AS Titulo,
	liv.autor AS CodAutor,
	aut.nome AS NomeAutor,
	liv.valor AS Valor,
	liv.editora AS CodEditora,
	edi.nome AS NomeEditora
	FROM livro AS liv
	LEFT JOIN autor AS aut
	ON aut.codautor = liv.autor 
	LEFT JOIN editora AS edi 
	ON edi.codeditora = liv.editora 
	ORDER BY Valor DESC 
	LIMIT 10;