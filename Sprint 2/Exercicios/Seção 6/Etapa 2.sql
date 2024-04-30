SELECT 
	edi.codeditora AS CodEditora,
	edi.nome AS NomeEditora,	
	count (liv.cod) AS QuantidadeLivros
FROM editora AS edi
LEFT JOIN livro AS liv 
ON liv.editora = edi.codeditora 
GROUP BY edi.codeditora
ORDER BY QuantidadeLivros DESC
LIMIT 5;