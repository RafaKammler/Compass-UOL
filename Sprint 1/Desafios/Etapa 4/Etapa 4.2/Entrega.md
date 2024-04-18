Na etapa 4.2 foi necessária a automatização do comando processamento_de_vendas.sh, para ser realizado de segunda-feira a quinta-feira às 15:27
 - Para isso foi utilizado a função crontab do ubuntu, que pode ser editada por meio do comando
```
crontab -e
```
 - E já dentro do crontab é utilizado o comando:
```
27 15 * * 1-4 ~/Programa/processamento_de_vendas.sh
```
 - Que fará com que o arquivo "processamento_de_vendas.sh" seja executado aos 27 minutos e 15 horas dos dias 1-4 (segunda à quinta) da semana
