### Nesta primeira etapa do desafio é onde se encontra a maior parte do código, a qual foi composta pelos seguintes passos

Passo 1 - uso do comando cd para chegar até o diretório desejado e criação de um arquivo em shell script para a execução dos comandos do terminal ubuntu

 - Para a criação e execução do arquivos foram usados dois comandos, sendo eles:
```
touch processamento_de_vendas.sh
chmod a+x processamento_de_vendas.sh
 ```
 - O primeiro sendo usado para a criação e o segundo para permitir a execução do arquivo "processamento_de_vendas.sh"

Passo 2 - começar a edição do conteudo do arquivo "processamento_de_vendas.sh" por meio do comando:
```
nano processamento_de_vendas.sh
```
 - A primeira ação realizada dentro do arquivo .sh é, alcançar o diretório especifico para o código ser executado, para isso é usado o comando:
```
cd ~/Programa
```
 - Já dentro da edição do arquivo são usados comandos para criação dos diretórios necessarios e a tranferencia de todos os arquivos usados para o resto da execução, sendo eles:
```
# criação das pastas

mkdir vendas
mkdir vendas/backup/
data=$(date +'%Y%m%d')

# transferencia e cópia dos arquivos csv e txt

cp ecommerce/dados*.csv vendas
cp vendas/dados_de_vendas.csv vendas/backup/dados-$data.csv
mv vendas/backup/dados-$data.csv vendas/backup/backup-dados-$data.csv
```
 - Onde cada linha de código, em ordem, realiza:

   - Criação do diretório vendas

   - Criação do diretório backup

   - Definição da variavel $data

   - Cópia do arquivo "dados_de_vendas.csv" para o diretório vendas

   - Cópia do "dados_de_vendas.csv" para o diretório backup e simultaneamente o renomeando para "dados-$data.csv", onde a váriavel $data equivale à data atual do sistema

   - Por fim a renomeação do arquivo "dados-$data.csv" para "backup-dados-$data.csv"
  
Passo 3 - Foi necessária a criação do arquivo relatorio, Para isso foi usado o comando:
```
touch vendas/backup/relatorio-$data.txt
```
 - Que utiliza a variável $data para evitar conflito entre relatórios
 
No passo 3 foi necessária a inserção de diversas informações no arquivo relatorio.txt, os quais foram realizados pelos comandos 
```
# Inserção dos requisitos do desafio para o arquivo relatorio

date +'%Y/%m/%d %H:%M' >> vendas/backup/relatorio-$data.txt
awk -F',' 'NR > 1{split($5, d, "/"); date = sprintf("%04d-%02d-%02d", d[3], d[2], d[1]); if (date < min || NR == 2) min = date} END {split(min, d, "-"); printf("%02d/%02d/%04d\n", d[3], d[2], d[1])}' vendas/dados_de_vendas.csv >> vendas/backup/relatorio-$data.txt
awk -F',' 'NR > 1{split($5, d, "/"); date = d[3] "-" d[2] "-" d[1]; if (date > max || NR == 2) max = date} END {split(max, d, "-"); print d[3] "/" d[2] "/" d[1]}' vendas/dados_de_vendas.csv >> vendas/backup/relatorio-$data.txt
wc -l < vendas/dados_de_vendas.csv | awk '{print $1 - 1}' >> vendas/backup/relatorio-$data.txt
head -n 10 vendas/dados_de_vendas.csv  >> vendas/backup/relatorio-$data.txt
```
 - Onde a função de cada código, respectivamente é

   - Escrever a data e hora atual, no modelo ano/mês/dia Hora:Minuto no relatório

   - Encontrar a data do primeiro registro de venda do arquivo e transferir ela para o relatório

   - Encotrar a data do último registro de venda do arquivo e transcrever para o relatório

   - Contar a quantia de linhas e diminuir 1, para descobrir a quantidade de itens únicos contidos no arquivo

   - Transcrever as 10 primeiras linhas do arquivo .csv para o relatório

Passo 4 - por fim foram usados 3 comandos:
```
# Finalização pela compactação e remoção dos arquivos

zip -r vendas/backup/backup-dados-$data.zip vendas/backup/backup-dados-$data.csv
rm vendas/backup/backup-dados-$data.csv
rm vendas/dados_de_vendas.csv
```
 - Que tinham como respectivas funções

   - Comprimir o arquivo "backup-dados-$data.csv" em um .zip

    - Excluir o arquivo "backup-dados-$data.csv"

    - Excluir o arquivo "dados_de_vendas.csv"
  
## Onde por fim, com a junção de todos os trechos o resultado foi:
### [Código](https://github.com/RafaKammler/Compass-UOL/blob/main/Sprint%201/Desafios/Etapa%204/Etapa%204.1/%20processamento_de_vendas.sh)

 



