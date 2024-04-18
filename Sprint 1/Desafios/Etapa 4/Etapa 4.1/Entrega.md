### Nesta primeira etapa do desafio é onde se encontra a maior parte do código, a qual foi composta pelos seguintes passos

Passo 1 - uso do comando cd para chegar até o diretório desejado e criação de um arquivo em shell script para a execução dos comandos do terminal ubuntu

 - Para essa criação foram usados os comandos "touch processamento_de_vendas.sh" para a criação do arquivo
 - Uso do comando "chmod a+x processamento_de_vendas.sh" para permitir a execução do comando para todos os usuários

```
touch processamento_de_vendas.sh
chmod a+x processamento_de_vendas.sh
 ```

Passo 2 - começar a edição do conteudo do arquivo, para isso optei por me manter no terminal e usar o editor de texto nano, com o comando "nano processamento_de_vendas.sh"

 - Já dentro do arquivo fiz uso do comando "cd ~/Programa" para chegar a pasta desejada e do comando "mkdir vendas" para criar o diretório vendas

 - Depois disso usei o comando "cp ecommerce/dados*.csv vendas" para copiar o arquivo dados_de_vendas.csv para o diretório vendas

 - Após isso foi usado o comando "mkdir vendas/backup/" para criar o diretório backup e o comando "data=$(date +'%Y%m%d')" para definir a variavel data que será usada no futuro

 - O comando usado para a cópia do arquivo dados_de_vendas.csv para dentro do diretório backup com a data de execução foi "cp vendas/dados_de_vendas.csv vendas/backup/dados-$data.csv"

 - Já o comando usado para renomear o arquivo como solicitado foi o "mv vendas/backup/dados-$data.csv vendas/backup/backup-dados-$data.csv"
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

Passo 3 - A criação do arquivo relatorio.txt foi realizada com o comando "touch vendas/backup/relatorio-$data.txt" que adiciona a variavel data para impedir conflito entre relátorios de dias diferentes
 
 - No passo 3 foi necessaria a inserção de diversas informações no arquivo relatorio.txt, os quais foram realizados pelos comandos
   - "date +'%Y/%m/%d %H:%M' >> vendas/backup/relatorio-$data.txt" para transferir a data e horário do sistema

   - "awk -F',' 'NR > 1{split($5, d, "/"); date = sprintf("%04d-%02d-%02d", d[3], d[2], d[1]); if (date < min || NR == 2) min = date} END {split(min, d, "-"); printf("%02d/%02d/%04d\n", d[3], d[2], d[1])}' vendas/dados_de_vendas.csv >> vendas/backup/relatorio-$data.txt" para a analise de qual a data do primeiro registro de venda

   - "awk -F',' 'NR > 1{split($5, d, "/"); date = d[3] "-" d[2] "-" d[1]; if (date > max || NR == 2) max = date} END {split(max, d, "-"); print d[3] "/" d[2] "/" d[1]}' vendas/dados_de_vendas.csv >> vendas/backup/relatorio-$data.txt" para analisar qual a data do último registro de venda

    - "wc -l < vendas/dados_de_vendas.csv | awk '{print $1 - 1}' >> vendas/backup/relatorio-$data.txt" para analisar a quantia de itens únicos na lista, por meio da contagem de linhas e desconsideração da primeia por ser um cabeçalho

    - "head -n 10 vendas/dados_de_vendas.csv  >> vendas/backup/relatorio-$data.txt" para copiar as 10 primeiras linhas do arquivo
```
# Inserção dos requisitos do desafio para o arquivo relatorio

date +'%Y/%m/%d %H:%M' >> vendas/backup/relatorio-$data.txt
awk -F',' 'NR > 1{split($5, d, "/"); date = sprintf("%04d-%02d-%02d", d[3], d[2], d[1]); if (date < min || NR == 2) min = date} END {split(min, d, "-"); printf("%02d/%02d/%04d\n", d[3], d[2], d[1])}' vendas/dados_de_vendas.csv >> vendas/backup/relatorio-$data.txt
awk -F',' 'NR > 1{split($5, d, "/"); date = d[3] "-" d[2] "-" d[1]; if (date > max || NR == 2) max = date} END {split(max, d, "-"); print d[3] "/" d[2] "/" d[1]}' vendas/dados_de_vendas.csv >> vendas/backup/relatorio-$data.txt
wc -l < vendas/dados_de_vendas.csv | awk '{print $1 - 1}' >> vendas/backup/relatorio-$data.txt
head -n 10 vendas/dados_de_vendas.csv  >> vendas/backup/relatorio-$data.txt
```

Passo 4 - por fim foram usados 3 comandos:

 - "zip -r vendas/backup/backup-dados-$data.zip vendas/backup/backup-dados-$data.csv" para compactar o backup-dados-$data.csv em .zip

 - "rm vendas/backup/backup-dados-$data.csv para excluir o arquivo backup-dados-$data.csv

 - "rm vendas/dados_de_vendas.csv" e por fim a exclusão do arquivo dados_de_vendas.csv

```
# Finalização pela compactação e remoção dos arquivos

zip -r vendas/backup/backup-dados-$data.zip vendas/backup/backup-dados-$data.csv
rm vendas/backup/backup-dados-$data.csv
rm vendas/dados_de_vendas.csv
```

 



