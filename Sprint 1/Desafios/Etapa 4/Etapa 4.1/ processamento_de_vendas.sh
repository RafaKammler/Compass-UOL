cd ~/Programa

# criação dos diretórios

mkdir vendas
mkdir vendas/backup/
data=$(date +'%Y%m%d')

# transferencia e cópia dos arquivos csv e txt

cp ecommerce/dados*.csv vendas
cp vendas/dados_de_vendas.csv vendas/backup/dados-$data.csv
mv vendas/backup/dados-$data.csv vendas/backup/backup-dados-$data.csv
touch vendas/backup/relatorio-$data.txt

# Inserção dos requisitos do desafio para o arquivo relatorio

date +'%Y/%m/%d %H:%M' >> vendas/backup/relatorio-$data.txt
awk -F',' 'NR > 1{split($5, d, "/"); date = sprintf("%04d-%02d-%02d", d[3], d[2], d[1]); if (date < min || NR == 2) min = date} END {split(min, d, "-"); printf("%02d/%02d/%04d\n", d[3], d[2], d[1])}' vendas/dados_de_vendas.csv >> vendas/backup/relatorio-$data.txt
awk -F',' 'NR > 1{split($5, d, "/"); date = d[3] "-" d[2] "-" d[1]; if (date > max || NR == 2) max = date} END {split(max, d, "-"); print d[3] "/" d[2] "/" d[1]}' vendas/dados_de_vendas.csv >> vendas/backup/relatorio-$data.txt
wc -l < vendas/dados_de_vendas.csv | awk '{print $1 - 1}' >> vendas/backup/relatorio-$data.txt
head -n 10 vendas/dados_de_vendas.csv  >> vendas/backup/relatorio-$data.txt

# Finalização pela compactação e remoção dos arquivos inúteis

zip -r vendas/backup/backup-dados-$data.zip vendas/backup/backup-dados-$data.csv
rm vendas/backup/backup-dados-$data.csv
rm vendas/dados_de_vendas.csv

