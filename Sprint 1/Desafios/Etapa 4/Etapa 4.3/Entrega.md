Na etapa 4.3 foi requisitada uma mudança no arquivo dados_de_vendas de maneira manual e criação do script consolidador_de_processamento_de_vendas.sh que gera um relatório final
 - Para isso utilizei o ChatGPT para alterar o arquivo de maneira integral
 - Para a criação do arquivo utilizei o mesmo método da criação do processamento_de_vendas.sh, por meio do comando touch e chmod
 - E dentro do arquivo .sh utilizei os comandos:
   -"cd ~/Programa" para alcançar o diretório desejado
   -"touch vendas/relatorio_fina.txt" para criação do arquivo relatorio_fina.txt
   -"cat vendas/backup/rel*.txt >> vendas/relatorio_fina.txt" para copiar os dados de todos os relatórios do pasta backup para o relatorio_fina
```                         
cd ~/Programa
touch vendas/relatorio_fina.txt
cat vendas/backup/rel*.txt >> vendas/relatorio_fina.txt
```
