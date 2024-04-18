Na etapa 4.3 foi requisitada uma mudança no arquivo dados_de_vendas de maneira manual e criação do script consolidador_de_processamento_de_vendas.sh que gera um relatório final
 - Para a alteração do arquivo dados_de_vendas.csv utilizei uma inteligencia artificial, que alterou de maneira integral o conteúdo do arquivo
 - Para a criação e permissão de execução do arquivo "consolidador_de_processamento_de_vendas.sh" foram usados os comandos:
```
touch ~/Programa/consolidador_de_processamento_de_vendas.sh
chmod a+x ~/Programa/consolidador_de_processamento_de_vendas.sh
```
Já dentro do arquivo "consolidador_de_processamento_de_vendas.sh", que é acessado por meio do comando nano, foram inseridas as seguintes instruções:
```                         
cd ~/Programa
touch vendas/relatorio_fina.txt
cat vendas/backup/rel*.txt >> vendas/relatorio_fina.txt
```
- Que realizam o seguinte
  - Viaja até o diretório Programa
  - Cria o arquivo "relatorio_fina.txt" no diretório vendas
  - Copia a informação de todos os outros relatórios para dentro do "relatorio_fina.txt"
