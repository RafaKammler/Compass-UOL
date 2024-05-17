import json
arquivo = open('person.json', 'r')
dados_arquivo = json.load(arquivo)
print(dados_arquivo)
arquivo.close()