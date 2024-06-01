with open('estudantes.csv') as arquivo:
    linhas = arquivo.readlines()

linhas = map(lambda linha: linha.strip().split(','), linhas)
linhas = map(lambda campos: [campos[0]] + sorted(map(float, campos[1:]), reverse=True), linhas)
linhas = map(lambda campos: [campos[0]] + campos[1:4] + [round(sum(campos[1:4])/3, 2)], linhas)

for linha in sorted(linhas):
    print(f'Nome: {linha[0]} Notas: {linha[1:4]} Média: {linha[4]}')