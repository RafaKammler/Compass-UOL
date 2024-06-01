with open('number.txt') as arquivo:
    conteudo = arquivo.read()
    numeros = list(map(int, conteudo.split()))
    numeros_pares = filter(lambda p: p % 2 == 0, numeros)
    lista_5_maiores = sorted(numeros_pares, reverse=True)[:5]
    soma = sum(lista_5_maiores)

print(lista_5_maiores)
print(soma)