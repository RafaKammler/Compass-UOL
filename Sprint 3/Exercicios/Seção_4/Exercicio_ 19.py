import random

random_list = random.sample(range(500), 50)

lista_cresc = random_list.sort()
quantia_de_valores = len(random_list)

if quantia_de_valores % 2 == 0:
    valor_do_meio = quantia_de_valores // 2
    mediana = sum(random_list[valor_do_meio - 1:valor_do_meio + 1]) / 2
else:
    valor_do_meio = quantia_de_valores // 2
    mediana = random_list[valor_do_meio]

media = sum(random_list) / quantia_de_valores
valor_minimo = min(random_list)
valor_maximo = max(random_list)

print(f'Media: {media}, Mediana: {mediana}, Mínimo: {valor_minimo}, Máximo: {valor_maximo}')