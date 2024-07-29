import random
import pandas
import names
import time
import os


def random_number_generator():
    inteiros = []
    for __ in range(250):
        inteiros.append(random.randint(0, 10000))
    inteiros_reversed = inteiros[::-1]
    print(inteiros_reversed)


def animais():
    animais = ['cachorro', 'gato', 'papagaio', 'peixe', 'coelho', 'cavalo', 'vaca', 'pato', 'galinha', 'pombo', 'tartaruga', 'cobra', 'lagarto', 'rato', 'porco', 'ovelha', 'alpaca', 'llama', 'camelo', 'elefante']
    animais_crescente = sorted(animais)
    for animal in animais_crescente:
        print(animal)
    animais_df = pandas.DataFrame(animais_crescente, columns=['animais'])
    animais_df.to_csv('animais.csv', index=False)


def gerador_nomes_pessoas():
    random.seed(40)
    qtd_nomes_unicos = 3000
    qtd_nomes_aleatorios = 10000000
    aux = []
    for __ in range(0, qtd_nomes_unicos):
        aux.append(names.get_full_name())
    print(f'Gerando {qtd_nomes_aleatorios} nomes aleatórios...')
    dados = []
    for __ in range(0, qtd_nomes_aleatorios):
        dados.append(random.choice(aux))
    with(open('nomes_aleatorios.txt', 'w')) as arquivo:
        for nome in dados:
            arquivo.write(nome + '\n')


def main():
    gerador_nomes_pessoas()
    animais()
    random_number_generator()


if __name__ == '__main__':
    main()