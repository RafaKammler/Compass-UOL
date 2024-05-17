def dividir_lista_em_3(lista):
    lista_a = lista
    tamanho_lista = len(lista_a)
    Quantia_trechos = tamanho_lista / 3
    Quantia_trechos = int(Quantia_trechos)
    lista_1 = lista_a[:Quantia_trechos]
    lista_2 = lista_a[Quantia_trechos:Quantia_trechos*2]
    lista_3 = lista_a[Quantia_trechos*2:Quantia_trechos*3]
    print(lista_1, lista_2, lista_3)


lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
dividir_lista_em_3(lista)
