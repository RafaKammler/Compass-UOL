lista_a = [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]

lista_b = [
        numero for numero in lista_a 
        if numero % 2 != 0
        ]
print(lista_b) 
