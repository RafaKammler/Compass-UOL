def my_map(lista, f):
    return [f(x) for x in lista]

# Testando a função
lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
resultado = my_map(lista, lambda x: x**2)
print(resultado)