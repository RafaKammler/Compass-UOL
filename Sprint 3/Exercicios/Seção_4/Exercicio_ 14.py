parametro_nomeado = 'alguma coisa'
x = 20

def funcao(lista):
    lista = [1, 3, 4, 'hello', parametro_nomeado, x]
    return lista 
for item in funcao([]):
    print(item)