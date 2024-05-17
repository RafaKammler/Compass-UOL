def soma(numeros_str):
    numeros = numeros_str.split(', ')
    numeros = [int(num) for num in numeros]
    SOMANUMEROS = sum(numeros)
    return SOMANUMEROS

numeros_str = "1, 3, 4, 6, 10, 76"
print(soma(numeros_str))