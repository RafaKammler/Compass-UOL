from functools import reduce

lancamentos = [
    (200,'D'),
    (300,'C'),
    (100,'C')
]

def calcula_saldo(lancamentos) -> float:
    valores_c = list(map(lambda x: x[0] if x[1] == 'C' else 0, lancamentos))  
    valores_d = list(map(lambda x: x[0] if x[1] == 'D' else 0, lancamentos))  

    creditos = reduce(lambda x, y: x + y, valores_c)  
    debitos = reduce(lambda x, y: x + y, valores_d)  

    return creditos - debitos 

print(calcula_saldo(lancamentos))  