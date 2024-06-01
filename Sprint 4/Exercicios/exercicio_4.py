operadores = ['+','-','*','/','+']
operandos  = [(3,6), (-7,4.9), (8,-8), (10,2), (8,4)]
def calcular_valor_maximo(operadores, operandos):
    aplicar_operacao = lambda op, ab: (
        ab[0] + ab[1] if op == '+' else
        ab[0] - ab[1] if op == '-' else
        ab[0] * ab[1] if op == '*' else
        ab[0] / ab[1] if op == '/' else
        ab[0] % ab[1]
    )

    combinacoes = zip(operadores, operandos)
    resultados = map(lambda x: aplicar_operacao(x[0], x[1]), combinacoes)
    return max(resultados)

print(calcular_valor_maximo(operadores, operandos))