class Calculo:
    def __init__(self, X, Y):
        self.valor1 = X
        self.valor2 = Y
        
    def soma(self):
        soma = self.valor1 + self.valor2
        return f'somando: {self.valor1} + {self.valor2} = {soma}'
    
    def subtracao(self):
        subtracao = self.valor1 - self.valor2
        return f'Subtraindo: {self.valor1} - {self.valor2} = {subtracao}'
        
def main():
    calculo = Calculo(4, 5)
    print(calculo.soma())
    print(calculo.subtracao())
main()