class Ordenadora:
    def __init__(self, listaBaguncada):
        self.lista = listaBaguncada
    
    def ordenacaoCrescente(self):
        return sorted(self.lista)
    
    def ordenacaoDecrescente(self):
        return sorted(self.lista, reverse=True)
def main():
    lista1 = [3,4,2,1,5]
    listaordenadacresc = Ordenadora(lista1).ordenacaoCrescente()
    print(listaordenadacresc)
    lista2 = [9,7,6,8]
    listaordenadadecresc = Ordenadora(lista2).ordenacaoDecrescente()
    print(listaordenadadecresc)
main()
