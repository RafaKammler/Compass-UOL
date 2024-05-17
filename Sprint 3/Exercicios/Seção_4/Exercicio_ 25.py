class Aviao:
    def __init__(self, modelo, velocidade_maxima, capacidade, cor = 'Azul'):
        self.modelo = modelo
        self.velocidade_maxima = velocidade_maxima
        self.cor = cor
        self.capacidade = capacidade

info_aviao1 = ["BOIENG456, 1500 km/h, 400"]
info_aviao2 = ["Embraer Praetor 600, 863km/h, 14"]
info_aviao3 = ["Antonov An-2, 258 Km/h, 12"]
lista_avioes = []
for info_aviao in [info_aviao1, info_aviao2, info_aviao3]:
    for entrada in info_aviao:
        modelo, velocidade, capacidade = entrada.split(',')
        lista_avioes.append(Aviao(modelo.strip(), velocidade.strip(), capacidade.strip()))
        
for aviao in lista_avioes:
    print(f"O avião de modelo '{aviao.modelo}' possui uma velocidade máxima de {aviao.velocidade_maxima}, capacidade para {aviao.capacidade} passageiros e é da cor '{aviao.cor}'.")