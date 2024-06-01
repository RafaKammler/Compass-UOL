conteudo = {
    "arroz": 4.99,
    "feijão": 3.49,
    "macarrão": 2.99,
    "leite": 3.29,
    "pão": 1.99
}
preco_total = 0
quantia_itens = 0

def maiores_que_media(conteudo:dict)->list:
    preco_total = sum(conteudo.values())
    quantia_itens = len(conteudo)
    media = preco_total / quantia_itens
    
    maiores = [(item, preco) for item, preco in conteudo.items() if preco > media]
    maiores = sorted(maiores, key=lambda x: x[1])

    return maiores

def main():
    mais_caros_que_media = maiores_que_media(conteudo)
    print(mais_caros_que_media)
    
main()
