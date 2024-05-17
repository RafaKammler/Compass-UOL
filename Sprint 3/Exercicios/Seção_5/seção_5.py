
#! declarando variáveis
maior_numero_filmes = 0
ator_com_mais_filmes = ''
toda_bilheteria_bruta = 0
quantidade_de_linhas = 0
maior_receita_media_por_filme = 0
valores_coluna_3 = []
valores_coluna_2 = []
valores_coluna_0 = []
quantidade_de_aparicoes = {}
total_receita_bruta_por_ator = {}

with open('actors.csv', 'r') as arquivo:
    linhas = arquivo.readlines()
    
    for linha in linhas[1:]:
        
        #! Tratamento de dados
        if '"' in linha:
            linha = linha.replace(',', '', 1)
            linha = linha.replace('"', '')
        registro = linha.strip().split(',')
        
        ator = registro[0]
        
        toda_bilheteria_bruta += float(registro[5])
        
        quantidade_de_linhas = len(linhas) - 1
        
        numero_filmes = int(registro[2])
        
        valores_coluna_3.append(float(registro[3])) 
        
        valores_coluna_2.append(int(registro[2]))
        
        valores_coluna_0.append(registro[0])
        
        #! Cálculos
        
        #** Criação da lista de filmes e quantidade de aparições
        filme = registro[4]
        if filme in quantidade_de_aparicoes:
            quantidade_de_aparicoes[filme] += 1
        else:
            quantidade_de_aparicoes[filme] = 1
        
        #** Criação da lista de atores e receita bruta
        if ator in total_receita_bruta_por_ator:
            total_receita_bruta_por_ator[ator] += float(registro[1])
        else:
            total_receita_bruta_por_ator[ator] = float(registro[1])

#** Ator com mais filmes
maior_numero_filmes = max(valores_coluna_2)
indice_maior_valor = valores_coluna_2.index(maior_numero_filmes)
ator_com_mais_filmes = valores_coluna_0[indice_maior_valor]

#** Bilheteria média
media_bilheteria = round(toda_bilheteria_bruta / quantidade_de_linhas, 2)

#** Ator com maior receita média por filme
maior_receita_media_por_filme = max(valores_coluna_3)
indice_maior_receita = valores_coluna_3.index(maior_receita_media_por_filme)
ator_com_maior_receita_media = valores_coluna_0[indice_maior_receita]

#** Ordenação da quantidade de aparições
quantidade_de_aparicoes = sorted(quantidade_de_aparicoes.items(), key=lambda item: item[1], reverse=True)
#** Ordenação da receita bruta por ator
total_receita_bruta_por_ator = sorted(total_receita_bruta_por_ator.items(), key=lambda item: item[1], reverse=True) 

#** Criação dos arquivos de saída

with open('Etapa_1.txt', 'w') as saida:
    print(f'Ator: {ator_com_mais_filmes}, Número de filmes: {maior_numero_filmes}', file=saida)

with open('Etapa_2.txt', 'w') as saida2:
    print(f'Bilheteria média: {media_bilheteria}', file=saida2)

with open('Etapa_3.txt', 'w') as saida3:
    print(f'Ator: {ator_com_maior_receita_media}, Receita média: {maior_receita_media_por_filme}', file=saida3)

with open('Etapa_4.txt', 'w') as saida4:
    for filme, quantidade_de_aparicoes in quantidade_de_aparicoes:
        print(f'O filme: {filme} aparece {quantidade_de_aparicoes} vez(es) no dataset,', file=saida4)

with open('Etapa_5.txt', 'w') as saida5:
    for ator, receita_bruta in total_receita_bruta_por_ator:
        print(f'O ator: {ator} possui uma receita bruta total de: {receita_bruta}', file=saida5)