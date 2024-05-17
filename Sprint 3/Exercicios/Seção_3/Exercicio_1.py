from datetime import datetime

nome = 'Pedro'
idade = 10

ano_atual = datetime.now().year

cem_anos = ano_atual + (100 - idade)

print(cem_anos)
