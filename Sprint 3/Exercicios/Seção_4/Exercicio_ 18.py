speed = {'jan':47, 'feb':52, 'march':47, 'April':44, 'May':52, 'June':53, 'july':54, 'Aug':44, 'Sept':54}

valores_unicos = []
for n in speed.values():
    valores_unicos.append(n)
valores_unicos = set(valores_unicos)

valores_unicos = list(valores_unicos)
print(valores_unicos)