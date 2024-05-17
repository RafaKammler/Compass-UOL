class Lampada:
    def __init__(self, ligada):
        self.estado = ligada

    def liga(self):
        self.estado = True

    def desliga(self):
        self.estado = False

    def esta_ligada(self):
        return self.estado

my_instance = Lampada(True)
print(f'A lâmpada está ligada? {my_instance.esta_ligada()}')

my_instance.desliga()
print(f'A lâmpada ainda está ligada? {my_instance.esta_ligada()}')