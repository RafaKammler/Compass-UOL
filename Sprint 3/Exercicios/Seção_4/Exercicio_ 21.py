class Passaro:
    def __init__(self, som, voar = True):
        self.som = som
        self.voar = voar


class Pato(Passaro):
    def __init__(self, som):
        super().__init__(som)
        self.som = som

    def voar_status(self, voar):
        if voar:
            return f'Pato Voando... Pato emitindo som... {self.som}'


class Pardal(Passaro):
    def __init__(self, som):
        super().__init__(som)
        self.som = som

    def voar_status(self, voar):
        if voar:
            return f'Pardal Voando... Pardal emitindo som... {self.som}'


def main():
    pato = Pato("Quack Quack")
    print(pato.voar_status(True))
    pardal = Pardal("Piu Piu")
    print(pardal.voar_status(True))

main()