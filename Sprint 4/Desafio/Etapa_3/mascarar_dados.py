import hashlib


def entrada_de_string():
    string_inserida = input("Insira a string que deseja converter para hash1: ")
    return string_inserida


def hash_string(string_inserida):
    return hashlib.sha1(string_inserida.encode()).hexdigest()


def main():
    
    input_de_string = entrada_de_string()
    transformar_em_hash = hash_string(input_de_string)
    print(f'Tradução: {transformar_em_hash}')

main()