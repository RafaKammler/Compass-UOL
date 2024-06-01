# Etapa 3

Já na etapa 3, nos foi solicitada, além da criação do Dockerfile para criação da imagem e execução do container, a criação de um script Python que traduz strings inseridas para a linguagem SHA-1, utilizando o hexdigest.

## Script mascarar-dados

O script Python que nos foi solicitado consistia no input de uma string que deveria ser convertida por meio do algoritmo SHA-1. Para o input da string e output do resultado, utilizei sintaxe básica do Python. Mas para a tradução da string, foi necessário o uso da biblioteca `hashlib` e de algumas outras instruções para legibilidade. Para isso, foram utilizados os seguintes comandos:

```python
import hashlib

def hash_string(string_inserida):
    return hashlib.sha1(string_inserida.encode()).hexdigest()
```

## Arquivo Dockerfile

Para a criação do Dockerfile dessa etapa, utilizei a mesma base da etapa 1, com comandos idênticos, alterando somente o nome e local dos arquivos. Sendo assim, também utilizei a versão alpine do Python para diminuir o espaço utilizado em disco.

## Adição ao compose

Já para a adição do compose na etapa 3, tive que realizar algumas modificações para que o container não se encerrasse automaticamente. Quando isso ocorre, ao tentar iniciar novamente o container, ocorre um problema relacionado à entrada de dados. Para resolver esse problema, utilizei dois comandos, sendo eles:

```yaml
    stdin_open: true
    tty: true   
```

- Onde o `stdin_open` serve para que o Docker mantenha o fluxo de entrada padrão aberto para o container, possibilitando a interação com o container depois dele ser executado.
- E o `tty` tem como função permitir que o Docker aloque um terminal para a interação com o script.

Mas no restante do arquuivo mantive as configurações iguais às da etapa 1, com exceção dos nomes de pastas e arquivos que foram alterados.
