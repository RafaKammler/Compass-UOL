# Etapa 1

A etapa 1 do desafio solicitava a criação de um Dockerfile com as instruções de execução de um script, que criaria uma imagem para ser utilizada em um container.

## Código carguru

O código carguru era um script pronto que nos foi repassado, que escolhia aleatoriamente entre uma lista predefinida de carros.

## Arquivo Dockerfile

A criação do arquivo Dockerfile foi extremamente simples, sendo necessárias somente 4 linhas de código para sua execução, onde a única coisa que realizei de diferente foi a escolha da versão do Python, por meio do comando:

```yml
FROM python:alpine3.20
```

Utilizei essa versão específica principalmente devido ao seu tamanho em MB, para poder poupar armazenamento no meu computador.

## Adição ao compose

Para o desafio, apesar de não solicitado, resolvi criar um arquivo `compose.yml` para melhor organização dos containers. Foram usadas configurações simples, que constroem a imagem do Dockerfile e realizam um bind mount, que faz com que a aplicação seja atualizada em tempo real. Tudo isso foi feito pelos seguintes comandos:

```yml
    build: ./Etapa_1/
    volumes: 
      - "./Etapa_1:/app"
```
