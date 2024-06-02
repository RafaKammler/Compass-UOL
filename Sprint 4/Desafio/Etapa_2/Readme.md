# Etapa 2

## Reutilização de containers

__Pergunta:__

É possível reutilizar containers? Em caso positivo, apresente o comando necessário para reiniciar um dos containers parados em seu ambiente Docker?Não sendo possível reutilizar, justifique sua resposta.

__Resposta:__

Sim, é possivel reutilizar containers no Docker, para continuar o uso de um container que foi parado, deve ser usado o comando

```Docker
docker start <nome ou id do container>
```

esse comando simplesmente iniciará novamente o container.
