FROM python:alpine3.20


WORKDIR /app


COPY . .


RUN pip3 install --no-cache-dir -r requirements.txt


VOLUME /app/data


CMD ["python", "main_script.py"]

## docker volume create datasets_csv
## docker run --rm -v datasets_csv:/volume -v /home/rafael/Desktop/comp_sci/CompassUOL/Aulas/Sprint_6/Desafio/csv_files:/arquivos busybox cp -r /arquivos/. /volume/

## docker build -t raw-upload-img .
## docker run -i --rm  -v datasets_csv:/data --name raw-data-upload raw-upload-img

