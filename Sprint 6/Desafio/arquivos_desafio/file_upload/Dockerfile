FROM python:alpine3.20


WORKDIR /app


COPY . .


RUN pip3 install --no-cache-dir -r requirements.txt


VOLUME /app/data


CMD ["python", "main_script.py"]

