FROM python:3.9-alpine

LABEL Maintainer="Valmir Barbosa"

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir data

EXPOSE 8000

CMD ["python", "main_loop.py"]
