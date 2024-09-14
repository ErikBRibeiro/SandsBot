FROM python:3.10-slim

LABEL Maintainer="SandsBot"

RUN apt-get update && apt-get install -y \
    gcc \
    make \
    libc6-dev \
    libffi-dev \
    python3-dev \
    wget

# Baixar e compilar o TA-LIB a partir do código-fonte
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir data

EXPOSE 8000

CMD ["python", "main_loop.py"]
