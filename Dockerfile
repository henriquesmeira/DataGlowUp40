# Usando uma imagem base Python
FROM python:3.10-slim

# Definindo o diretório de trabalho no container
WORKDIR /app

# Copiando os arquivos necessários para dentro do container
COPY . /app

# Instalando as dependências necessárias
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary


# Definindo a variável de ambiente para não interagir com o prompt (necessário para execução de scripts)
ENV PYTHONUNBUFFERED=1

# Comando para rodar o script Python
CMD ["python", "import.py"]
