import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime
import time

# Definir o database_url no início do código
database_url = 'postgresql://myuser:mypassword@my_postgres:5432/mydatabase'

# Configuração do logger
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

# Função para medir o tempo de execução de cada operação
def log_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logging.info(f"Tempo de execução de '{func.__name__}': {execution_time:.4f} segundos")
        return result
    return wrapper

# Leitura e processamento do arquivo CSV em partes (chunks)
@log_time
def read_csv_in_chunks(file_path, chunk_size=10000):
    try:
        logging.info(f"Iniciando a leitura do arquivo CSV: {file_path}")
        # Iterando sobre o CSV em chunks
        chunks = pd.read_csv(file_path, sep=";", chunksize=chunk_size)
        logging.info("Arquivo CSV lido em chunks com sucesso.")
        return chunks
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo CSV: {e}")
        exit(1)

# Tratamento de dados
@log_time
def process_data(df):
    try:
        logging.info("Iniciando o tratamento de dados...")

        # Ajustando as colunas de data para o formato datetime
        logging.info("Ajustando as colunas de data para o formato datetime...")
        df['Partida Prevista'] = pd.to_datetime(df['Partida Prevista'], errors='coerce', format='%d/%m/%Y %H:%M')
        df['Partida Real'] = pd.to_datetime(df['Partida Real'], errors='coerce', format='%d/%m/%Y %H:%M')
        df['Chegada Prevista'] = pd.to_datetime(df['Chegada Prevista'], errors='coerce', format='%d/%m/%Y %H:%M')
        df['Chegada Real'] = pd.to_datetime(df['Chegada Real'], errors='coerce', format='%d/%m/%Y %H:%M')

        logging.info("Datas ajustadas com sucesso.")
        
        # Ajustando outras colunas conforme necessário (removendo espaços em branco nas colunas de string)
        logging.info("Removendo espaços em branco nas colunas...")
        df.columns = df.columns.str.strip()

        logging.info(f"Tratamento concluído. Número de colunas: {len(df.columns)}")
        return df
    except Exception as e:
        logging.error(f"Erro ao tratar os dados: {e}")
        exit(1)

# Criação da conexão com o banco de dados PostgreSQL
@log_time
def create_db_connection(database_url):
    try:
        logging.info(f"Iniciando a conexão com o banco de dados: {database_url}")
        engine = create_engine(database_url)
        logging.info("Conexão com o banco de dados estabelecida com sucesso.")
        return engine
    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        exit(1)

# Enviando os dados para o banco de dados em chunks
@log_time
def send_to_db_in_chunks(chunks, engine):
    try:
        logging.info("Enviando dados para o banco de dados PostgreSQL...")
        for chunk in chunks:
            chunk = process_data(chunk)  # Processando cada chunk
            chunk.to_sql('voos', con=engine, if_exists='replace', index=False)
            logging.info(f"Chunk enviado com sucesso. Número de linhas: {len(chunk)}")
        logging.info("Todos os dados enviados com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao enviar dados para o banco de dados: {e}")
        exit(1)

# Fluxo principal
if __name__ == "__main__":
    file_path = 'dados_combinados.csv'

    # Leitura do arquivo CSV em chunks
    chunks = read_csv_in_chunks(file_path)

    # Conexão com o banco de dados
    engine = create_db_connection(database_url)

    # Envio dos dados para o banco de dados em chunks
    send_to_db_in_chunks(chunks, engine)

