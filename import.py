import pandas as pd
from sqlalchemy import create_engine, text, Text
import logging
import time

# Definir o database_url no início do código
database_url = 'postgresql://myuser:mypassword@my_postgres:5432/mydatabase'

# Configuração do logger
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def log_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logging.info(f"Tempo de execução de '{func.__name__}': {execution_time:.4f} segundos")
        return result
    return wrapper

@log_time
def read_csv_in_chunks(file_path, chunk_size=10000):
    try:
        logging.info(f"Iniciando a leitura do arquivo CSV: {file_path}")
        
        # Forçando o separador para vírgula
        separator = ','

        chunks = pd.read_csv(
            file_path, 
            sep=separator, 
            chunksize=chunk_size,
            encoding='utf-8',
            low_memory=False,
            on_bad_lines='warn'
        )
        
        # Testar a leitura de um dos chunks para garantir que os dados estão corretos
        for chunk in chunks:
            logging.info(f"Primeiras linhas do chunk: {chunk.head()}")
            break  # Verificar apenas o primeiro chunk
            
        logging.info("Arquivo CSV lido em chunks com sucesso.")
        return chunks
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo CSV: {e}")
        exit(1)

@log_time
def create_db_connection(database_url):
    try:
        logging.info("Iniciando a conexão com o banco de dados")
        engine = create_engine(database_url)
        logging.info("Conexão com o banco de dados estabelecida com sucesso.")
        return engine
    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        exit(1)

@log_time
def test_db_connection(engine):
    try:
        logging.info("Testando conexão com o banco...")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logging.info("Conexão testada com sucesso!")
        return True
    except Exception as e:
        logging.error(f"Erro ao testar conexão: {e}")
        return False

@log_time
def send_to_db_in_chunks(chunks, engine):
    try:
        logging.info("Enviando dados para o banco de dados PostgreSQL...")
        first_chunk = True
        total_rows = 0
        
        for i, chunk in enumerate(chunks):
            if chunk.empty:
                logging.warning(f"Chunk {i+1} está vazio, pulando...")
                continue
                
            logging.info(f"Chunk {i+1} - Número de colunas: {len(chunk.columns)}")
            logging.info(f"Primeiras colunas: {chunk.columns[:5].tolist()}")

            chunk = chunk.astype(str)  # Convertendo todas as colunas para string
            
            row_count = len(chunk)
            total_rows += row_count
            
            if first_chunk:
                chunk.to_sql('voos', con=engine, if_exists='replace', index=False, dtype={col: Text for col in chunk.columns})
                first_chunk = False
                logging.info(f"Tabela criada e primeiro chunk enviado. Linhas: {row_count}")
            else:
                chunk.to_sql('voos', con=engine, if_exists='append', index=False)
                logging.info(f"Chunk {i+1} enviado. Linhas: {row_count}")
                
        logging.info(f"Total de {total_rows} linhas importadas.")
    except Exception as e:
        logging.error(f"Erro ao enviar dados: {e}")
        exit(1)

if __name__ == "__main__":
    try:
        file_path = 'dados_combinados.csv'
        logging.info(f"Iniciando processamento do arquivo: {file_path}")
        
        chunks = read_csv_in_chunks(file_path)
        engine = create_db_connection(database_url)
        
        if not test_db_connection(engine):
            logging.error("Conexão falhou. Encerrando.")
            exit(1)
        
        send_to_db_in_chunks(chunks, engine)
        
        logging.info("Processamento concluído com sucesso!")
    except Exception as e:
        logging.error(f"Erro no fluxo principal: {e}")
        exit(1)

