import pandas as pd
from sqlalchemy import create_engine, text
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
        logging.error(f"Tipo do erro: {type(e).__name__}")
        logging.error(f"Detalhes do erro: {str(e)}")
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
        logging.error(f"Tipo do erro: {type(e).__name__}")
        logging.error(f"Detalhes do erro: {str(e)}")
        if hasattr(e, '__traceback__'):
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
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
        logging.error(f"Tipo do erro: {type(e).__name__}")
        logging.error(f"Detalhes do erro: {str(e)}")
        exit(1)

# Função para testar a conexão com o banco (Corrigida para usar text() do SQLAlchemy)
@log_time
def test_db_connection(engine):
    try:
        logging.info("Testando conexão com o banco...")
        with engine.connect() as conn:
            # Usando text() para criar um objeto executável SQL
            result = conn.execute(text("SELECT 1"))
            logging.info("Conexão testada com sucesso!")
        return True
    except Exception as e:
        logging.error(f"Erro ao testar conexão: {e}")
        logging.error(f"Tipo do erro: {type(e).__name__}")
        logging.error(f"Detalhes do erro: {str(e)}")
        return False

# Enviando os dados para o banco de dados em chunks
@log_time
def send_to_db_in_chunks(chunks, engine):
    try:
        logging.info("Enviando dados para o banco de dados PostgreSQL...")
        first_chunk = True
        for i, chunk in enumerate(chunks):
            chunk = process_data(chunk)  # Processando cada chunk
            
            if first_chunk:
                # Na primeira iteração, substitui a tabela existente
                chunk.to_sql('voos', con=engine, if_exists='replace', index=False)
                first_chunk = False
                logging.info("Tabela criada e primeiro chunk enviado com sucesso.")
            else:
                # Nas iterações seguintes, apenas adiciona à tabela existente
                chunk.to_sql('voos', con=engine, if_exists='append', index=False)
                
            logging.info(f"Chunk {i+1} enviado com sucesso. Número de linhas: {len(chunk)}")
        logging.info("Todos os dados enviados com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao enviar dados para o banco de dados: {e}")
        logging.error(f"Tipo do erro: {type(e).__name__}")
        logging.error(f"Detalhes do erro: {str(e)}")
        # Se possível, imprima algumas linhas do chunk problemático
        if 'chunk' in locals():
            try:
                logging.error(f"Primeiras linhas do chunk problemático: {chunk.head()}")
                logging.error(f"Tipos de dados do chunk: {chunk.dtypes}")
            except:
                logging.error("Não foi possível exibir informações do chunk problemático")
        exit(1)

# Fluxo principal
if __name__ == "__main__":
    try:
        file_path = 'dados_combinados.csv'
        logging.info(f"Iniciando processamento do arquivo: {file_path}")

        # Leitura do arquivo CSV em chunks
        chunks = read_csv_in_chunks(file_path)

        # Conexão com o banco de dados
        engine = create_db_connection(database_url)
        
        # Teste a conexão antes de prosseguir
        if not test_db_connection(engine):
            logging.error("Não foi possível estabelecer conexão estável com o banco de dados. Encerrando.")
            exit(1)

        # Envio dos dados para o banco de dados em chunks
        send_to_db_in_chunks(chunks, engine)
        
        logging.info("Processamento concluído com sucesso!")
    except Exception as e:
        logging.error(f"Erro não tratado no fluxo principal: {e}")
        logging.error(f"Tipo do erro: {type(e).__name__}")
        logging.error(f"Detalhes do erro: {str(e)}")
        exit(1)