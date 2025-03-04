import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame
from datetime import datetime

load_dotenv()

def baixar_arquivos_gdrive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)

def show_files(directory):
    files_repo = []
    all_files = os.listdir(directory)
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            path_complete = os.path.join(directory, filename)
            files_repo.append(path_complete)
    print(files_repo)
    return files_repo

    #function to read the csv and add on duckdb PyRelation(dataframeDuck)
def read_csv_duckdb(file_path):
        dfDuck = duckdb.read_csv(file_path)
        print(dfDuck)
        print(type(dfDuck)) 
        return dfDuck
        #conn = duckdb.connect(':memory:')
        #conn.execute("CREATE TABLE data AS SELECT * FROM read_csv_auto('"+file_path+"')")
        #return conn.table('data')

def transform(df:DuckDBPyRelation) -> DataFrame:
    #pr_transformed = duckdb.sql("""SELECT categoria, COUNT(*) AS total_vendas, SUM(valor) AS valor_total FROM df GROUP BY categoria ORDER BY valor_total DESC""").df()
    pr_transformed = duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()
    print(pr_transformed)
    return pr_transformed

def save_on_postgre(df_duckdb,table):
     DATABASE_URL = os.getenv("DATABASE_URL")
     engine = create_engine(DATABASE_URL)
     df_duckdb.to_sql(table, engine, if_exists='replace', index=False)

def db_connect():
     return duckdb.connect(database='duckdb.db', read_only=False)

def initialize_table(conn):
    #conn = db_connect()
    conn.execute("CREATE TABLE IF NOT EXISTS historic_files (file_name VARCHAR, process_time TIMESTAMP)")

def insert_file(conn, file_name):
    conn.execute("INSERT INTO historic_files(file_name, process_time) VALUES (?,?)",(file_name, datetime.now()))
                  #VALUES ('{}', '{}')".format(file_name, datetime.now()))

def get_files(conn):#arquivos_proces
    return set(row[0] for row in conn.execute("SELECT file_name FROM historic_files").fetchdf())

if __name__ == "__main__":
    url_pasta = 'https://drive.google.com/file/d/1X68rAOyf7Eczoxw5ljAZljO9sX7chhes'#'https://drive.google.com/drive/folders/1maqV7E3NRlHp12CsI4dvrCFYwYi7BAAf'
    diretorio_local = './pasta/gdow'
    #baixar_arquivos_gdrive(url_pasta, diretorio_local)
    files = show_files(diretorio_local)

    con = db_connect()
    initialize_table(con)
    processed = get_files(con)

    for file_path in files:
        file = os.path.basename(file_path)
        if file not in processed:
            df = read_csv_duckdb(file_path)
            pandas_df_transformed = transform(df)
            save_on_postgre(pandas_df_transformed, 'vendas_calculado')
            insert_file(con, file)
            print(f'Dados {file} salvos com sucesso no Postgre')
            print(f'ETL finalizado com sucesso')
        else:
            print(f'Arquivo já processado {file}')

    #for file in get_files:
     #   df = read_csv_duckdb(file)
      #  pandas_df_transformed = transform(df)
       # save_on_postgre(pandas_df_transformed, 'vendas_calculado')
        #print('Dados salvos com sucesso no Postgre')
        #print('ETL finalizado com sucesso')

        #duckdbPR = read_csv_duckdb(files)
        #transform(duckdbPR)
        #save_on_postgre(duckdbPR, 'vendas') */