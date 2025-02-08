import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

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

if __name__ == "__main__":


    url_pasta = 'https://drive.google.com/file/d/1X68rAOyf7Eczoxw5ljAZljO9sX7chhes'#'https://drive.google.com/drive/folders/1maqV7E3NRlHp12CsI4dvrCFYwYi7BAAf'
    diretorio_local = './pasta/gdow'
    #baixar_arquivos_gdrive(url_pasta, diretorio_local)
    files = show_files(diretorio_local)

    for file in files:
        df = read_csv_duckdb(file)
        pandas_df_transformed = transform(df)
        save_on_postgre(pandas_df_transformed, 'vendas_calculado')
        print('Dados salvos com sucesso no Postgre')
        print('ETL finalizado com sucesso')

        #duckdbPR = read_csv_duckdb(files)
        #transform(duckdbPR)
        #save_on_postgre(duckdbPR, 'vendas')