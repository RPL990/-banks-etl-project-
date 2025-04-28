import pandas as pd
import sqlite3
from datetime import datetime
import numpy as np

# 1. Leer tablas desde el sitio
URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
tables = pd.read_html(URL)
df = tables[1]
print(df)

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open('code_log.txt', 'a') as f:
        f.write(f"{timestamp} : {message}\n")

log_progress('Preliminares completos. Iniciando proceso ETL')


def extract(url, table_attribs):
    tables = pd.read_html(url)
    df = tables[1]
    
    extracted_data = pd.DataFrame()
    extracted_data['Name'] = df['Bank name']
    extracted_data['MC_USD_Billion'] = (
        df['Market cap (US$ billion)']  # <-- aquí agregamos el espacio correcto
        .astype(str)
        .str.replace(r'\n', '', regex=True)  # Limpia saltos de línea
        .astype(float)  # Convierte a float
    )
    
    return extracted_data

output_df = extract(URL, ["Bank name", "Market cap(US$ billion)"])
print(output_df)

log_progress('Extracción de datos completa. Iniciando proceso de transformación')


def transform(df, exchange_rate_file):
    '''This function transforms the extracted data.'''
    
    # Leer exchange_rate.csv
    exchange_rate = pd.read_csv(exchange_rate_file)
    exchange_rate = exchange_rate.set_index('Currency').to_dict()['Rate']
    
    # Agregar columnas nuevas
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    
    return df
transformed_df = transform(output_df, 'exchange_rate.csv')
print(transformed_df)
log_progress('Transformación de datos completa.')


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    
load_to_csv(transformed_df, 'transformed_bank_data.csv')  # Puedes cambiar el nombre si quieres

log_progress('Datos transformados cargados a CSV.')

def load_to_db(df, sql_connection, table_name):
    '''This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

conn = sqlite3.connect('Banks.db')

load_to_db(transformed_df, conn, 'Largest_banks')

log_progress('Datos cargados en la base de datos.')

def run_query(query_statement, sql_connection):
    '''This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing.'''
    
    cursor = sql_connection.cursor()
    print(f"Query: {query_statement}")  # Muestra la consulta
    cursor.execute(query_statement)
    records = cursor.fetchall()  # Trae todos los resultados
    for record in records:
        print(record)
''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
# Consulta 1: Mostrar toda la tabla
run_query("SELECT * FROM Largest_banks", conn)
log_progress('Consulta: mostrar toda la tabla ejecutada.')

# Consulta 2: Capitalización de mercado promedio en GBP
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)
log_progress('Consulta: capitalización de mercado promedio ejecutada.')

# Consulta 3: Nombres de los 5 principales bancos
run_query("SELECT Name FROM Largest_banks LIMIT 5", conn)
log_progress('Consulta: nombres de los 5 principales bancos ejecutada.')
