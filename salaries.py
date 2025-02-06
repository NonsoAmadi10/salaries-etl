import pandas as pd
import psycopg2
import re
from os import getenv, path
from io import StringIO
from dotenv import load_dotenv
from typing import Dict, OrderedDict
from collections import OrderedDict


load_dotenv()

# Environment Configuration
DB_HOST = getenv('POSTGRES_HOST')
DB_USER = getenv('POSTGRES_USER')
DB_PORT = 5432
DB_NAME = getenv('POSTGRES_DB')
DB_PASSWORD = getenv('POSTGRES_PASSWORD')

# File Paths
SQL_SCHEMA_FILE = path.join(
    path.dirname(path.abspath(__file__)),
    "salaries.sql"  # Contains CREATE TABLE statement
)

CSV_FILE = path.join(
    path.dirname(path.abspath(__file__)),
    "salaries.csv"  # Actual CSV data file
)

def parse_sql_schema(sql_file_path: str) -> OrderedDict[str, str]:
    """
    Parses SQL schema file to extract column names and types in order.

    Returns:
        OrderedDict preserving column order with types
    """
    with open(sql_file_path, 'r') as sql_file:
        sql_content = sql_file.read()

    # Find CREATE TABLE statement
    table_pattern = re.compile(
        r"CREATE TABLE (\w+)\s*\((.*?)\);",
        re.DOTALL | re.IGNORECASE
    )
    match = table_pattern.search(sql_content)

    if not match:
        raise ValueError("No valid CREATE TABLE statement found")

    columns_str = match.group(2)
    schema = OrderedDict()

    # Extract columns and types
    column_pattern = re.compile(
        r"^\s*(\w+)\s+([\w\(\)]+)",
        re.MULTILINE | re.IGNORECASE
    )

    for col_match in column_pattern.finditer(columns_str):
        col_name = col_match.group(1).strip()
        col_type = col_match.group(2).split(' ')[0].upper()  # Get base type
        schema[col_name] = col_type

    return schema

def clean_data(df: pd.DataFrame, schema: OrderedDict) -> pd.DataFrame:
    """
    Cleans DataFrame to match database schema requirements

    1. Selects and orders columns according to schema
    2. Handles missing values
    3. Converts data types
    """
    # Align columns with schema order
    df = df[schema.keys()].copy()

    # Clean values
    df.replace(["Not Provided", ""], None, inplace=True)

    # Type conversions
    for col, dtype in schema.items():
        if dtype in ('INT', 'INTEGER', 'BIGINT'):
            df[col] = pd.to_numeric(df[col], errors='coerce')
        elif dtype in ('FLOAT', 'NUMERIC', 'DECIMAL'):
            df[col] = pd.to_numeric(df[col], errors='coerce')
        elif dtype == 'BOOLEAN':
            df[col] = df[col].astype(bool)
        elif dtype == 'DATE':
            df[col] = pd.to_datetime(df[col], errors='coerce')
        elif dtype == 'TEXT':
            df[col] = df[col].astype(str)

    # Convert pandas NaN to None for proper NULL handling
    return df.where(df.notnull(), None)

def create_postgres_table(conn, sql_file_path: str) -> None:
    """Executes DDL statements from schema file"""
    with open(sql_file_path, 'r') as f:
        ddl = f.read()

    with conn.cursor() as cursor:
        cursor.execute(ddl)
    conn.commit()

def load_csv_to_postgres(sql_schema_path: str, csv_path: str) -> None:
    """Main ETL pipeline"""
    try:
        with psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        ) as conn:
            # Create table if not exists
            create_postgres_table(conn, sql_schema_path)

            # Read and clean data
            schema = parse_sql_schema(sql_schema_path)
            df = pd.read_csv(csv_path)
            clean_df = clean_data(df, schema)

            # Extract table name from schema file
            with open(sql_schema_path, 'r') as f:
                table_name = re.search(r"CREATE TABLE (\w+)", f.read(), re.IGNORECASE).group(1)

            # Prepare data stream
            buffer = StringIO()
            clean_df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            # Efficient bulk insert
            with conn.cursor() as cursor:
                cursor.copy_expert(
                    f"COPY {table_name} FROM STDIN WITH CSV DELIMITER ','",
                    buffer
                )
            conn.commit()

            print(f"Successfully loaded {len(clean_df)} records")

    except Exception as e:
        raise RuntimeError(f"ETL failed: {str(e)}")



if __name__ == '__main__':
    load_csv_to_postgres(SQL_SCHEMA_FILE, CSV_FILE)
