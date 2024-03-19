import os
import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm
import logging
from psycopg2 import errors as psycopg_errors
from data_utils import sql_dim_user, sql_user_metrics

# Configure logging
logging.basicConfig(filename='data_insertion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

import os
import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm
import logging
from psycopg2 import errors as psycopg_errors
from data_utils import sql_dim_user, sql_user_metrics

# Configure logging
logging.basicConfig(filename='data_insertion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def execute_query(cursor, query_name, query):
    try:
        cursor.execute(query)
        logging.info(f"Query executed successfully: {query_name}")
    except psycopg_errors.UniqueViolation as e:
        logging.warning(f"Table {query_name} already exists")
    except Exception as e:
        logging.error(f"Error executing query: {query_name}. Error: {str(e)}")
        print(f"Error executing query: {query_name}. Error: {str(e)}")  # Print error message
        raise

def insert_data():
    # Get the current directory of the script
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Load environment variables from secrets.env file in the same directory as the script
    dotenv_path = os.path.join(current_dir, 'secrets.env')
    load_dotenv(dotenv_path)

    # Environment variables
    DB_HOST= os.getenv('DB_HOST')
    DB_USER= os.getenv('DB_USER')
    DB_PASSWORD= os.getenv('DB_PASSWORD')
    DB_NAME= os.getenv('DB_NAME')
    DB_PORT= os.getenv('DB_PORT')

    # List of SQL queries along with their names
    sql_queries = [
        ("User Dimensions Query", sql_dim_user),
        ("User Metrics Query", sql_user_metrics),
    ]

    for query_name, query in tqdm(sql_queries, desc="Executing SQL queries", unit="query"):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            cursor = conn.cursor()

            # Execute the query
            execute_query(cursor, query_name, query)

            # Commit changes in the database
            conn.commit()
        except Exception as e:
            logging.error(f"Error executing query: {query_name}. Error: {str(e)}")
            if 'conn' in locals():
                conn.rollback()  # Rollback transaction if connection is established
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

    logging.info("All queries executed successfully.")

if __name__ == "__main__":
    insert_data()


def insert_data():
    # Get the current directory of the script
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Load environment variables from secrets.env file in the same directory as the script
    dotenv_path = os.path.join(current_dir, 'secrets.env')
    load_dotenv(dotenv_path)

    # Environment variables
    DB_HOST= os.getenv('DB_HOST')
    DB_USER= os.getenv('DB_USER')
    DB_PASSWORD= os.getenv('DB_PASSWORD')
    DB_NAME= os.getenv('DB_NAME')
    DB_PORT= os.getenv('DB_PORT')

    # List of SQL queries along with their names
    sql_queries = [
        ("User Dimensions Query", sql_dim_user),
        ("User Metrics Query", sql_user_metrics),
    ]

    for query_name, query in tqdm(sql_queries, desc="Executing SQL queries", unit="query"):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            cursor = conn.cursor()

            # Execute the query
            execute_query(cursor, query_name, query)

            # Commit changes in the database
            conn.commit()
        except Exception as e:
            logging.error(f"Error executing query: {query_name}. Error: {str(e)}")
            if 'conn' in locals():
                conn.rollback()  # Rollback transaction if connection is established
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

    logging.info("All queries executed successfully.")

if __name__ == "__main__":
    insert_data()
