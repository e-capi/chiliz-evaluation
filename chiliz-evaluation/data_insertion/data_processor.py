import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from data_utils import Base, Stg_user, Str_user_registration, Stg_user_kyc, Dim_country, Fact_transaction, Dim_conversion_rate
import logging
from dotenv import load_dotenv
from tqdm import tqdm

# Get the current directory of the script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Load environment variables from secrets.env file in the same directory as the script
dotenv_path = os.path.join(current_dir, 'secrets.env')
load_dotenv(dotenv_path)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def insert_raw_data_to_database():
    try:
        # Database connection parameters
        DB_HOST = os.getenv('DB_HOST')
        DB_USER = os.getenv('DB_USER')
        DB_PASSWORD = os.getenv('DB_PASSWORD')
        DB_NAME = os.getenv('DB_NAME')
        DB_PORT = os.getenv('DB_PORT')

        # Define Excel file path
        excel_file_path = '../../raw_data/sql_dbt_test_2024.2.xlsx'


        # Define mapping of classes to sheet names
        class_sheet_mapping = {
            Stg_user: 'stg_user',
            Str_user_registration: 'str_user_registration',
            Stg_user_kyc: 'stg_user_kyc',
            Dim_country: 'dim_country',
            #Dim_user: 'dim_user',
            Fact_transaction: 'fact_transaction',
            Dim_conversion_rate: 'dim_conversion_rate',
            #User_metrics: 'user_metrics'
        }

        # Create database engine and session
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        # Read Excel file and insert data into corresponding tables
        xls = pd.ExcelFile(excel_file_path)
        for db_class, sheet_name in class_sheet_mapping.items():
            logger.info(f"Inserting data for {db_class.__name__} from sheet: {sheet_name}")

            # Load data from Excel sheet into DataFrame
            df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
            df.rename(columns=lambda x: x.lower(), inplace=True)
            df.replace({pd.NA: None}, inplace=True)  # Replace pandas NA values with None for consistency

            # Check if table exists
            inspector = inspect(engine)
            if db_class.__table__.name in inspector.get_table_names():
                # Check if table is empty
                session = Session()
                if session.query(db_class).count() == 0:
                    # Replace the table with new data
                    session.query(db_class).delete()
                    session.commit()
                else:
                    logger.info(f"Table {db_class.__table__.name} already exists and is not empty. Skipping insertion.")
                    session.close()
                    continue
                session.close()

            with tqdm(total=df.shape[0], desc=f"Inserting data for {db_class.__name__}", unit="row") as pbar:
                # Insert data into the database
                session = Session()
                session.bulk_insert_mappings(db_class, df.to_dict(orient='records'))

                # Commit the changes
                session.commit()
                session.close()
                pbar.update(df.shape[0])

        logger.info("Data insertion completed successfully")
    except Exception as e:
        logger.exception("An error occurred during data insertion")

if __name__ == "__main__":
    insert_raw_data_to_database()
