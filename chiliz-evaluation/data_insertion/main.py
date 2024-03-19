import os
import logging
import subprocess
import data_processor
import data_insertion
import time
from tqdm import tqdm

def start_docker_container():
    # Get the directory path of the script
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # Change the current working directory to the script directory
    os.chdir(script_dir)

    logging.info("Starting Docker container...")
    try:
        subprocess.run(["docker-compose", "up", "-d"], check=True)

        # ADelay to allow PostgreSQL to initialize
        logging.info("Waiting for PostgreSQL to initialize...")
        for _ in tqdm(range(5), desc="Waiting for PostgreSQL"):
            time.sleep(1)
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to start Docker container: {e}")
        exit(1)

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    start_docker_container()

    logging.info("Starting raw data processing...")
    data_processor.insert_raw_data_to_database()

    logging.info("Inserting DataMart data...")
    data_insertion.insert_data()

    logging.info("All processes completed successfully.")

if __name__ == "__main__":
    main()
