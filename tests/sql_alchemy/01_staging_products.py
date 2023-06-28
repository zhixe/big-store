import os
import csv
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

# Parse the .env file and then load all the variables found as environment variables.
load_dotenv("C:/Users/datamicron/Documents/Project/big_store/.env")
datasetName="products"

# Set up logging
log_file = fr"C:/Users/datamicron/Documents/Project/big_store/logs/log_TEST_staging_{datasetName}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename=log_file)

# Add a StreamHandler to print log messages to the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)
logging.getLogger().addHandler(console_handler)

class CSVToMySQL:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.table_name = datasetName
        self.mysql_host = os.getenv("mysqlHost")
        self.mysql_port = os.getenv("mysqlPort")
        self.mysql_username = os.getenv("mysqlUsername")
        self.mysql_password = os.getenv("mysqlPassword")
        self.mysql_database = os.getenv("mysqlDatabaseTest")
        self.variables = self.create_variables()

        self.create_table()
        self.extract_from_csv()

    def create_variables(self):
        with open(self.csv_file, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)  # Read the first line as the header

            # Create variables for each column using the header
            variables = {}
            for column_name in header:
                # Remove any special characters or spaces from the column name
                variable_name = ''.join(e for e in column_name if e.isalnum())
                variables[variable_name] = column_name
        return variables

    def detect_data_type(self, values):
        data_types = set()
        for value in values:
            try:
                int(value)
                data_types.add(int)
            except ValueError:
                try:
                    float(value)
                    data_types.add(float)
                except ValueError:
                    try:
                        datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                        data_types.add(datetime)
                    except ValueError:
                        data_types.add(str)

        # Prioritize data types in the following order: datetime, int, float, str
        if datetime in data_types:
            return datetime
        elif int in data_types:
            return int
        elif float in data_types:
            return float
        else:
            return str

    def create_table(self):
        try:
            # Create the engine to establish a connection to the MySQL server
            engine = create_engine(
                f"mysql+mysqlconnector://{self.mysql_username}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}",
                echo=False
            )

            # Create the table if it doesn't exist
            with engine.begin() as connection:
                drop_table_query = f"DROP TABLE IF EXISTS {self.mysql_database}.{self.table_name}"
                connection.execute(text(drop_table_query))  # Use text() to execute the query as a string

                # The table is created:
                # with compressed format to reduce memory usage
                # with default character set
                # with optimal dataset format
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {self.mysql_database}.{self.table_name} (
                    {', '.join(f"`{variable}` {self.get_column_type(variable)}" for variable in self.variables.values())}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED
                """
                connection.execute(text(create_table_query))

            print("\n")
            logging.info(f"[[ {self.table_name.upper()} ]]")
            logging.info(f"Table {self.mysql_database}.{self.table_name} created successfully.")
        except Exception as error:
            logging.error(f"An error occurred while creating the table: {error}")
            raise

    def get_column_type(self, variable):
        values = [row[variable] for row in csv.DictReader(open(self.csv_file))]
        column_type = self.detect_data_type(values)
        
        if column_type == datetime:
            return 'DATETIME'
        elif column_type == int:
            return 'INT'
        elif column_type == float:
            return 'DOUBLE'
        else:
            max_length = max(len(str(value)) for value in values)
            if max_length <= 10:
                return f'CHAR({max_length})'
            elif max_length <= 100:
                return f'VARCHAR({max_length})'
            else:
                return 'TEXT'

    def extract_from_csv(self):
        try:
            logging.info(f"Importing data from {self.csv_file} to {self.table_name}")

            # Read the CSV file
            logging.info(f"Reading CSV file: {self.csv_file}")
            df = pd.read_csv(self.csv_file)

            # Replace NaN values with None
            df = df.where(pd.notnull(df), None)

            # Create a database connection
            engine = create_engine(
                f"mysql+mysqlconnector://{self.mysql_username}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
            )

            # Truncate the table to avoid duplicates
            with engine.begin() as connection:
                truncate_query = f"TRUNCATE TABLE {self.mysql_database}.{self.table_name}"
                connection.execute(text(truncate_query))

                # Insert data into the table
                df.to_sql(self.table_name, connection, if_exists="append", index=False)

            logging.info("Data imported successfully.")
        except Exception as error:
            logging.error(
                f"An error occurred while importing data from {self.csv_file} to {self.table_name}: {error}"
            )
            raise

if __name__ == "__main__":
    csv_file = fr"C:\Users\datamicron\Documents\Sample Dataset\CSV\test_{datasetName}.csv"  # Define the CSV file path
    csv_to_mysql = CSVToMySQL(csv_file)  # Create an instance of CSVToMySQL and pass the necessary parameters
