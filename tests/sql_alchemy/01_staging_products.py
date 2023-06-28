import os
import csv
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

# THIS SRC CODE FOR WINDOWS ENVIRONMENT ONLY
# Parse the .env file and then load all the variables found as environment variables.
load_dotenv("C:/Users/datamicron/Documents/vscode/house_pricing/.env")
datasetName="products"

# Set up logging
log_file = fr"C:/Users/datamicron/Documents/vscode/house_pricing/logs/log_TEST_staging_{datasetName}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

    def create_table(self):
        try:
            # Create the engine to establish a connection to the MySQL server
            engine = create_engine(
                f"mysql+mysqlconnector://{self.mysql_username}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}",
                echo=False
            )

            with engine.begin() as connection:
                
                # 1. DROP TABLE
                drop_table_query = f"DROP TABLE IF EXISTS {self.mysql_database}.{self.table_name}"
                connection.execute(text(drop_table_query))  # Use text() to execute the query as a string

                # 2. CREATE TABLE
                # with compressed format to reduce memory usage
                # with default character set
                # with optimal dataset format
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {self.mysql_database}.{self.table_name} (
                    {', '.join(f"`{variable}` {self.get_column_type(variable)}" for variable in self.variables.values())}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED
                """
                connection.execute(text(create_table_query))

            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                {self.productID} VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, 
                {self.category} VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, 
                {self.subCategory} VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, 
                {self.productName} TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED
            """

            create_index = f"""
            CREATE INDEX idx_categoryIdx
            ON {self.table_name} ({self.productID}, {self.category}, {self.subCategory})
            """

            cursor.execute(drop_table_query)
            cursor.execute(create_table_query)
            cursor.execute(create_index)

            # Close the connection
            conn.close()

            print("\n")
            logging.info(f"[[ {self.table_name.upper()} ]]")
            logging.info(f"Table {self.mysql_database}.{self.table_name} created successfully.")
        except mysql.connector.Error as error:
            logging.error(f"An error occurred while creating the table: {error}")
            raise

    def extract_from_csv(self):
        try:
            logging.info(f"Importing data from {self.csv_file} to {self.table_name}")

            # Establish a connection to the MySQL server
            conn = mysql.connector.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user=self.mysql_username,
                password=self.mysql_password,
                database=self.mysql_database
            )

            # Truncate the table to avoid duplicates
            cursor = conn.cursor()
            truncate_query = f"TRUNCATE TABLE {self.table_name}"
            cursor.execute(truncate_query)

            # Read the CSV file
            logging.info(f"Reading CSV file: {self.csv_file}")
            df = pd.read_csv(self.csv_file)

            # Export the DataFrame to MySQL
            cursor = conn.cursor()

            # Insert data into the table
            insert_query = f"""
            INSERT INTO {self.table_name} (
                {self.productID}, 
                {self.category}, 
                {self.subCategory}, 
                {self.productName}
                )
            VALUES (%s, %s, %s, %s)
            """
            values = df.values.tolist()
            cursor.executemany(insert_query, values)

            # Close the connection
            conn.commit()
            conn.close()
            logging.info("Data imported successfully.")
        except mysql.connector.Error as error:
            logging.error(f"An error occurred while importing data from {self.csv_file} to {self.table_name}: {error}")
            raise


if __name__ == "__main__":
    # Specify the CSV file
    # WSL
    # csv_file = fr'/mnt/c/Users/datamicron/Documents/Sample Dataset/CSV/{datasetName}.csv'

    # WINDOWS
    csv_file = fr'C:\Users\datamicron\Documents\Sample Dataset\CSV\{datasetName}.csv'

    # Create an instance of CSVToMySQL and pass the necessary parameters
    csv_to_mysql = CSVToMySQL(csv_file)

    # Create the table
    csv_to_mysql.create_table()

    # Extract data from the CSV file and import it into the table
    csv_to_mysql.extract_from_csv()