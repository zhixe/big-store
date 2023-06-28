import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import logging
from datetime import datetime

# WSL
load_dotenv("/mnt/c/Users/datamicron/Documents/Project/big_store/.env")
# WINDOWS
# load_dotenv("C:/Users/datamicron/Documents/Project/big_store/.env")
datasetName="sales"

# Set up logging configuration
# WSL
log_file = f"/mnt/c/Users/datamicron/Documents/Project/big_store/logs/log_PROD_staging__{datasetName}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# WINDOWS
# log_file = f"C:/Users/datamicron/Documents/Project/big_store/logs/log_PROD_staging__{datasetName}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
        self.mysql_database = os.getenv("mysqlDatabase")
        self.rowID = "rowID"
        self.orderID = "orderID"
        self.productID = "productID"
        self.customerID = "customerID"
        self.sales = "sales"
        self.quantity = "quantity"
        self.discount = "discount"
        self.profit = "profit"

    def create_table(self):
        try:
            # Establish a connection to the MySQL server
            conn = mysql.connector.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user=self.mysql_username,
                password=self.mysql_password,
                database=self.mysql_database
            )

            # Create the table if it doesn't exist
            cursor = conn.cursor()

            drop_table_query = f"""
            DROP TABLE IF EXISTS {self.table_name};
            """

            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                {self.rowID}     INT NOT NULL,
                {self.orderID}   VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.productID} VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.customerID} VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.sales}      DOUBLE NOT NULL,
                {self.quantity}   INT NOT NULL,
                {self.discount}   DOUBLE NOT NULL,
                {self.profit}    DOUBLE NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED
            """

            cursor.execute(drop_table_query)
            cursor.execute(create_table_query)

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

            # Replace NaN values with None
            df = df.where(pd.notnull(df), None)

            # Export the DataFrame to MySQL
            cursor = conn.cursor()

            # Insert data into the table
            insert_query = f"""
            INSERT INTO {self.table_name} (
                {self.rowID}     ,
                {self.orderID}   ,
                {self.productID} ,
                {self.customerID},
                {self.sales}      ,
                {self.quantity}   ,
                {self.discount}   ,
                {self.profit}     
                )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
    csv_file = fr'/mnt/c/Users/datamicron/Documents/Sample Dataset/CSV/{datasetName}.csv'

    # WINDOWS
    # csv_file = fr'C:\Users\datamicron\Documents\Sample Dataset\CSV\{datasetName}.csv'

    # Create an instance of CSVToMySQL and pass the necessary parameters
    csv_to_mysql = CSVToMySQL(csv_file)

    # Create the table
    csv_to_mysql.create_table()

    # Extract data from the CSV file and import it into the table
    csv_to_mysql.extract_from_csv()
