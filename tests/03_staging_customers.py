import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import logging
from datetime import datetime

# WSL
# load_dotenv("/mnt/c/Users/datamicron/Documents/Project/big_store/.env")
# WINDOWS
load_dotenv("C:/Users/datamicron/Documents/Project/big_store/.env")
datasetName="customers"

# Set up logging configuration
# WSL
# log_file = f"/mnt/c/Users/datamicron/Documents/Project/big_store/logs/log_TEST_staging__{datasetName}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# WINDOWS
log_file = fr"C:/Users/datamicron/Documents/Project/big_store/logs/log_TEST_staging__{datasetName}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
        self.customerID = "customerID"
        self.customerName = "customerName"
        self.customerType = "customerType"
        self.newCustomer = "newCustomer"
        self.country = "country"
        self.city = "city"
        self.state = "state"
        self.postalCode = "postalCode"
        self.region = "region"
        self.orderID = "orderID"
        self.gender = "gender"
        self.maritalStatus = "maritalStatus"
        self.religious = "religious"
        self.phoneNumber = "phoneNumber"
        self.age = "age"
        self.bloodGroup = "bloodGroup"
        self.designation = "designation"
        self.email = "email"
        self.companyAddress1 = "companyAddress1"
        self.companyAddress2 = "companyAddress2"
        self.homeAddress1 = "homeAddress1"
        self.homeAddress2 = "homeAddress2"
        self.companyName = "companyName"
        self.homeFaxno = "homeFaxno"
        self.officeFaxno = "officeFaxno"

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
                {self.customerID}        VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.customerName}      TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.customerType}      VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.newCustomer}       VARCHAR(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.country}            VARCHAR(60) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.city}               VARCHAR(70) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.state}              VARCHAR(70) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.postalCode}        INT NOT NULL,
                {self.region}             VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.orderID}           VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.gender}             VARCHAR(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.maritalStatus}     VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.religious}          VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                {self.phoneNumber}       VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.age}                INT NOT NULL,
                {self.bloodGroup}        VARCHAR(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.designation}        VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.email}              VARCHAR(70) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.companyAddress1}   TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.companyAddress2}   TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.homeAddress1}      TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.homeAddress2}      TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.companyName}       VARCHAR(70) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.homeFaxno}         VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                {self.officeFaxno}       VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL
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
                {self.customerID}        ,
                {self.customerName}      ,
                {self.customerType}      ,
                {self.newCustomer}       ,
                {self.country}            ,
                {self.city}               ,
                {self.state}              ,
                {self.postalCode}        ,
                {self.region}             ,
                {self.orderID}           ,
                {self.gender}             ,
                {self.maritalStatus}     ,
                {self.religious}          ,
                {self.phoneNumber}       ,
                {self.age}                ,
                {self.bloodGroup}        ,
                {self.designation }       ,
                {self.email}              ,
                {self.companyAddress1}   ,
                {self.companyAddress2}   ,
                {self.homeAddress1 }     ,
                {self.homeAddress2}      ,
                {self.companyName}       ,
                {self.homeFaxno}         ,
                {self.officeFaxno}
                )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    csv_file = fr'C:\Users\datamicron\Documents\Sample Dataset\CSV\test_{datasetName}.csv'

    # Create an instance of CSVToMySQL and pass the necessary parameters
    csv_to_mysql = CSVToMySQL(csv_file)

    # Create the table
    csv_to_mysql.create_table()

    # Extract data from the CSV file and import it into the table
    csv_to_mysql.extract_from_csv()
