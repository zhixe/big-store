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
datasetName="cost_metric"
datasetProducts="products"
datasetOrders="orders"
datasetSales="sales"
datasetCustomers="customers"

# Set up logging configuration
# WSL
log_file = f"/mnt/c/Users/datamicron/Documents/Project/big_store/logs/log_PROD_analytics_{datasetName}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# WINDOWS
# log_file = f"C:/Users/datamicron/Documents/Project/big_store/logs/log_PROD_analytics_{datasetName}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename=log_file)

# Add a StreamHandler to print log messages to the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)
logging.getLogger().addHandler(console_handler)

class etl:
    def __init__(self):
        self.table_name = datasetName
        self.table_orders = datasetOrders
        self.table_sales = datasetSales
        self.table_customers = datasetCustomers
        self.table_products = datasetProducts
        self.mysql_host = os.getenv("mysqlHost")
        self.mysql_port = os.getenv("mysqlPort")
        self.mysql_username = os.getenv("mysqlUsername")
        self.mysql_password = os.getenv("mysqlPassword")
        self.mysql_database = os.getenv("mysqlDatabase")
        self.mysql_databaseAnalytics = os.getenv("mysqlDatabaseAnalytics")

    def create_table(self):
        try:
            # Establish a connection to the MySQL server
            conn = mysql.connector.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user=self.mysql_username,
                password=self.mysql_password,
                database=self.mysql_databaseAnalytics
            )

            # Establish a connection to the MySQL server
            conn = mysql.connector.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user=self.mysql_username,
                password=self.mysql_password,
                database=self.mysql_databaseAnalytics
            )

            # Create the table if it doesn't exist
            cursor = conn.cursor()

            drop_table_query = f"""
            DROP TABLE IF EXISTS {self.mysql_databaseAnalytics}.{self.table_name};
            """

            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.mysql_databaseAnalytics}.{self.table_name}
            CHARACTER SET utf8 COLLATE utf8_general_ci (
            SELECT
                b.orderID,
                DATE_FORMAT(b.orderDate, '%Y-%M') AS yearMonth,
                SUM(a.sales - a.profit) AS totalCost
            FROM {self.mysql_database}.{self.table_sales} AS a
            LEFT JOIN {self.mysql_database}.{self.table_orders} AS b
            ON a.orderID = b.orderID
            GROUP BY 
                yearMonth,b.orderID
            ORDER BY
                yearMonth ASC
            )
            """

            cursor.execute(drop_table_query)
            cursor.execute(create_table_query)

            # Close the connection
            conn.close()

            print("\n")
            logging.info(f"[[ {self.table_name.upper()} ]]")
            logging.info(f"Table {self.mysql_databaseAnalytics}.{self.table_name} created successfully.")
        except mysql.connector.Error as error:
            logging.error(f"An error occurred while creating the table: {error}")
            raise

    def transformation(self):
        try:
            logging.info(f"Transforming data from {self.mysql_databaseAnalytics} to {self.table_name}")

            # Establish a connection to the MySQL server
            conn = mysql.connector.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user=self.mysql_username,
                password=self.mysql_password,
                database=self.mysql_databaseAnalytics
            )

            # Truncate the table to avoid duplicates
            cursor = conn.cursor()
            truncate_query = f"TRUNCATE TABLE {self.table_name}"
            cursor.execute(truncate_query)

            # Export the DataFrame to MySQL
            cursor = conn.cursor()

            # Insert data into the table
            alter_query = f"""
            ALTER TABLE {self.mysql_databaseAnalytics}.{self.table_name}
            ADD total_cost_id INT AUTO_INCREMENT PRIMARY KEY
            """
            cursor.execute(alter_query)

            # Close the connection
            conn.commit()
            conn.close()
            logging.info("Data imported successfully.")
        except mysql.connector.Error as error:
            logging.error(f"An error occurred while transforming data from {self.mysql_databaseAnalytics} to {self.table_name}: {error}")
            raise


if __name__ == "__main__":
    # Create an instance of transformation and pass the necessary parameters
    data_transformation = etl()

    # Create the table
    data_transformation.create_table()

    # Extract data from the CSV file and import it into the table
    data_transformation.transformation()
