import os
import pandas as pd
from openpyxl import load_workbook
import logging
from datetime import datetime

# Specify the Excel file path
excel_file = r'C:\Users\datamicron\Documents\Sample Dataset\Sample - Big Store.xlsx'
csvDir = r'C:\Users\datamicron\Documents\Sample Dataset\CSV'

# Set up logging configuration
log_file = r"C:\Users\datamicron\Documents\Project\big_store\logs\log_TEST_staging_preprocessing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename=log_file)

# Add a StreamHandler to print log messages to the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)
logging.getLogger().addHandler(console_handler)

# Load the Excel file
excel_data = pd.ExcelFile(excel_file)

# Get the sheet names
sheet_names = excel_data.sheet_names

logging.info("[[ DATA PREPROCESSING ]]")
logging.info("Parsing Excel to CSV format.")

# Method to clean and rename column names
def clean_column_name(column_name):
    return column_name.strip().replace(' ', '_').replace('\t', '_').lower()

# Remove existing CSV files
for file_name in os.listdir(csvDir):
    if file_name.endswith('.csv') and file_name.startswith('test_'):
        os.remove(os.path.join(csvDir, file_name))
        logging.info(f"Existing CSV file {file_name} removed.")

# Iterate over each sheet and export to CSV
for sheet_name in sheet_names:
    # Read the sheet data
    df = excel_data.parse(sheet_name)

    # Clean and rename the header row names
    df.columns = [clean_column_name(column_name) for column_name in df.columns]

    # Specify the CSV file path for the current sheet
    csv_file = f'{csvDir}/test_{sheet_name.lower()}.csv'

    # Export the sheet data to CSV
    df.to_csv(csv_file, index=False)
    logging.info(f"CSV file {sheet_name} created.")

logging.info("Data preprocessed successfully.")
