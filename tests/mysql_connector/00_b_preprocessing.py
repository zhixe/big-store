import os
import pandas as pd
from openpyxl import load_workbook
import logging
from datetime import datetime

# Specify the Excel file path
# WINDOWS
excel_file = r'C:\Users\datamicron\Documents\Sample Dataset\Sample - Big Store.xlsx'
csvDir = r'C:\Users\datamicron\Documents\Sample Dataset\CSV'

# WSL
# excel_file = r'/mnt/c/Users/datamicron/Documents/Sample Dataset/Sample - Big Store.xlsx'
# csvDir = r'/mnt/c/Users/datamicron/Documents/Sample Dataset/CSV'

# Set up logging configuration
# WINDOWS
log_file = r"C:\Users\datamicron\Documents\vscode\house_pricing\logs\log_TEST_staging_preprocessing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# WSL
# log_file = f"/mnt/c/Users/datamicron/Documents/vscode/house_pricing/logs/log_TEST_staging_preprocessing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

print("\n")
logging.info("[[ DATA PREPROCESSING ]]")
logging.info("Parsing Excel to CSV format.")
# Iterate over each sheet and export to CSV
for sheet_name in sheet_names:
    # Read the sheet data
    df = excel_data.parse(sheet_name)

    # Specify the CSV file path for the current sheet
    csv_file = f'{csvDir}/{sheet_name.lower()}.csv'

    # Export the sheet data to CSV
    df.to_csv(csv_file, index=False)
    logging.info(f"CSV file {sheet_name} created.")

# Renaming the CSV files to lowercase
for file_name in os.listdir(csvDir):
    if file_name.endswith('.csv'):
        new_file_name = file_name.lower()
        os.rename(os.path.join(csvDir, file_name), os.path.join(csvDir, new_file_name))
logging.info("Data preprocessed successfully.")