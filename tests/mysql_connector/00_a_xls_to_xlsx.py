import pandas as pd
import logging
from datetime import datetime

# Set up logging configuration
# WINDOWS
log_file = f"C:/Users/datamicron/Documents/vscode/house_pricing/logs/log_TEST_staging_XLSX_conversion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# WSL
# log_file = f"/mnt/c/Users/datamicron/Documents/vscode/house_pricing/logs/log_TEST_staging_XLSX_conversion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename=log_file)

# Add a StreamHandler to print log messages to the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)
logging.getLogger().addHandler(console_handler)

def convert_xls_to_xlsx(xls_file, xlsx_file):
    logging.info("[[ XLS TO XLSX CONVERSION ]]")
    logging.info("Converting XLS file to XLSX file format.")
    # Read the .xls file using pandas
    df = pd.read_excel(xls_file, sheet_name=None)

    # Create a new Excel writer using openpyxl
    writer = pd.ExcelWriter(xlsx_file, engine='openpyxl')

    # Write each sheet to the new .xlsx file
    for sheet_name, df_sheet in df.items():
        df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)

    # Save the .xlsx file
    writer._save()

if __name__ == "__main__":
    # Specify the .xls and output .xlsx file paths
    #  WINDOWS
    xls_file = r'C:\Users\datamicron\Documents\Sample Dataset\XLS\Sample - Big Store.xls'
    xlsx_file = r'C:\Users\datamicron\Documents\Sample Dataset\Sample - Big Store.xlsx'

    # WSL
    # xls_file = r'/mnt/c/Users/datamicron/Documents/Sample Dataset/XLS/Sample - Big Store.xls'
    # xlsx_file = r'/mnt/c/Users/datamicron/Documents/Sample Dataset/Sample - Big Store.xlsx'

    convert_xls_to_xlsx(xls_file, xlsx_file)
    logging.info("File format converted successfully.")
