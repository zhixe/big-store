import glob
import subprocess
import os
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

def execute_python_file(file_path_test):
    """Execute a Python file."""
    subprocess.run(["python", file_path_test], check=True)

if __name__ == "__main__":
    # Specify the directory path where the Python files are located
    srcDirTest = r"C:\Users\datamicron\Documents\Project\big_store\tests\sql_alchemy"
    workDirTest = r"C:\Users\datamicron\Documents\Project\big_store" 

    # Get the current date and time
    current_date = datetime.now()

    # Calculate the date 7 days ago
    seven_days_ago = current_date - timedelta(days=7)

    os.chdir(f"{workDirTest}\logs")

    # Get the list of log files in the directory
    log_files = os.listdir()

    # Iterate over the log files
    for log_file in log_files:
        if match := re.search(r"\d{8}", log_file):
            # Parse the date from the file name
            file_date = datetime.strptime(match.group(), "%Y%m%d")

            # Check if the log file is older than or equal to 7 days
            if file_date <= seven_days_ago:
                # Remove the log file
                os.remove(log_file)

    # Change the working directory back to the original directory
    os.chdir("..")

    # subprocess.run(["rm", "-rf", "logs/*"])

    os.chdir(srcDirTest)
    print("------- STAGING PROCESS STARTS HERE -------")

    python_files = glob.glob("0*.py")
    sorted_files = sorted(python_files, key=lambda x: int(os.path.basename(x).split("_")[0]) if "_" in os.path.basename(x) else float("inf"))

    for file in sorted_files:
        execute_python_file(file)

    print("------- STAGING PROCESS ENDS HERE -------")
