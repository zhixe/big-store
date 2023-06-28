import glob
import subprocess
import os

def execute_python_file(file_path):
    # WINDOWS
    # venv = r"C:\Users\datamicron\Documents\vscode\house_pricing\prj_venv\Scripts\activate"

    # WSL
    # venv = "/mnt/c/Users/datamicron/Documents/vscode/house_pricing/prj_venv/Scripts/activate"

    # """Activate Python virtual environment."""
    # subprocess.run(["dos2unix", venv], check=True)
    
    """Execute a Python file."""
    subprocess.run(["python3", file_path], check=True)

if __name__ == "__main__":
    # Specify the directory path where the Python files are located
    # WINDOWS
    # srcDir = r"C:\Users\datamicron\Documents\vscode\house_pricing\src"
    # workDir = r"C:\Users\datamicron\Documents\vscode\house_pricing" 

    # WSL
    srcDir = r"/mnt/c/Users/datamicron/Documents/vscode/house_pricing/src"
    workDir = r"/mnt/c/Users/datamicron/Documents/vscode/house_pricing/"

    os.chdir(workDir)
    subprocess.run(["rm", "-rf", "logs/*"])

    os.chdir(srcDir)
    print("------- STAGING PROCESS STARTS HERE -------")

    python_files = glob.glob("0*.py")
    sorted_files = sorted(python_files, key=lambda x: int(os.path.basename(x).split("_")[0]) if "_" in os.path.basename(x) else float("inf"))

    for file in sorted_files:
        execute_python_file(file)

    print("------- STAGING PROCESS ENDS HERE -------")
