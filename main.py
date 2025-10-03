import subprocess
import sys
import os

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def check_and_install_dependencies():
    try:
        import pyodbc
    except ImportError:
        print("pyodbc not found. Installing...")
        install("pyodbc")

    try:
        import msaccessdb
    except ImportError:
        print("msaccessdb not found. Installing...")
        install("msaccessdb")

    try:
        import tkinter
    except ImportError:
        print("tkinter not found. Please install it manually.")
        sys.exit(1)

def main():
    check_and_install_dependencies()
    
    # Get the absolute path to the script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the absolute path to conversor_cuiles.py
    conversor_path = os.path.join(script_dir, "conversor_cuiles.py")
    
    # Execute the conversor_cuiles.py script
    subprocess.call([sys.executable, conversor_path])

if __name__ == "__main__":
    main()