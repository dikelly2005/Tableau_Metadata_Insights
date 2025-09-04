import os
import subprocess
import sys

# Full path to the folder containing the Python scripts
SCRIPT_DIR = 'Your-python-repository' # Replace with where your Tableau REST python scripts are located

# Get the name of this script (so we can skip it)
CURRENT_SCRIPT = os.path.basename(__file__)

def main():
    # List all Python files in the folder
    python_scripts = [
        f for f in os.listdir(SCRIPT_DIR)
        if f.endswith(".py") and f != CURRENT_SCRIPT
    ]

    # Sort scripts alphabetically (optional, remove if you want random or custom order)
    python_scripts.sort()

    for script in python_scripts:
        script_path = os.path.join(SCRIPT_DIR, script)
        print(f"\n➡️ Running: {script}")
        result = subprocess.run(["python", script_path])
        
        if result.returncode != 0:
            print(f"❌ Script {script} failed with exit code {result.returncode}")
            break  # Optional: stop on failure
        else:
            print(f"✅ Completed: {script}")

if __name__ == "__main__":
    main()
