import os
import pandas as pd

# Define the path to the dataset folder
dataset_dir = 'dataset'

# Check if the directory exists
if not os.path.exists(dataset_dir):
    print(f"Directory '{dataset_dir}' not found.")
else:
    # Iterate through all files in the directory
    for filename in os.listdir(dataset_dir):
        if filename.endswith('.csv'):
            # Extract table name from the filename
            table_name = os.path.splitext(filename)[0]
            file_path = os.path.join(dataset_dir, filename)
            
            print("========================================")
            print(f"Table Name: {table_name}")
            print("========================================")
            
            try:
                # Read the first 10 rows of the CSV file
                df = pd.read_csv(file_path, nrows=10)
                print(df)
            except Exception as e:
                print(f"Error reading {filename}: {e}")
            
            print("\n")
