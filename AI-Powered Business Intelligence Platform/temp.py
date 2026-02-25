import pandas as pd
import os


def show_df_info():


    # Path to the dataset directory
    dataset_path = r'd:\Project\Data Analysis Portfolio Project\AI-Powered Business Intelligence Platform\dataset'

    # Dictionary to store DataFrames
    dataframes = {}
    metadata = {}

    # Iterate over all files in the dataset directory
    for file in os.listdir(dataset_path):
        if file.endswith('.csv'):
            # Get file name without extension
            df_name = os.path.splitext(file)[0]
            file_path = os.path.join(dataset_path, file)
            
            # Read the CSV file
            try:
                df = pd.read_csv(file_path)
                # Store in globals to satisfy "df name as file name" request
                globals()[df_name] = df
                dataframes[df_name] = df
                
                # Extract column list with types
                col_info = df.dtypes.to_dict()
                metadata[df_name] = [{"column": col, "type": str(dtype)} for col, dtype in col_info.items()]
                
                print(f"Successfully loaded: {df_name}")
            except Exception as e:
                print(f"Error loading {file}: {e}")

    # Display the metadata (columns and types)
    print("\n--- Dataset Metadata (Column Names and Types) ---")
    for df_name, cols in metadata.items():
        print(f"\nDataFrame: {df_name}")
        for col in cols:
            print(f"  - {col['column']}: {col['type']}")



show_df_info()