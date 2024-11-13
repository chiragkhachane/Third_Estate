import pandas as pd

def load_data(filepath: str) -> pd.DataFrame:
    """Load a dataset from a CSV file."""
    return pd.read_csv(filepath, low_memory=False)

def rename_columns(df: pd.DataFrame, columns_map: dict) -> pd.DataFrame:
    """Rename specified columns in the DataFrame."""
    return df.rename(columns=columns_map, inplace=False)

def filter_columns(df: pd.DataFrame, columns_to_keep: list) -> pd.DataFrame:
    """Retain only the specified columns in the DataFrame."""
    return df[columns_to_keep]

def fill_null_values(df: pd.DataFrame, fill_values: dict) -> pd.DataFrame:
    """Fill null or empty values in specified columns with provided defaults."""
    for col, value in fill_values.items():
        if col in df.columns:
            df[col].fillna(value, inplace=True)
    return df

def clean_local_assessment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the local assessment data by renaming, filtering, and filling null values.
    Returns the cleaned DataFrame.
    """
    # Rename columns
    df = rename_columns(df, {'PrintKeyCode': 'PrintKey'})
    
    # Retain only specified columns
    columns_to_keep = ["RollYear", "PrintKey", "Bank", "FullMarketValue", "CountyTaxableValue", "SchoolTaxable"]
    df = filter_columns(df, columns_to_keep)

    # Fill null values
    fill_values = {
        "Bank": "UNKNOWN",
        "FullMarketValue": "-1",
        "CountyTaxableValue": "N/A",
        "SchoolTaxable": "N/A"
    }
    df = fill_null_values(df, fill_values)

    return df

def merge_and_filter_data(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Merge two DataFrames on a specified column and filter by a condition.
    Returns the filtered DataFrame.
    """
    #merged_df = pd.merge(df1, df2, on=merge_column, how='inner')

    merged_df = pd.merge(df2, df1, on='PrintKey', how='left')
    filtered_df = merged_df[merged_df['RollYear'] == 2023]
    return filtered_df

def process_assessment_data(input_filepath: str, stage_filepath: str, output_filepath: str) -> None:
    """
    Main function to process assessment data:
    - Load and clean local assessment data
    - Merge with updated data and filter by RollYear
    - Save the final merged and filtered dataset
    """
    # Load and clean local assessment data
    df = load_data(input_filepath)
    df = clean_local_assessment(df)
    #df.to_csv(updated_filepath, index=False)

    # Load the updated assessment data
    df2 = load_data(stage_filepath)
    df2 = rename_columns(df2, {'Print_Key': 'PrintKey'})

    # Merge and filter by RollYear 2023 (using the cleaned local assessment data and updated data)
    final_df = merge_and_filter_data(df, df2)
    
    # Save the final merged and filtered dataset
    final_df.to_csv(output_filepath, index=False)
    print(f"Data processing completed and saved as '{output_filepath}'.")


def run_local_assessment_cleaning():
    input_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/raw/Local_Assessment.csv'  # Replace with your actual input file path
    stage_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/prod/Assessment.csv'
    output_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/prod/Assessment_with_Local.csv'  # Replace with your desired output file path
    process_assessment_data(input_filepath, stage_filepath, output_filepath)
    print("Local Assessment data cleaning completed successfully.")