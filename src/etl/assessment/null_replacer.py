import pandas as pd

def load_csv(filepath):
    """Load CSV file with low memory mode disabled."""
    return pd.read_csv(filepath, low_memory=False)

def replace_nulls(df):
    """Replace NULLs in string fields with 'UNKNOWN' and in numeric fields with -1."""
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            df[column].fillna(-1, inplace=True)
        elif pd.api.types.is_string_dtype(df[column]):
            df[column].fillna("UNKNOWN", inplace=True)
    return df

def standardize_dates(df, filler_date="9999-12-31"):
    """Standardize date columns to 'YYYY-MM-DD' and replace NULL dates with a default filler date."""
    for column in df.columns:
        if 'date' in column.lower():
            df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime("%Y-%m-%d")
            df[column].fillna(filler_date, inplace=True)
    return df

def save_csv(df, filepath):
    """Save DataFrame to a CSV file."""
    df.to_csv(filepath, index=False)
    print(f"Data cleaning complete. Output saved to {filepath}")

def clean_csv_data(input_filepath, output_filepath):
    """
    Main function to clean CSV data:
    - Replaces NULLs in string fields with 'UNKNOWN' and in numeric fields with -1
    - Standardizes date formats and replaces NULL dates with '9999-12-31'
    - Saves the cleaned data to the specified output file
    """
    # Load data
    df = load_csv(input_filepath)

    # Clean data
    df = replace_nulls(df)
    df = standardize_dates(df)

    # Save cleaned data
    save_csv(df, output_filepath)

# Example function for use in a main script
def run_csv_data_cleaning():
    input_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/stage/Assessment_header.csv'  # Replace with your actual input file path
    output_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/stage/Assessment_cleaned.csv'  # Replace with your desired output file path
    clean_csv_data(input_filepath, output_filepath)
    print("Data Assessment null cleaning completed successfully.")
