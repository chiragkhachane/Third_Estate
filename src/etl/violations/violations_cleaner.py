import pandas as pd
import re

# Load dataset
def load_data(filepath: str) -> pd.DataFrame:
    """Load dataset from a CSV file, suppressing dtype warnings by setting low_memory=False."""
    return pd.read_csv(filepath, low_memory=False)


# Drop unnecessary columns
def drop_columns(df: pd.DataFrame, columns_to_drop: list) -> pd.DataFrame:
    """Drop specified columns if they exist in the dataframe."""
    return df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')

# Remove specific text in the 'Type' column
def remove_text_from_column(df: pd.DataFrame, column: str, text: str) -> pd.DataFrame:
    """Remove specified text from a column's values."""
    if column in df.columns:
        df[column] = df[column].apply(lambda x: re.sub(text, '', x).strip() if pd.notnull(x) else x)
    return df

# Rename columns
def rename_columns(df: pd.DataFrame, columns_to_rename: dict) -> pd.DataFrame:
    """Rename specified columns."""
    return df.rename(columns=columns_to_rename, inplace=False)

# Filter 'Print Key' column
def filter_print_key(df: pd.DataFrame, column: str = 'Print Key') -> pd.DataFrame:
    """Filter rows where 'Print Key' is null or contains only numeric values."""
    if column in df.columns:
        df[column] = df[column].astype(str)
        df = df[df[column].notna() & ~df[column].str.isnumeric()]
    return df

# Convert date columns to date-only format and fill missing dates
def format_date_columns(df: pd.DataFrame, date_columns: list, filler_date: str = '9999-12-31') -> pd.DataFrame:
    """Convert date columns to 'YYYY-MM-DD' format and fill missing dates with a default value."""
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
            df[col].fillna(filler_date, inplace=True)
    return df

# Replace spaces in column headers with underscores
def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Replace spaces in column headers with underscores."""
    df.columns = df.columns.str.replace(' ', '_')
    return df

# Fill missing values with "UNKNOWN" in all text columns
def fill_missing_text_values(df: pd.DataFrame) -> pd.DataFrame:
    """Replace null values with 'UNKNOWN' in all text columns."""
    text_columns = df.select_dtypes(include='object').columns
    df[text_columns] = df[text_columns].fillna("UNKNOWN")
    return df

# Save cleaned dataset
def save_data(df: pd.DataFrame, output_filepath: str) -> None:
    """Save the modified dataframe to a new CSV file."""
    df.to_csv(output_filepath, index=False)
    print(f"Data saved to {output_filepath}")

# Main cleaning function
def process_housing_data(input_filepath: str, output_filepath: str) -> None:
    """
    Main function to clean the housing violations dataset.
    - Drops unnecessary columns
    - Removes specific text from 'Type' column
    - Renames columns
    - Filters 'Print Key' values
    - Formats date columns and fills missing dates
    - Standardizes column names
    - Fills missing values in text columns with "UNKNOWN"
    """
    columns_to_drop = [
        'City', 'State', 'X Coordinate', 'Y Coordinate', 'Address Number', 'Address Line 1',
        'Address Line 2', 'Zipcode', 'Location', 'Latitude', 'Longitude', 'Council District',
        'Police District', 'Census Tract', 'Census Block Group', 'Census Block', 'Neighborhood'
    ]
    columns_to_rename = {'Property ID': 'Print Key'}
    date_columns = ['Open Date', 'Closed Date']

    # Load data
    df = load_data(input_filepath)
    
    # Clean data
    df = drop_columns(df, columns_to_drop)
    df = remove_text_from_column(df, 'Type', r'\(Req_Serv\)')
    df = rename_columns(df, columns_to_rename)
    df = filter_print_key(df, 'Print Key')
    df = format_date_columns(df, date_columns)
    df = standardize_column_names(df)
    df = fill_missing_text_values(df)

    # Save cleaned data
    save_data(df, output_filepath)


# Example function for use in a main script
def run_housing_data_cleaning():
    input_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/raw/Housing_Violations.csv'  # Replace with your actual input file path
    output_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/prod/Housing_Violations.csv'  # Replace with your desired output file path
    process_housing_data(input_filepath, output_filepath)
    print("Data 311 housing violations cleaning completed successfully.")
