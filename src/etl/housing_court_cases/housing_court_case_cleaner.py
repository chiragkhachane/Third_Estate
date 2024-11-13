import pandas as pd

def load_data(input_filepath: str) -> pd.DataFrame:
    """Load dataset from a CSV file."""
    return pd.read_csv(input_filepath)

def keep_columns_up_to_contact(df: pd.DataFrame) -> pd.DataFrame:
    """Keep columns up to 'Contact' and remove all columns to the right."""
    if 'Contact' in df.columns:
        contact_index = df.columns.get_loc("Contact")
        return df.iloc[:, :contact_index + 1]
    return df

def handle_missing_address(df: pd.DataFrame, address_column: str = 'Address') -> pd.DataFrame:
    """Replace empty or NaN values in address column with 'UNKNOWN'."""
    if address_column in df.columns:
        df[address_column].fillna("UNKNOWN", inplace=True)
    return df

def handle_missing_contact(df: pd.DataFrame, contact_column: str = 'Contact') -> pd.DataFrame:
    """Replace empty or NaN values in contact column with 'UNKNOWN'."""
    if contact_column in df.columns:
        df[contact_column].fillna("UNKNOWN", inplace=True)
    return df

def handle_resolution_column(df: pd.DataFrame) -> pd.DataFrame:
    """Replace empty or NaN values in 'Resolution' column with 'UNKNOWN'."""
    if 'Resolution' in df.columns:
        df['Resolution'].fillna("UNKNOWN", inplace=True)
    return df

def handle_date_column(df: pd.DataFrame, column_name: str, filler_date: str = "9999-12-31") -> pd.DataFrame:
    """Convert a date column to date-only format and fill NaT values with filler_date."""
    if column_name in df.columns:
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce').dt.strftime("%Y-%m-%d")
        df[column_name].fillna(filler_date, inplace=True)
    return df

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Replace spaces with underscores in column headers."""
    df.columns = df.columns.str.replace(" ", "_")
    return df

def drop_unwanted_columns(df: pd.DataFrame, columns_to_drop: list = ['City', 'State', 'Zipcode']) -> pd.DataFrame:
    """Drop specified columns if they exist in the dataframe."""
    return df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')

def save_data(df: pd.DataFrame, output_filepath: str) -> None:
    """Save the modified dataframe to a new CSV file."""
    df.to_csv(output_filepath, index=False)
    print(f"Data saved to {output_filepath}")

def process_housing_data(input_filepath: str, output_filepath: str) -> None:
    """
    Main function to process housing court cases data:
    - Keeps columns up to 'Contact'
    - Replaces empty values in 'Resolution' and contact columns with 'UNKNOWN'
    - Standardizes date columns and fills missing dates with '9999-12-31'
    - Renames columns by replacing spaces with underscores
    - Drops unwanted columns
    - Handles missing addresses and contacts
    - Saves the modified data to a new file
    """
    df = load_data(input_filepath)
    df = keep_columns_up_to_contact(df)
    df = handle_missing_address(df)  # Handle missing addresses
    df = handle_missing_contact(df)  # Handle missing contacts
    df = handle_resolution_column(df)
    for date_column in ['Resolution Date', 'Last Action', 'Case Add Date']:
        df = handle_date_column(df, date_column)
    df = rename_columns(df)
    df = drop_unwanted_columns(df)
    save_data(df, output_filepath)

# Example function for use in a main script
def run_housing_data_cleaning():
    input_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/raw/Housing_Court_Cases.csv'  # Replace with actual input file path
    output_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/prod/Housing_Court_Cases.csv'  # Replace with desired output file path
    process_housing_data(input_filepath, output_filepath)
    print("Housing data cleaning completed successfully.")

__all__ = [
    'load_data',
    'keep_columns_up_to_contact',
    'handle_missing_address',  # Added to exports
    'handle_missing_contact',  # Added to exports
    'handle_resolution_column',
    'handle_date_column',
    'rename_columns',
    'drop_unwanted_columns',
    'save_data',
    'process_housing_data',
    'run_housing_data_cleaning'
]
