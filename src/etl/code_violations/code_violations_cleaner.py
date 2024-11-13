import pandas as pd
import re
from bs4 import BeautifulSoup
import html

def clean_text(text):
    """
    Cleans text by:
    - Unescaping HTML entities.
    - Stripping HTML tags.
    - Removing special characters.
    - Splitting into sentences and capitalizing each one.
    
    Parameters:
    - text (str): Original text to clean.
    
    Returns:
    - str: Cleaned and formatted text.
    """
    if not isinstance(text, str) or text.strip() == '':
        return "UNKNOWN"
    text = html.unescape(text)
    soup = BeautifulSoup(text, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s.,;()]', '', text)
    
    sentences = re.split(r'(?<=[.;])\s*', text)
    cleaned_sentences = [sentence.capitalize() for sentence in sentences if sentence.strip()]
    return '. '.join(cleaned_sentences)

def handle_null_values(df, columns):
    """
    Checks and replaces NULL values in specified columns with 'UNKNOWN'.
    
    Parameters:
    - df (DataFrame): The DataFrame to modify.
    - columns (list): List of columns to check for NULL values.
    """
    for column in columns:
        null_count = df[column].isnull().sum()
        if null_count > 0:
            df[column] = df[column].fillna('UNKNOWN')
            print(f"Replaced {null_count} NULL values in '{column}' column with 'UNKNOWN'")

def select_columns(df, end_column_name):
    """
    Keeps columns up to and including `end_column_name`.
    
    Parameters:
    - df (DataFrame): The DataFrame to modify.
    - end_column_name (str): The column name up to which columns are kept.
    
    Returns:
    - DataFrame: Modified DataFrame with selected columns.
    """
    columns_to_keep = df.columns[:df.columns.get_loc(end_column_name) + 1].tolist()
    return df[columns_to_keep]

def standardize_column_names(df):
    """
    Replaces spaces in column names with underscores.
    
    Parameters:
    - df (DataFrame): The DataFrame to modify.
    
    Returns:
    - DataFrame: Modified DataFrame with standardized column names.
    """
    df.columns = df.columns.str.replace(' ', '_')
    return df

def drop_column(df, column_name):
    """
    Drops a specified column if it exists.
    
    Parameters:
    - df (DataFrame): The DataFrame to modify.
    - column_name (str): The column to drop.
    """
    if column_name in df.columns:
        df = df.drop(column_name, axis=1)
        print(f"'{column_name}' column has been dropped.")
    return df

def set_column_as_string(df, column_name):
    """
    Ensures a specified column is of string type.
    
    Parameters:
    - df (DataFrame): The DataFrame to modify.
    - column_name (str): The column to convert to string.
    """
    df[column_name] = df[column_name].astype(str)
    return df

def format_date_column(df, column_name):
    """
    Converts a date column to date-only format.
    
    Parameters:
    - df (DataFrame): The DataFrame to modify.
    - column_name (str): The date column to format.
    """
    df[column_name] = pd.to_datetime(df[column_name], errors='coerce').dt.date
    return df

def fill_empty_values(df, column_name, fill_value):
    """
    Fills empty values in a specified column with a specified value.
    
    Parameters:
    - df (DataFrame): The DataFrame to modify.
    - column_name (str): The column to fill empty values in.
    - fill_value (str): The value to use for filling empty entries.
    """
    if column_name in df.columns:
        df[column_name] = df[column_name].fillna(fill_value)
    return df

def process_code_violations(input_filepath, output_filepath):
    """
    Main function to process code violations data by:
    - Handling NULL values in specified columns.
    - Keeping columns up to a specified endpoint.
    - Standardizing column names.
    - Dropping unwanted columns.
    - Ensuring specific column types.
    - Cleaning text in 'Comments' column.
    - Saving cleaned data to a new CSV file.
    
    Parameters:
    - input_filepath (str): Path to the input CSV file.
    - output_filepath (str): Path to the output CSV file.
    """
    df = pd.read_csv(input_filepath, low_memory=False)
    
    handle_null_values(df, ['SBL', 'Address'])
    df = select_columns(df, 'Address')
    df = standardize_column_names(df)
    df = drop_column(df, 'Prop_Class')
    df = set_column_as_string(df, 'SBL')
    df = format_date_column(df, 'Date')
    df = fill_empty_values(df, 'Violation_Location', 'N/A')
    
    if 'Comments' in df.columns:
        df['Comments'] = df['Comments'].apply(clean_text)
    
    df.to_csv(output_filepath, index=False)
    print(f"Cleaning complete. Results saved to '{output_filepath}'")

# Example function for use in a main script
def run_code_violations_cleaning():
    input_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/raw/Code_Violations.csv'
    output_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/prod/Code_Violations.csv'
    process_code_violations(input_filepath, output_filepath)
    print("Code violations data cleaning completed successfully.")
