import pandas as pd

def capitalize_headers(data):
    """
    Capitalizes headers and replaces spaces with underscores.

    Parameters:
    - data (pd.DataFrame): Input DataFrame.

    Returns:
    - pd.DataFrame: Updated DataFrame with modified headers.
    """
    data.columns = [col.strip().upper().replace(' ', '_') for col in data.columns]
    return data

def merge_notes_columns(data):
    """
    Merges the bank name, notes, and any additional data into one column named 'BANK_NAME', capitalized and with null values replaced by 'UNKNOWN'.

    Parameters:
    - data (pd.DataFrame): Input DataFrame.

    Returns:
    - pd.DataFrame: Updated DataFrame with only 'BANK_CODE' and 'BANK_NAME'.
    """
    data['BANK_NAME'] = data.iloc[:, 1:].apply(
        lambda x: ' '.join(x.dropna().astype(str)).upper(), axis=1
    )
    data['BANK_NAME'] = data['BANK_NAME'].replace('', 'UNKNOWN')
    data = data[['BANK_CODE', 'BANK_NAME']]
    return data

def load_and_process_data(input_filepath, output_filepath):
    """
    Loads data, processes headers and notes columns, and saves the cleaned data.

    Parameters:
    - input_filepath (str): Path to the input file.
    - output_filepath (str): Path to save the output file.

    Returns:
    - pd.DataFrame: Final cleaned DataFrame saved to the output file.
    """
    # Load the data
    data = pd.read_csv(input_filepath)

    # Process headers
    data = capitalize_headers(data)

    # Merge notes columns
    data = merge_notes_columns(data)

    # Save the cleaned data
    data.to_csv(output_filepath, index=False)
    print(f"Data cleaned and saved to {output_filepath}")

    return data


# Example function for use in a main script
def run_bank_data_cleaning():
    input_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/raw/Bank_Code_Identifier.csv'  # Replace with your actual file path
    output_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/prod/Bank_Code_Identifier.csv'  # Replace with your desired output file path
    load_and_process_data(input_filepath, output_filepath)
    print("Bank Codes Data cleaning completed!")