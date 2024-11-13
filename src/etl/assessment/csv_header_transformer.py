import csv
import re

def transform_header(header):
    """
    Transforms a CSV header by:
    - Ignoring headers in all uppercase.
    - Inserting "_" before capital letters except the first.
    - Removing any "of" and special characters.
    - Replacing multiple underscores with a single one.
    - Converting to title case format (e.g., "Deed_Book").
    
    Parameters:
    - header (str): Original header string.

    Returns:
    - str: Transformed header string.
    """
    if header.isupper():
        return header

    # Insert "_" before capital letters (excluding the first letter), remove "of" and special characters
    transformed = re.sub(r'(?<!^)(?=[A-Z])', '_', header).replace("of", "")
    transformed = re.sub(r'[^a-zA-Z0-9_]', '', transformed)
    transformed = re.sub(r'_+', '_', transformed)  # Replace multiple underscores with one
    return transformed.title()

def modify_and_export_csv_headers(input_filepath, output_filepath):
    """
    Reads headers from a CSV, applies transformation to specified headers, and exports to a new CSV file.
    - Ignores the first column and last four columns for transformation.

    Parameters:
    - input_filepath (str): Path to the input CSV file.
    - output_filepath (str): Path to the output CSV file.
    """
    with open(input_filepath, mode='r', newline='') as file:
        reader = csv.reader(file)
        headers = next(reader)
        data_rows = list(reader)

        # Select headers for modification, ignoring first and last four columns
        headers_to_modify = headers[1:-4]
        transformed_headers = [headers[0]] + [transform_header(header) for header in headers_to_modify] + headers[-4:]

    with open(output_filepath, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(transformed_headers)
        writer.writerows(data_rows)
    print(f"Headers transformed and saved to {output_filepath}")

# Exported function for main file usage
def process_csv_headers():
    """
    Wrapper function to process CSV headers, intended for use in main scripts.
    """
    input_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/raw/Assessment.csv'  # Replace with your actual input file path
    output_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/stage/Assessment_header.csv'  # Replace with your desired output file path
    modify_and_export_csv_headers(input_filepath, output_filepath)
    print("Data Assessment header cleaning completed successfully.")
