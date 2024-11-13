import pandas as pd

def add_historic_property_column(assessment_df, parcel_df):
    """
    Adds a 'Historic_Property' column to the assessment DataFrame, marking properties as historic if they exist in the parcel DataFrame.

    Parameters:
    - assessment_df (pd.DataFrame): DataFrame containing assessment data.
    - parcel_df (pd.DataFrame): DataFrame containing parcel data.

    Returns:
    - pd.DataFrame: Updated assessment DataFrame with 'Historic_Property' column.
    """
    assessment_df['Historic_Property'] = assessment_df['Print_Key'].isin(parcel_df['PRINT_KEY']).astype(int)
    return assessment_df

def filter_historic_properties(assessment_df):
    """
    Filters the assessment DataFrame to only include rows where 'Historic_Property' is 1.

    Parameters:
    - assessment_df (pd.DataFrame): DataFrame containing assessment data with 'Historic_Property' column.

    Returns:
    - pd.DataFrame: Filtered DataFrame containing only historic properties.
    """
    return assessment_df[assessment_df['Historic_Property'] == 1]

def load_and_process_assessment_data(assessment_filepath, parcel_filepath, output_filepath):
    """
    Loads, processes, and saves assessment data with historic property information.

    Parameters:
    - assessment_filepath (str): Path to the assessment CSV file.
    - parcel_filepath (str): Path to the parcel CSV file.
    - output_filepath (str): Path where the updated assessment CSV will be saved.

    Returns:
    - pd.DataFrame: Final DataFrame with 'Historic_Property' column added, saved to output file.
    """
    # Load data
    assessment_df = pd.read_csv(assessment_filepath, low_memory=False)
    parcel_df = pd.read_csv(parcel_filepath, low_memory=False)

    # Add historic property information
    assessment_df = add_historic_property_column(assessment_df, parcel_df)

    # Optionally, print or log a sample if required in the main file
    print("Historic Properties Sample:")
    print(filter_historic_properties(assessment_df).head())

    # Save updated assessment data
    assessment_df.to_csv(output_filepath, index=False)
    print("Updated assessment data saved to:", output_filepath)

    return assessment_df

# Example function for use in a main script
def run_assessment_data_cleaning():
    assessment_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/stage/Assessment_cleaned.csv'  # Replace with your actual assessment file path
    parcel_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/raw/All_Historic_Parcels.csv'  # Replace with your actual parcel file path
    output_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/stage/Assessment_is_Historic.csv'  # Replace with your desired output file path
    load_and_process_assessment_data(assessment_filepath, parcel_filepath, output_filepath)
    print("Final Assessment exported!")
