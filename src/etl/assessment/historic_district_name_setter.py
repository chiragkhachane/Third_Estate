import pandas as pd

def add_historic_district_column(assessment_df, historic_keys_df):
    """
    Adds a 'Historic_District_Name' column to the assessment DataFrame by matching 'Print_Key' to the historic keys DataFrame.

    Parameters:
    - assessment_df (pd.DataFrame): DataFrame containing assessment data.
    - historic_keys_df (pd.DataFrame): DataFrame containing historic district keys.

    Returns:
    - pd.DataFrame: Updated assessment DataFrame with 'Historic_District_Name' column.
    """
    # Merge to add Historic_District_Name, with unmatched entries set to 'UNKNOWN'
    assessment_df = assessment_df.merge(
        historic_keys_df[['Print_Key', 'Historic_District_Name']],
        on='Print_Key',
        how='left'
    )
    assessment_df['Historic_District_Name'] = assessment_df['Historic_District_Name'].fillna("UNKNOWN")
    return assessment_df

def load_and_process_assessment_data(assessment_filepath, historic_keys_filepath, output_filepath):
    """
    Loads, processes, and saves assessment data with historic district information.

    Parameters:
    - assessment_filepath (str): Path to the assessment CSV file.
    - historic_keys_filepath (str): Path to the historic district keys CSV file.
    - output_filepath (str): Path where the updated assessment CSV will be saved.

    Returns:
    - pd.DataFrame: Final DataFrame with 'Historic_District_Name' column added, saved to output file.
    """
    # Load data
    print("Loading assessment data...")
    assessment_df = pd.read_csv(assessment_filepath, low_memory=False)
    
    print("Loading historic district keys data...")
    historic_keys_df = pd.read_csv(historic_keys_filepath, low_memory=False)
    
    # Add historic district information
    print("Adding Historic_District_Name column...")
    assessment_df = add_historic_district_column(assessment_df, historic_keys_df)
    
    # Optionally, print or log a sample if required in the main file
    print("Sample with Historic_District_Name:")
    print(assessment_df.head())

    # Save updated assessment data
    print("Saving updated assessment data to:", output_filepath)
    assessment_df.to_csv(output_filepath, index=False)

    return assessment_df

# Example function for use in a main script
def run_assessment_data_cleaning():
    """
    Main function to execute the assessment data cleaning process, 
    matching historic district data and saving the final output.
    """
    # Replace with your actual file paths
    assessment_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/stage/Assessment_is_Historic.csv'
    historic_keys_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/raw/Historic_Districts_Print_Keys.csv'
    output_filepath = '/Users/chiragkhachane/Projects/Third_Estate/src/data/prod/Assessment.csv'

    # Run the processing
    print("Starting the assessment data cleaning process...")
    load_and_process_assessment_data(assessment_filepath, historic_keys_filepath, output_filepath)
    print("Final Assessment data exported with Historic_District_Name!")
