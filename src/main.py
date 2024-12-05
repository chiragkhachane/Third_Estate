from etl.assessment import csv_header_transformer, historic_setter, null_replacer, historic_district_name_setter
from etl.code_violations import  code_violations_cleaner
from etl.violations import violations_cleaner
from etl.housing_court_cases import housing_court_case_cleaner
from etl.local_assessment import local_assessmnet_cleaner
from etl.data_upload import data_upload
from etl.bank import bank_cleaner


def run_assessment_cleaner():
    csv_header_transformer.process_csv_headers()
    null_replacer.run_csv_data_cleaning()
    historic_setter.run_assessment_data_cleaning()
    historic_district_name_setter.run_assessment_data_cleaning()

def run_code_violations_cleaner():
    code_violations_cleaner.run_code_violations_cleaning()

def run_violations_cleaner():
    violations_cleaner.run_housing_data_cleaning()

def run_housing_court_case_cleaner():
    housing_court_case_cleaner.run_housing_data_cleaning()

def run_local_assessment_cleaner():
    local_assessmnet_cleaner.run_local_assessment_cleaning()

def run_bank_data_cleaning():
    bank_cleaner.run_bank_data_cleaning()

def run_data_upload_snowflake():
    data_upload.run_data_upload_pipeline()

def main():
    """
    Main function to process a CSV file in a data pipeline:
    1. Reads input file path and output file path from a configuration or predefined setup.
    2. Transforms headers according to specified rules.
    3. Outputs the final processed file.
    """
    
    # Step 1: Transform and clean Assessment data
    run_assessment_cleaner()
    
    # Step 2: Clean Code Violations data
    run_code_violations_cleaner()

    # Step 3: Clean Violations data
    run_violations_cleaner()

    # Step 4: Clean Housing Court Cases data
    run_housing_court_case_cleaner()

    # Step 5: Clean Local_Assessment data
    run_local_assessment_cleaner()
    
    # Step 6: Clean Bank Data
    run_bank_data_cleaning()
    
    print("Data cleaning and transformation 100'%' complete.")

    # Step 7: Upload cleaned data to Snowflake
    print("Startig data upload to Snowflake...")
    run_data_upload_snowflake()


if __name__ == "__main__":
    main()