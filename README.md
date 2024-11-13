# Project Name: Third_Estate

This repository contains a data pipeline and analytics application designed to process, analyze, and enrich assessment data. The pipeline includes cleaning and merging data from various sources, specifically integrating assessment data with historic district identifiers. The project leverages Snowflake, Python, and various data processing libraries to streamline data management tasks.

## Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [Folder Structure](#folder-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Data Pipeline Workflow](#data-pipeline-workflow)
- [Configuration](#configuration)

## Project Overview

The Third_Estate project serves as a structured, modular pipeline for reading, transforming, and storing assessment data. It focuses on aligning assessment data with historic district markers to facilitate property analysis. The project processes and enriches datasets by matching `Print_Key` values between assessment records and historic district information.

## Features

- **Data Cleaning and Transformation**: Processes raw data and organizes it into cleaned formats.
- **Historic Property Identification**: Integrates assessment data with historic property information, tagging properties as historic when applicable.
- **Snowflake Integration**: Uploads data to Snowflake across three schemas: raw, stage, and prod.
- **Git and GitHub Deployment**: Follows version control standards to ensure streamlined collaboration and project management.
- **Environment Variable Security**: Utilizes a `.env` file for secure storage of sensitive credentials and excludes data files from version control.

## Folder Structure

The repository is organized as follows:

    ```bash
    Third_Estate/
    ├── src/
    │ ├── data/
    │ │ ├── raw/ # Contains unprocessed raw data files
    │ │ ├── stage/ # Contains data post initial cleaning
    │ │ └── prod/ # Final cleaned data, ready for production
    │ ├── etl/ # ETL scripts for data loading and transformation
    │ ├── config/ # Configuration files for database connections
    │ └── main.py # Main script to initiate the data pipeline
    ├── README.md # Project documentation
    └── .env # Environment variables (excluded from version control)
    ```

## Installation

### Prerequisites

- **Python 3.8+**
- **Snowflake account and Snowflake Connector for Python**
- **Git for version control**

### Step-by-Step Setup

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/your-username/Third_Estate.git
   cd Third_Estate
   ```

2. **Set Up Virtual Environment**:
   Set Up Virtual Environment:

   ```bash
   python3 -m venv venv
   source venv/bin/activate # On Windows: venv\Scripts\activate
   ```

3. **Install Required Packages**:
   Install the necessary Python packages, including the Snowflake Connector:

   ```bash
   pip install -r requirements.txt
   ```

   If the requirements.txt file is not needed, directly install the Snowflake Connector and any other dependencies:

   ```bash
   pip install snowflake-connector-python
   ```

   **Configure Environment Variables**:

   Create a .env file in the root directory and populate it with your Snowflake credentials:
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_WAREHOUSE=your_warehouse

**Set Up Git LFS for Large Files (if necessary)**:
git lfs install
Usage

**Data Processing and Enrichment**:
Run the main pipeline script to process data:

    python src/main.py

**Data Upload to Snowflake**:
The script data_upload.py within etl/ handles uploading data to Snowflake’s raw, stage, and prod schemas.

**Configurable Paths**:
Update file paths in the script main.py or use environment variables to specify file locations for different stages of the pipeline.

## Data Pipeline Workflow

**Raw Data Loading**
Loads data from raw files in src/data/raw and performs initial cleaning.

**Data Transformation**
Adds Historic_District_Name to assessment data by matching Print_Key with historic data files. Unmatched records are marked as "UNKNOWN."

**Data Upload**
Data is pushed to Snowflake in three schemas (raw, stage, prod) for efficient storage and access.

## Configuration

**Database Configuration**
The .env file in the root directory should contain the required credentials for Snowflake. Ensure that .env is not tracked by Git to keep your credentials secure.

**Excluding Files from Git**

The .gitignore file includes the following exclusions:

.env file for environment variables
src/data/ folder containing raw and processed data files
