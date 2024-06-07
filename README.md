# Data Ingestion Project

This project demonstrates how to ingest sales data from a CSV file into an Amazon Redshift database using PySpark and SQLAlchemy.

## Project Structure

1. **Setup AWS Redshift:**
    - Create a Redshift cluster on AWS.
    - Set up IAM roles and permissions.
    - Configure security groups to allow access to the Redshift cluster.
    - Note down the Redshift cluster endpoint, database name, username, and password.

2. **Prepare the CSV Data:**
    - Download the sales data CSV from Kaggle.
    - Upload the CSV file to an S3 bucket.

3. **PySpark Code for Data Ingestion:**
    - The provided `main.py` script reads the CSV file from S3 and ingests it into the Redshift database.

## Setup Instructions

1. **Install necessary packages:**
    ```sh
    pip install pyspark boto3 pandas sqlalchemy
    ```

2. **Run the Python Script:**
    - Update the script with your AWS credentials, S3 bucket name, Redshift endpoint, database details, and CSV file details.
    - Execute the script:
    ```sh
    python main.py
    ```

## Usage

- The script reads the CSV data from the specified S3 bucket.
- Data is loaded into the specified table in the Amazon Redshift database.
- Ensure you have the necessary permissions to access the S3 bucket and the Redshift cluster.

## Project Files

- `main.py`: Python script for data ingestion.
- `README.md`: Project documentation.


