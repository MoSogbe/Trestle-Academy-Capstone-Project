# Data Ingestion Project

This project demonstrates how to ingest data from a CSV file stored in an S3 bucket into an Amazon Redshift database using `boto3` and `psycopg2`.

## Project Structure

1. **Setup AWS Redshift:**
    - Create a Redshift cluster on AWS.
    - Set up IAM roles and permissions.
    - Configure security groups to allow access to the Redshift cluster.
    - Note down the Redshift cluster endpoint, database name, username, and password.

2. **Prepare the CSV Data:**
    - Upload your CSV files to an S3 bucket.

3. **Python Code for Data Ingestion:**
    - The provided `main.py` script reads the CSV files from S3 and ingests them into the Redshift database.

## Setup Instructions

1. **Clone the repository:**
    ```sh
    git clone https://github.com/MoSogbe/Trestle-Academy-Capstone-Project.git
    cd <repository_name>
    ```

2. **Install necessary packages:**
    ```sh
    pip install boto3 psycopg2
    ```

3. **Configure AWS credentials:**
    Ensure you have your AWS credentials and region configured in your environment or use a credentials file. You can also directly set them in the script.

4. **Modify configuration in `main.py`:**
    Update the following variables in the script:
    ```python
    aws_access_key_id = 'your_aws_access_key_id'
    aws_secret_access_key = 'your_aws_secret_access_key'
    aws_region = 'your_aws_region'
    bucket_name = 'your_s3_bucket_name'
    s3_data_prefix = 'data/'
    redshift_database = 'your_redshift_database_name'
    redshift_user = 'your_redshift_user_name'
    redshift_password = 'your_redshift_password'
    redshift_host = 'your_redshift_cluster_endpoint'
    redshift_port = '5439'
    redshift_table = 'orders'
    file_metadata_table = 'file_metadata'
    ```

5. **Run the Python Script:**
    ```sh
    python data_ingestion.py
    ```

## Usage

- The script reads CSV data from the specified S3 bucket.
- Data is loaded into the specified table in the Amazon Redshift database.
- The `file_metadata` table logs details of the ingested files.

## Project Files

- `main.py`: Python script for data ingestion.
- `README.md`: Project documentation.

## Script Explanation

- **Connect to S3**: The script connects to your S3 bucket using the `boto3` library.
- **Connect to Redshift**: The script connects to your Redshift cluster using the `psycopg2` library.
- **Create Metadata Table**: If the `file_metadata` table does not exist, it is created.
- **List S3 Objects**: The script lists all objects in the specified S3 bucket and prefix.
- **Load Data**: For each CSV file found in the S3 bucket, the script copies its contents to the Redshift table and logs the file's metadata.

## Handling Errors

The script includes basic error handling. In case of an error, it prints the error message and rolls back any changes made during the transaction.

## License

This project is licensed under the MIT License.

## Acknowledgements

Special thanks to the data engineering programme team for their guidance and support.
