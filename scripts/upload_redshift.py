import os
import boto3
import psycopg2
import logging
from psycopg2 import OperationalError
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from the file outside the scripts folder
env_path = os.path.join(os.path.dirname(__file__), '../env_vars')
load_dotenv(env_path)

# AWS credentials and S3 bucket details
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
bucket_name = os.getenv('S3_BUCKET_NAME')
s3_data_prefix = os.getenv('S3_DATA_PREFIX')

# Redshift database connection parameters
redshift_host = os.getenv('REDSHIFT_HOST')
redshift_port = os.getenv('REDSHIFT_PORT')
redshift_user = os.getenv('REDSHIFT_USER')
redshift_password = os.getenv('REDSHIFT_PASSWORD')
redshift_dbname = os.getenv('REDSHIFT_DATABASE')
redshift_table = os.getenv('REDSHIFT_TABLE')
file_metadata_table = os.getenv('FILE_METADATA_TABLE')

# IAM role for Redshift COPY command
iam_role = os.getenv('IAM_ROLE')

# Ensure all environment variables are set
required_vars = [
    'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET_NAME', 'S3_DATA_PREFIX',
    'REDSHIFT_HOST', 'REDSHIFT_PORT', 'REDSHIFT_USER', 'REDSHIFT_PASSWORD',
    'REDSHIFT_DATABASE', 'REDSHIFT_TABLE', 'FILE_METADATA_TABLE', 'IAM_ROLE'
]

for var in required_vars:
    if not os.getenv(var):
        logging.error(f"Environment variable {var} is not set.")
        raise EnvironmentError(f"Environment variable {var} is not set.")

# Initialize the S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

def connect_to_redshift():
    try:
        conn = psycopg2.connect(
            dbname=redshift_dbname,
            user=redshift_user,
            password=redshift_password,
            host=redshift_host,
            port=redshift_port
        )
        return conn
    except OperationalError as e:
        logging.error(f"OperationalError: {e}")
        logging.info("Please check the following:")
        logging.info("1. Redshift cluster status.")
        logging.info("2. Network settings (VPC, subnets, security groups).")
        logging.info("3. Public accessibility settings.")
        logging.info("4. Firewall and network ACLs.")
        raise

# Attempt to connect to Redshift
try:
    conn = connect_to_redshift()
    cur = conn.cursor()

    try:
        # Create the metadata table if it doesn't exist
        create_metadata_table_query = f"""
        CREATE TABLE IF NOT EXISTS {file_metadata_table} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            file_name VARCHAR(255) NOT NULL,
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(create_metadata_table_query)
        conn.commit()

        # List objects in S3 bucket
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_data_prefix)

        # Iterate over each object and load into Redshift
        for obj in response.get('Contents', []):
            file_name = obj['Key']
            if file_name.endswith('.csv'):  # adjust file extension check as needed
                # Load data into Redshift
                copy_query = f"""
                COPY {redshift_table}
                FROM 's3://{bucket_name}/{file_name}'
                IAM_ROLE '{iam_role}'
                CSV
                IGNOREHEADER 1;
                """
                cur.execute(copy_query)
                conn.commit()

                logging.info(f"Successfully loaded data from {file_name} into {redshift_table}")

                # Insert into file_metadata table
                insert_metadata_query = f"""
                INSERT INTO {file_metadata_table} (file_name)
                VALUES (%s);
                """
                cur.execute(insert_metadata_query, (file_name,))
                conn.commit()
                logging.info(f"Successfully inserted metadata for {file_name} into {file_metadata_table}")

                # Update the ORDERDATE format in Redshift
                update_date_query = f"""
                UPDATE {redshift_table}
                SET ORDERDATE = TO_CHAR(TO_DATE(ORDERDATE, 'MM/DD/YYYY'), 'YYYY-MM-DD HH24:MI:SS')
                WHERE ORDERDATE IS NOT NULL AND ORDERDATE != '';
                """
                cur.execute(update_date_query)
                conn.commit()

            else:
                logging.info(f"Skipped {file_name} as it is not a CSV file")

    except (Exception, psycopg2.DatabaseError) as e:
        logging.error(f"Error: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()

except OperationalError as e:
    logging.error(f"OperationalError: {e}")
    logging.error("Failed to connect to Redshift cluster.")
