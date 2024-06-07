import boto3
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from env_vars file into current environment
load_dotenv('env_vars')  # Specify the name of your env file here

# Access AWS credentials and region
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')

# Access S3 bucket and Redshift configuration
bucket_name = os.getenv('S3_BUCKET_NAME')
s3_data_prefix = os.getenv('S3_DATA_PREFIX')
redshift_database = os.getenv('REDSHIFT_DATABASE')
redshift_user = os.getenv('REDSHIFT_USER')
redshift_password = os.getenv('REDSHIFT_PASSWORD')
redshift_host = os.getenv('REDSHIFT_HOST')
redshift_port = os.getenv('REDSHIFT_PORT')
redshift_table = os.getenv('REDSHIFT_TABLE')
file_metadata_table = os.getenv('FILE_METADATA_TABLE')

# Use the variables in your application
print(f"AWS region: {aws_region}")
print(f"Connecting to Redshift cluster at {redshift_host}:{redshift_port}...")

# Connect to S3
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  region_name=aws_region)

# Connect to Redshift
conn = psycopg2.connect(
    dbname=redshift_database,
    user=redshift_user,
    password=redshift_password,
    host=redshift_host,
    port=redshift_port
)
cur = conn.cursor()

try:
    # Create the metadata table if it doesn't exist
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {file_metadata_table} (
        id SERIAL PRIMARY KEY,
        file_name VARCHAR(255) NOT NULL,
        load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
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
            IAM_ROLE 'arn:aws:iam::851725590066:role/CapstoneRedshiftS3ReadOnlyRole'
            CSV;
            """
            cur.execute(copy_query)

            # Insert into file_metadata table
            cur.execute(f"""
            INSERT INTO {file_metadata_table} (file_name)
            VALUES ('{file_name}');
            """)

            conn.commit()
            print(f"Successfully loaded data from {file_name} into {redshift_table}")
        else:
            print(f"Skipped {file_name} as it is not a CSV file")

except Exception as e:
    print(f"Error: {e}")
    conn.rollback()

finally:
    cur.close()
    conn.close()
