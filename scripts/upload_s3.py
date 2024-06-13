import os
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Load environment variables from the file outside the scripts folder
env_path = os.path.join(os.path.dirname(__file__), '../env_vars')
load_dotenv(env_path)

# Database connection parameters
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_base = os.getenv('DB_BASE')
db_query = os.getenv('DB_QUERY')

# AWS credentials and S3 bucket details
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
bucket_name = os.getenv('S3_BUCKET_NAME')
s3_file_key = os.getenv('S3_FILE_KEY')
local_file_path = os.path.join(os.path.dirname(__file__), '../data/sales_data.csv')  # Set the desired file name here
temp_dir = os.path.join(os.path.dirname(__file__), '../data/temp_output')  # Temporary directory

# Ensure all required environment variables are set
required_env_vars = {
    'DB_HOST': db_host,
    'DB_PORT': db_port,
    'DB_USER': db_user,
    'DB_PASSWORD': db_password,
    'DB_BASE': db_base,
    'DB_QUERY': db_query,
    'AWS_ACCESS_KEY_ID': aws_access_key_id,
    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key,
    'BUCKET_NAME': bucket_name,
    'S3_FILE_KEY': s3_file_key,
}

missing_vars = [key for key, value in required_env_vars.items() if value is None]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataIngestionApp") \
    .config("spark.executor.extraLibraryPath", "C:\\hadoop\\bin") \
    .config("spark.driver.extraLibraryPath", "C:\\hadoop\\bin") \
    .config("spark.driver.extraClassPath", "C:/mysql-connector-j-8.4.0/mysql-connector-j-8.4.0.jar") \
    .getOrCreate()

# Read data from MySQL database
jdbc_url = f"jdbc:mysql://{db_host}:{db_port}/{db_base}"
properties = {
    "user": db_user,
    "password": db_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

df = spark.read.jdbc(url=jdbc_url, table=f"({db_query}) as query", properties=properties)

# Combine the output into a single CSV file and save it to the temporary directory
df.coalesce(1).write.csv(temp_dir, header=True, mode='overwrite')

# Find the part file in the temporary directory and rename it to the desired file name
for file_name in os.listdir(temp_dir):
    if file_name.startswith('part-') and file_name.endswith('.csv'):
        part_file_path = os.path.join(temp_dir, file_name)
        os.rename(part_file_path, local_file_path)
        break

# Remove the temporary directory
for file_name in os.listdir(temp_dir):
    os.remove(os.path.join(temp_dir, file_name))
os.rmdir(temp_dir)

print(f'Data extracted and saved to {local_file_path}')

# Upload the file to AWS S3 (Optional)
if aws_access_key_id and aws_secret_access_key:
    try:
        # Create a session using the credentials
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        # Create an S3 client
        s3 = session.resource('s3')

        # Upload the file
        print(f'Attempting to upload {local_file_path} to {bucket_name}/{s3_file_key}')
        s3.Bucket(bucket_name).upload_file(local_file_path, s3_file_key)
        print(f'File {local_file_path} uploaded to {bucket_name}/{s3_file_key}')
    except boto3.exceptions.S3UploadFailedError as e:
        print(f'Failed to upload to S3: {e}')
    except Exception as e:
        print(f'An error occurred: {e}')
else:
    print("AWS credentials not found. Skipping S3 upload.")

# Stop Spark session
spark.stop()
