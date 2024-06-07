import boto3
import psycopg2

# AWS credentials and region
aws_access_key_id = 'AKIA4MTWNDYZCXXIUWOY'
aws_secret_access_key = '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'
aws_region = 'eu-north-1'

# S3 bucket and Redshift cluster configuration
bucket_name = 'capstone-data-ingestion-bucket'
s3_data_prefix = 'data/'  # adjust prefix if your files are in a specific directory
redshift_database = 'capstonesalesdb'
redshift_user = 'mosogbe123'
redshift_password = 'Sogbe123$$$'
redshift_host = 'capstone-project-redshift-cluster.cwo3ts8pt0ig.eu-north-1.redshift.amazonaws.com:5439/dev'
redshift_port = '5439'
redshift_table = 'orders'
file_metadata_table = 'file_metadata'

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
