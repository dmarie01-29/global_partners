import json
import sys
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

# --- SECRETS MANAGER CONFIGURATION ---
def get_database_credentials():
    # 1. UPDATE OR VERIFY THESE TWO VALUES IF RUNNING IN A DIFFERENT REGION
    secret_name = "sqlworkbench!c78d7d47-1c5e-4086-8d3f-23c4aa6617ee"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Error retrieving secret from Secrets Manager: {e}")
        raise e

    # Parse and extract the key-value secrets dictionary
    return json.loads(get_secret_value_response['SecretString'])


# Fetch credentials securely from AWS before launching Spark
db_credentials = get_database_credentials()

# 2. UPDATE OR VERIFY THESE KEYS MATCH YOUR SECRETS MANAGER KEY LABELS
# (Standard AWS RDS secrets map exactly to 'username' and 'password' keys)
# Force the user string to 'admin' instead of pulling 'glue_user' from the secret
db_user = "admin"  
db_password = db_credentials['password']


# Initialize a clean standard Spark Session
spark = SparkSession.builder \
    .appName("RDS_to_S3_Migration") \
    .getOrCreate()

# 3. VERIFY YOUR SERVER ADDRESS AND DATA LAKE BUCKET TARGETS
server_address = "globalpartnersdb.ckdakm62stml.us-east-1.rds.amazonaws.com"
database_name = "GLOBALPARTNERS_RAW"
s3_output_path = "s3://globalpartners-bucket/raw/"

# Build compliant JDBC URL string
jdbc_url = f"jdbc:mysql://{server_address}:3306/{database_name}?useSSL=false&allowPublicKeyRetrieval=true"

# Connection properties utilizing the MySQL JDBC driver
connection_properties = {
    "url": jdbc_url,
    "user": db_user,
    "password": db_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

# List of tables to extract from RDS and write to S3
tables_to_migrate = ["date_dim_raw", "order_items_raw", "order_item_options_raw"]

for table_name in tables_to_migrate:
    print(f"Extracting table {table_name} from RDS...")
    
    # Read table data into a Spark DataFrame using JDBC
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
    
    # Write DataFrame as Parquet format directly into S3
    target_path = f"{s3_output_path}{table_name}/"
    print(f"Writing {table_name} to S3 target: {target_path}")
    
    df.write \
      .mode("overwrite") \
      .format("parquet") \
      .save(target_path)

print("Migration completed successfully.")
