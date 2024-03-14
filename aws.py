import boto3, argparse, os
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
import sqlalchemy as sql
from sqlalchemy_utils import database_exists, create_database

#set up a Argument Parser instance to pass my AWS token, RDS DB PW, or any sensitive data
parser = argparse.ArgumentParser()
parser.add_argument("--awsKeyId",)
parser.add_argument("--awsKeySecret")
parser.add_argument("--rdsPW",)

args = parser.parse_args()

aws_key_id=args.awsKeyId
aws_key_token=args.awsKeySecret

# RDS connection details
database_name = 'BEAR_COG_TEST'
table_name = 'SPACEXDATA'
rds_host = 'database-1.cfigaasqi775.us-east-1.rds.amazonaws.com'
rds_port = '3306'
rds_user = 'admin'
rds_password = args.rdsPW

testBucket = 'bear-cog-test-bucket'
s3 = boto3.client('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_key_token ) #create s3 client to connect/manipulate s3 objects

# using s3 client, look for bucket by name, and create new if not exist
def create_s3_bucket(s3client: boto3.client, bucketName):
    s3_resp = s3client.list_buckets()
    buckets = [ bucket['Name'] for bucket in s3_resp['Buckets']]

    if bucketName not in buckets:
        print("Bucket not exist, creating bucket")
        s3client.create_bucket(Bucket=bucketName)

    s3_resp = s3client.list_buckets()
    print(s3_resp['Buckets']) #print out a human readable confirmation of the action
    return 

#based on a looked up function to copy a full dir, sub-dir, and files from local to AWS S3
# create the key/dir in the S3 bucket based on the fileYr var
def upload_full_dir(s3client: boto3.client, path, bucketName):
    for root,dirs,files in os.walk(path):
        for file in files:
            fileYr = file.split(".")[1][0:4]
            fileDir = f"/spacex-data/year={fileYr}/"
            outFile = fileDir+file
            print(fileDir) #print out human readable confirmation that the script is running / where it is at
            s3client.upload_file(os.path.join(root,file),bucketName,outFile)
    return 

#created RDS manually in AWS Console
#inspiration for writing parquet to RDS -> https://medium.com/@pranay1001090/how-to-load-data-from-amazon-s3-csv-parquet-to-aws-rds-using-python-3dc51dd2186e
# having never used python to write to S3 or used parquet files, used this function to:
##  go from list of objects (files) in S3
## create a 'master' pandas df to store the values from the s3 parquet files
##  use a df because it should be easily writeable to SQL/RDS/DB
filePrefix = "/spacex-data/year="
def load_parquet_data(s3client: boto3.client, bucketName, s3_prefix):
    file_objects = s3client.list_objects_v2(Bucket=bucketName, Prefix=s3_prefix)['Contents']
    dfs = []
    for file_object in file_objects:
        file_key = file_object['Key']
        file_obj = s3client.get_object(Bucket=bucketName, Key=file_key)
        parquet_file = pq.ParquetFile(BytesIO(file_obj['Body'].read()))
        print(f"Reading from {parquet_file}")
        df = parquet_file.read().to_pandas()
        dfs.append(df)
    return pd.concat(dfs)

"""
    Unable to get the dataframe.to_sql to load the records into the DB
    Error is assert schema is not None
    Connection to MySQL RDS is successful, can create the DB if it wasn't already created in a previous run
        Writing the records from the DF to the DB is the main issue
        Most likely the root of my problem is stemming from the schema captured in the launch object
            This launch object contains a payload key whose value is a list of objects (and trying to use a parquet viewer always returned an unsupported column)
            The better idea that was stumbled upon was that the individual rocket and payload objects needed to be captured in their own unique lists
                These rocket and payloads would then be written to their own tables so that we can join them when we run any SQL statements instead of having a nested column data type that is not easily query-able (via SQL means, no-sql may be easier)

"""
def load_data_to_rds(s3client: boto3.client, bucketName, s3_prefix ):
    df = load_parquet_data(s3client, bucketName, s3_prefix)
    # Connect to RDS
    conn_str = f'mysql+mysqlconnector://{rds_user}:{rds_password}@{rds_host}:{rds_port}'
    engine = sql.create_engine(conn_str)
    with engine.connect() as db_conn:
        createDB = sql.text(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        create_DB_result = db_conn.execute(createDB)
        print("create_DB_result: ", create_DB_result)

        useDB = sql.text(f"USE {database_name}")
        use_DB_result = db_conn.execute(useDB)
        print("use_DB_result: ", use_DB_result)

        """ My attempt at defining a schema for the SPACEXDATA table, but writing to the table never worked"""
        #based on parquet file structure built in api.py
        # createTbl = sql.text(f"""
        #     CREATE TABLE IF NOT EXISTS {table_name} (
        #         rocket JSON,
        #         payloads JSON, 
        #         launch_id VARCHAR(70) , 
        #         launch_name VARCHAR(70) , 
        #         launch_date_utc VARCHAR(70) , 
        #         launch_date_local DATETIME , 
        #         launch_success BIT , 
        #         launch_window INT , 
        #         launch_failures VARCHAR(255), 
        #         launch_details INT , 
        #         launch_flight_number BIGINT , 
        #         launch_upcoming BIT , 
        #         launch_launch_library_id VARCHAR(70) 
        #         );
        # """)
        # create_Tbl_result = db_conn.execute(createTbl)
        # print("create_Tbl_result: ", create_Tbl_result)
        # drop_Tbl_result = db_conn.execute(sql.text(f"DROP TABLE {table_name}"))
        # print(drop_Tbl_result)

        # Write the DataFrame to RDS
        df.to_sql(name=database_name+"."+table_name, con=engine, if_exists='replace', index=False)

    print('Data loaded successfully!')




#define the workflow of the AWS load procedure
    
"""
    Ultimately this would be defined as its own function or in some way wrapped in a prefect Flow
    The flow would have the tasks:
        - create_s3_bucket
        - upload_full_dir
        - load_data_to_rds
"""
if __name__ == '__main__':
    create_s3_bucket(s3, bucketName=testBucket)
    upload_full_dir(s3, "./parqOutputs/", bucketName=testBucket)

    """
        This would not be called in the working scenario, it is only called here to show that the data is stored on the S3 bucket
            and we would then take this df and write to the RDS DB
    """
    df = load_parquet_data(s3, testBucket, filePrefix) 
    print(df.head(100))
    # load_data_to_rds() <-- unable to get to work due to mysql connector issue(s)

