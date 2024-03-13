import boto3, argparse, os
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from sqlalchemy import create_engine

parser = argparse.ArgumentParser()
parser.add_argument("--awsKeyId",)
parser.add_argument("--awsKeySecret")
parser.add_argument("--rdsPW",)

args = parser.parse_args()

aws_key_id=args.awsKeyId
aws_key_token=args.awsKeySecret

# RDS connection details
database_name = 'database-2-instance-1'
table_name = 'SPACEXDATA'
rds_host = 'database-2-instance-1.cfigaasqi775.us-east-1.rds.amazonaws.com'
rds_port = '3306'
rds_user = 'admin'
rds_password = args.rdsPW

testBucket = 'bear-cog-test-bucket'
s3 = boto3.client('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_key_token )

def create_s3_bucket(s3client: boto3.client, bucketName):
    s3_resp = s3client.list_buckets()
    buckets = [ bucket['Name'] for bucket in s3_resp['Buckets']]

    if bucketName not in buckets:
        print("Bucket not exist, creating bucket")
        s3client.create_bucket(Bucket=bucketName)


    s3_resp = s3client.list_buckets()
    print(s3_resp['Buckets'])
    return 

def upload_full_dir(s3client: boto3.client, path, bucketName):
    
    for root,dirs,files in os.walk(path):
        for file in files:
            fileYr = file.split(".")[1][0:4]
            fileDir = f"/spacex-data/year={fileYr}/"
            outFile = fileDir+file
            print(fileDir)

            # s3client.upload_file(os.path.join(root,file),bucketName,file)
            s3client.upload_file(os.path.join(root,file),bucketName,outFile)
    return 

#create RDS manually in AWS Console
#inspiration for writing parquet to RDS -> https://medium.com/@pranay1001090/how-to-load-data-from-amazon-s3-csv-parquet-to-aws-rds-using-python-3dc51dd2186e
#s3_prefix ~~ "/spacex-data/year="
filePrefix = "/spacex-data/year="
def load_parquet_data(s3client: boto3.client, bucketName, s3_prefix):
    file_objects = s3client.list_objects_v2(Bucket=bucketName, Prefix=s3_prefix)['Contents']
    # print(file_objects)
    dfs = []
    for file_object in file_objects:
        file_key = file_object['Key']
        file_obj = s3client.get_object(Bucket=bucketName, Key=file_key)
        parquet_file = pq.ParquetFile(BytesIO(file_obj['Body'].read()))
        df = parquet_file.read().to_pandas()
        dfs.append(df)
    return pd.concat(dfs)

# def load_data_to_rds():
#     df = load_parquet_data(s3, testBucket, filePrefix)
#     # Connect to RDS
#     conn_str = f'mysql+mysqlconnector://{rds_user}:{rds_password}@{rds_host}:{rds_port}/{database_name}'
#     print(conn_str)
#     engine = create_engine(conn_str)
#     engine.connect()

#     # Write the DataFrame to RDS
#     df.to_sql(table_name, con=engine, if_exists='replace', index=False)

#     # Closing the connection
#     engine.dispose()

#     print('Data loaded successfully!')

"""
    Unable to get connection established to the DB
    Kept 'timing' out the connection, when most likely it is some network traffic rule preventing me from connecting to the instance
    Attempted to add a security group rule to allow inbound traffic from my IP, but still didn't work
    The dataframe generated via load_parquet_data contains all the gathered data from the S3 objects
        Next step would be to just get an active python-ic connection to the RDS and write the results via SQL
    The Schema/structure would be loosely preserved in the RDS because the 'date_utc' field would still exist and could be used to sort / filter the records 
"""



if __name__ == '__main__':
    create_s3_bucket(s3, bucketName=testBucket)
    upload_full_dir(s3, "./parqOutputs/", bucketName=testBucket)
    df = load_parquet_data(s3, testBucket, filePrefix)
    print(df.head(100))
    # load_data_to_rds() <-- unable to get to work due to mysql connector issue(s)

