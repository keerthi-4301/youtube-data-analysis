import awswrangler as wr
import pandas as pd
import urllib.parse
import os

# Read environment variables
os_s3_path_to_store_transformed_data = os.environ['path_to_store_transformed_data_s3_bucket']
os_glue_database = os.environ['glue_catalog_db_name']
os_glue_table = os.environ['glue_catalog_table_name']
os_write_data_operation = os.environ['write_data_operation']

def lambda_handler(event, context):
    # Log the incoming event for debugging purposes
    print("Received event:", event)

    # Extract bucket and key from the S3 event trigger
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

        # Log the bucket and file key being processed
        print(f"Processing file: {key} from bucket: {bucket}")

        # Load the JSON data from S3 into a dataframe
        df_raw = wr.s3.read_json(f's3://{bucket}/{key}')
        print(f"Data loaded, shape: {df_raw.shape}")

        # Extract the required columns using json_normalize
        df_step_1 = pd.json_normalize(df_raw['items'])
        print(f"Transformed data, shape: {df_step_1.shape}")

        # Write the transformed dataframe back to S3 as Parquet
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_s3_path_to_store_transformed_data,
            dataset=True,
            #database=os_glue_database,
            #table=os_glue_table,
            mode=os_write_data_operation
        )

        # Log the response and return a success message
        print(f"Write response: {wr_response}")
        return {
            'statusCode': 200,
            'body': f"Successfully processed file {key} from bucket {bucket}."
        }

    except Exception as e:
        # Log the exception and error details
        print(f"Error: {e}")
        print(f"Error getting object {key} from bucket {bucket}. Make sure the object exists and the Lambda is in the correct region.")
        
        # Return error response
        return {
            'statusCode': 500,
            'body': f"Failed to process file {key} from bucket {bucket}. Error: {str(e)}"
        }
