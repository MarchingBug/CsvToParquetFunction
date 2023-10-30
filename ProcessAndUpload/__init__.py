import logging
import azure.functions as func
import azure.storage.blob
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas,generate_container_sas,ContainerClient
from azure.storage.blob import ResourceTypes, AccountSasPermissions, generate_account_sas
import pandas as pd
import os   
import json
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import base64
from io import StringIO
import pandas as pd

AZURE_ACC_NAME = os.environ['StorageAccountName']
AZURE_PRIMARY_KEY = os.environ['StorageAccountKey']
STORAGE_ACCOUNT_CONTAINER = os.environ['StorageAccountContainer']

storage_account_connection_string = "DefaultEndpointsProtocol=https;AccountName="+AZURE_ACC_NAME+";AccountKey="+AZURE_PRIMARY_KEY+";EndpointSuffix=core.windows.net"


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
   
    req_body = req.get_json()      
    #From request body:
    fileName = req_body.get("fileName")
    fileContent = req_body.get("fileContent")
    blobContainer = req_body.get("container") 
    
    try:
        logging.info('Got Request parameters')
        connection_string = storage_account_connection_string    
        
        blobContainer = blobContainer.lower()
        parquet_container_name = blobContainer + 'parquet'
        
        parquet_file_name = fileName.replace('.csv','.parquet')     
        
        logging.info('parquet file name' + parquet_file_name)
        
        bytes = base64.b64decode(fileContent)
        data = StringIO(bytes.decode('utf-8'))
        df = pd.read_csv(data)               
        
        parquet_file = BytesIO()
        df.to_parquet(parquet_file, engine='pyarrow')   
        parquet_file.seek(0)
        
        logging.info('saving parquet file' + parquet_file_name)
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=parquet_container_name, blob=parquet_file_name)
        
        blob_client.upload_blob(
            data=parquet_file,overwrite=True
            )
        
        return func.HttpResponse(
                f"File {parquet_file_name} in container {parquet_container_name} saved",
                status_code=200
            )
    except Exception as ex:
        return func.HttpResponse(
                f"Error processing function ProcessAndUpload {ex}",
                status_code=400
            )
    

   