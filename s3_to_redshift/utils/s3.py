import boto3
import boto3.session
from botocore.exceptions import NoCredentialsError, ClientError
import logging
import os
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
# from openpyxl import load_workbook
from io import BytesIO
import pandas as pd
from typing import Union


logging.basicConfig(
    filename='app.log',
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%y-%m-%d %H:%M:%S',
    level=logging.DEBUG
)


class S3Object:
    def __init__(self, region_name, access_key=None, secret_key=None) -> None:
        try:
            self.s3_session = boto3.session.Session(region_name=region_name, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            self.s3_resource = self.s3_session.resource("s3")
            self.s3_client = self.s3_session.client('s3')
            logging.info("S3 client successfully created.")
        except NoCredentialsError:
            logging.error("Credentials not available")
            raise
        except Exception as e:
            logging.error(f"Error: {e}")
            raise

    def get_object_content(self, bucket_name: str, file_key: str, decode_as_text=True, encoding='utf-8', errors='ignore') -> Union[pd.DataFrame, str]:
        try:
            object = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            object_content = object["Body"].read()
            
            if file_key.endswith(".xlsx"):
                object_content = pd.read_excel(BytesIO(object_content))
                logging.info(f"Loaded Excel file '{file_key}' from bucket '{bucket_name}'.")
                return object_content
            
            if decode_as_text:
                object_content = object_content.decode(encoding, errors=errors)
                logging.info(f"Got text object '{file_key}' from bucket '{bucket_name}'.")
            else:
                logging.info(f"Got binary object '{file_key}' from bucket '{bucket_name}'.")
            return object_content
        
        except UnicodeDecodeError as e:
            logging.exception(f"Decoding error for object '{file_key}' from bucket '{bucket_name}': {e}")
            raise
        except ClientError:
            logging.exception(f"Could not get object '{file_key}' from bucket '{bucket_name}'.")
            raise
        
    def retrieve_objects_with_folders(self, bucket_name: str, bucket_prefix: str, file_type=".xlsx"):
        objects = []
        folders = []
        try:
            # paginator = self.s3_client.get_paginator('list_objects_v2')
            # page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=bucket_prefix)
            # for page in page_iterator:
            #     if 'Contents' in page:
            #         for file_name in page['Contents']:
            #             if file_name['Key'].endswith('/'):
            #                 folders.append(file_name['Key'])
            #                 logging.info(f"Got folder '{file_name['Key']}' from bucket '{bucket_name}'.")
            #             if file_name['Key'].endswith(file_type):
            #                 objects.append(file_name['Key'])
            #                 logging.info(f"Got file '{file_name['Key']}' from bucket '{bucket_name}'.")
            #     else:
            #         logging.info(f"There is no file in Bucket {bucket_name} with prefix {bucket_prefix}.")
            s3_bucket = self.s3_resource.Bucket(bucket_name)
            for object in s3_bucket.objects.filter(Prefix=bucket_prefix):
                if object.key.endswith('/'):
                    folders.append(object.key)
                    logging.info(f"Got folder '{object.key}' from bucket '{bucket_name}'.")
                elif object.key.endswith(file_type):
                    objects.append(object.key)
                    logging.info(f"Got file '{object.key}' from bucket '{bucket_name}'.")
                else:
                    logging.info(f"There is no file {file_type} in Bucket {bucket_name} with prefix {bucket_prefix}.")
        except ClientError as e:
            logging.exception(f"Could not list files in bucket '{bucket_name}' under folder '{bucket_prefix}': {e}")
            raise
        finally:
            return (folders, objects)
    
    def download_objects(self, bucket_name: str, local_path: str, objects, folders):
        local_path = Path(local_path)
        try:
            for folder in folders:
                folder_path = Path.joinpath(local_path, folder)
                folder_path.mkdir(parents=True, exist_ok=True)
                logging.info(f"Created folder path '{folder_path}'")
            for object in objects:
                file_path = Path.joinpath(local_path, object)
                file_path.parent.mkdir(parents=True, exist_ok=True)
                self.s3_client.download_file(bucket_name, object, str(file_path))
                logging.info(f"Downloaded '{object}' to '{file_path}'")
        except ClientError as e:
            logging.exception(f"Client error while downloading objects: {e}")
            raise
        except Exception as e:
            logging.exception(f"Unexpected error: {e}")
            raise
                    
    # def download_parallel_multiprocessing():
    #     with ProcessPoolExecutor() as executor:
    #         future_to_key = {executor.submit(download_object, key): key for key in KEYS_TO_DOWNLOAD}

    #         for future in futures.as_completed(future_to_key):
    #             key = future_to_key[future]
    #             exception = future.exception()

    #             if not exception:
    #                 yield key, future.result()
    #             else:
    #                 yield key, exception
                    