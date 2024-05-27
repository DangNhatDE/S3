from utils.s3 import S3Object
from dotenv import dotenv_values


config = dotenv_values(".env.development")
AWS_ACCESS_KEY = config.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = config.get("AWS_SECRET_KEY")
BUCKET_NAME = "gf-dwh-win"
object = S3Object(region_name="ap-southeast-1", access_key=AWS_ACCESS_KEY, secret_key=AWS_SECRET_KEY)
folders, objects = object.retrieve_objects_with_folders(bucket_name=BUCKET_NAME, bucket_prefix="FOOD/IN/FORECAST_ACCURACY")
# print(folder)
# print(objects)
# get_obj = object.get_object_content(bucket_name=BUCKET_NAME, file_key="FOOD/IN/FORECAST_ACCURACY/30_FC_Template_Upload.xlsx")
# print(get_obj)
object.download_objects(bucket_name=BUCKET_NAME, local_path="C:/Users/617727/OneDrive - GREENFEED VN/DATA/", objects=objects, folders=folders)