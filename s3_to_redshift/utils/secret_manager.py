import base64
import json

from boto3.session import Session


def get_secret(secret_name):
    region_name = "ap-northeast-1"

    session = Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    if "SecretString" in get_secret_value_response:
        return json.loads(get_secret_value_response["SecretString"])
    else:
        return base64.b64decode(get_secret_value_response["SecretBinary"])