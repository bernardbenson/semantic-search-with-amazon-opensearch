import json
import boto3
from os import environ
from opensearchpy import OpenSearch, RequestsHttpConnection

def get_awsauth_from_secret(region, secret_id):
    """
    Retrieves AWS OpenSearch credentials stored in AWS Secrets Manager.
    """
    client = boto3.client('secretsmanager', region_name=region)
    
    try:
        response = client.get_secret_value(SecretId=secret_id)
        secret = json.loads(response['SecretString'])
        
        master_username = secret['username']
        master_password = secret['password']

        return (master_username, master_password)
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

def lambda_handler(event, context):
    # Load environment variables
    region = environ['MY_AWS_REGION']
    aos_host = environ['OS_ENDPOINT']
    os_secret_id = environ['OS_SECRET_ID']
    model_name = environ['MODEL_NAME']

    # Get AWS authentication credentials
    awsauth = get_awsauth_from_secret(region, secret_id=os_secret_id)
    if not awsauth:
        return {
            'Status': 'FAILED',
            'Message': 'Failed to retrieve OpenSearch credentials from Secrets Manager'
        }
    
    # Initialize OpenSearch client
    os_client = OpenSearch(
        hosts=[{'host': aos_host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    

    # The slow log settings to apply
    settings = {
        "settings": {
      "index.search.slowlog.threshold.query.warn": "1ms",
      "index.search.slowlog.threshold.fetch.warn": "1ms",
      "index.search.slowlog.level": "TRACE",
      "index.indexing.slowlog.threshold.index.warn": "1ms"
        }
    }

    try:
        # Apply settings to the specified index
        response = os_client.indices.put_settings(
            index=model_name,
            body=settings
        )
        return {
            'Status': 'SUCCESS',
            'Message': 'Slow logs settings applied successfully',
            'Data': response
        }
    except Exception as e:
        return {
            'Status': 'FAILED',
            'Message': f"Failed to apply slow logs settings: {str(e)}"
        }