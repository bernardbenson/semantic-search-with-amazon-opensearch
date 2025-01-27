import os
import re
import json
import boto3
from datetime import datetime
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

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

def create_opensearch_index(os_client, index_name):
    """Create a new OpenSearch index if it doesn't exist."""
    if not os_client.indices.exists(index=index_name):
        # Define the mapping for the new index
        index_body = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "filters_applied": {"type": "nested"},
                    "search_string": {"type": "keyword"}
                }
            }
        }
        response = os_client.indices.create(index=index_name, body=index_body)
        print(f"Created new OpenSearch index: {index_name}")
        return response
    else:
        print(f"Index '{index_name}' already exists.")
        return None

def fetch_log_streams(log_group_name):
    """
    Fetch the log streams from the CloudWatch log group
    """
    logs_client = boto3.client('logs')
    response = logs_client.describe_log_streams(logGroupName=log_group_name, orderBy='LastEventTime', descending=True)
    log_streams = response.get('logStreams', [])
    return [stream['logStreamName'] for stream in log_streams]

def fetch_log_events(log_group_name, log_stream_name):
    """
    Fetch the log events from the log stream
    """
    logs_client = boto3.client('logs')
    response = logs_client.get_log_events(logGroupName=log_group_name, logStreamName=log_stream_name, limit=10000)
    events = response.get('events', [])
    return events

def transform_logs(events):
    """
    Transforms raw log events to include only the required fields
    """
    transformed_logs = []
    for event in events:
        print("event: ", event)

        # Extract timestamp
        timestamp = event.get("timestamp")
        print("timestamp ", timestamp)

        # Extract raw message
        raw_message = event.get("message")

        # Use regex to extract the `source` JSON
        source_pattern = r'source\[(\{.*\})\]'
        source_match = re.search(source_pattern, raw_message)

        filters = None
        _name = None

        if source_match:
            source_json = source_match.group(1)
            try:
                # Parse the JSON fragment
                source_dict = json.loads(source_json)

                # Extract `_name` from `must` clauses
                must_clauses = source_dict.get("query", {}).get("bool", {}).get("must", [])
                for clause in must_clauses:
                    if "match_all" in clause and "_name" in clause["match_all"]:
                        _name = clause["match_all"]["_name"]
                        break

                # Extract `filters` from the JSON structure
                filters = source_dict.get("query", {}).get("bool", {}).get("must", [])
            except json.JSONDecodeError as e:
                print("Error decoding JSON: ", e)
        else:
            print("No valid source JSON found in the message")

        # Print extracted values for debugging
        print("filters ", filters)
        print("_name ", _name)

        # Append transformed log
        transformed_logs.append({
            'timestamp': datetime.utcfromtimestamp(timestamp / 1000).isoformat(),
            'filters_applied': filters, 
            'search_string': _name
        })
    
    return transformed_logs

def save_to_opensearch(os_client, index, documents):
    """
    Loads the transformed log data into OpenSearch.
    """
    for doc in documents:
        response = os_client.index(index=index, body=doc)
        print(f"Document saved to OpenSearch: {response['_id']}")

def lambda_handler(event, context):
    """
    Lambda function handler to process logs from a CloudWatch log group and index them into OpenSearch.
    """
    # Environment variable configuration
    s3_bucket = os.environ['S3_BUCKET']
    region = os.environ['MY_AWS_REGION']
    aos_host = os.environ['OS_ENDPOINT']
    new_index_name = os.environ['NEW_INDEX_NAME']
    os_secret_id = os.environ['OS_SECRET_ID']
    log_group_name = os.environ['CLOUDWATCH_LOG_GROUP'] 

    # Get OpenSearch credentials from AWS Secrets Manager
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

    try:
        # Ensure the new OpenSearch index exists
        create_opensearch_index(os_client, new_index_name)

        # Fetch log streams from the CloudWatch log group
        log_streams = fetch_log_streams(log_group_name)
        #print(log_streams)

        excluded_streams = {'es-test-log-stream', 'cust-test-log-stream'} #created automatically by OpenSearch
        for log_stream_name in log_streams:
            if log_stream_name not in excluded_streams:
                # Fetch log events from each log stream
                #print(log_stream_name)
                events = fetch_log_events(log_group_name, log_stream_name)
                #print(events)
                
                # Transform logs for OpenSearch indexing
                transformed_logs = transform_logs(events)
                
                # Save transformed logs to OpenSearch
                save_to_opensearch(os_client, new_index_name, transformed_logs)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Logs successfully processed and indexed into OpenSearch."})
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }