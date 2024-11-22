import json
from os import environ

import boto3
from urllib.parse import urlparse

from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
from requests_aws4auth import AWS4Auth

#Global variables for prod 
region = environ['MY_AWS_REGION']
aos_host = environ['OS_ENDPOINT'] 
sagemaker_endpoint = environ['SAGEMAKER_ENDPOINT'] 
os_secret_id = environ['OS_SECRET_ID']
model_name = 'minilm-pretrain-knn'

def get_awsauth_from_secret(region, secret_id):
    """
    Retrieves AWS opensearh credentials stored in AWS Secrets Manager.
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
        
        
def invoke_sagemaker_endpoint(sagemaker_endpoint, payload, region):
    """Invoke a SageMaker endpoint to get embedding with ContentType='text/plain'."""
    runtime_client = boto3.client('runtime.sagemaker', region_name=region)  
    try:
        # Ensure payload is a string, since ContentType is 'text/plain'
        if not isinstance(payload, str):
            payload = str(payload)
        
        response = runtime_client.invoke_endpoint(
            EndpointName=sagemaker_endpoint,
            ContentType='text/plain',
            Body=payload
        )
        
        result = json.loads(response['Body'].read().decode())
        return (result)
    except Exception as e:
        print(f"Error invoking SageMaker endpoint {sagemaker_endpoint}: {e}")
        

def semantic_search_neighbors(features, os_client, k_neighbors=30, idx_name=model_name, filters=None):
    """
    Perform semantic search and get neighbots using the cosine similarity of the vectors 
    output: a list of json, each json contains _id, _score, title, and uuid 
    """
    query={
        "size": k_neighbors,
        "query": {
            "bool": {
                "must": {
                    "knn": {
                        "vector": {
                            "vector": features,
                            "k": k_neighbors
                        }
                    }
                },
                "filter": filters if filters else []  # Apply filters
            }
        }
    }

    print(query)
    
    res = os_client.search(
        request_timeout=55, 
        index=idx_name,
        body=query)
        
    
    # # Return a dataframe of the searched results, including title and uuid 
    # query_result = [
    #     [hit['_id'], hit['_score'], hit['_source']['title'], hit['_source']['id']]
    #     for hit in res['hits']['hits']]
    # query_result_df = pd.DataFrame(data=query_result,columns=["_id","_score","title",'uuid'])
    # return query_result_df

    api_response = create_api_response_geojson(res)
    #api_response = create_api_response(res)
    return api_response 

def text_search_keywords(payload, os_client, k=30,idx_name=model_name):
    """
    Keyword search of the payload string 
    """
    search_body = {
        "size": k,
        "_source": {
            "excludes": ["vector"]
        },
        "highlight": {
            "fields": {
                "description": {}
            }
        },
        "query": {
            "multi_match": {
                "query": payload,
                "fields": ["topicCategory","keywords", "description", "title*", "organisation", "systemName"]
            }
        }
    }
    
    res = os_client.search(
        request_timeout=55, 
        index=idx_name,
        body=search_body)
    
    # query_result = [
    #     [hit['_id'], hit['_score'], hit['_source']['title'], hit['_source']['id']]
    #     for hit in res['hits']['hits']]
    # query_result_df = pd.DataFrame(data=query_result,columns=["_id","_score","title",'uuid'])
    # return query_result_df
    
    api_response = create_api_response_geojson(res)
    return api_response 

def add_to_top_of_dict(original_dict, key, value):
    """
    Adds a new key-value pair to the top of an existing dictionary.
    """
    # Check if the key or value is empty
    if key is None or value is None:
        print("Key and value must both be non-empty.")
        return original_dict  # Optionally handle this case differently
        
    new_dict = {key: value}

    new_dict.update(original_dict)

    return new_dict

def create_api_response(search_results):
    response = {
        "total_hits": len(search_results['hits']['hits']),
        "items": []
    }
    
    for count, hit in enumerate(search_results['hits']['hits'], start=1):
        try:
            source_data = hit['_source'].copy()
            source_data.pop('vector', None)
            source_data = add_to_top_of_dict(source_data, 'relevancy', hit.get('_score', ''))
            source_data = add_to_top_of_dict(source_data, 'row_num', count)
            response["items"].append(source_data)
        except Exception as e:
            print(f"Error processing hit: {e}")
    return response

def create_api_response_geojson(search_results):
    response = {
        "total_hits": len(search_results['hits']['hits']),
        "items": []
    }
    
    for count, hit in enumerate(search_results['hits']['hits'], start=1):
        try:
            source_data = hit['_source'].copy()
            source_data.pop('vector', None)
            source_data = add_to_top_of_dict(source_data, 'relevancy', hit.get('_score', ''))
            source_data = add_to_top_of_dict(source_data, 'row_num', count)
            
            #Get geometry and delete geometry from the source_data
            geometry = source_data.get('coordinates')
            source_data.pop('coordinates')
            #Create the GeoJson object 
            feature_collection = {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": geometry,
                            "properties": source_data
                        }
                    ]
                }
    
            response["items"].append(feature_collection)    
        except Exception as e:
            print(f"Error processing hit: {hit} - {e}")
    return response
    
def lambda_handler(event, context):
    """
    /postText: Uses semantic search to find similar records based on vector similarity.
    Other paths: Uses a direct keyword text match to find matched records .
    """
    awsauth = get_awsauth_from_secret(region, secret_id=os_secret_id)
    os_client = OpenSearch(
        hosts=[{'host': aos_host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    
    k =10
    payload = event['searchString']
    
    # Debug event
    print("event", event)
    
    # Extract filters from the event input
    province_filter = event.get('province', None)
    organization_filter = event.get('org', None)
    metadata_source_filter = event.get('metadata_source', None)

    # Convert filter string into list (handle multi-selection of filters) 
    organization_list = [org.strip() for org in organization_filter.split(",")]
    
    filters = []
    if province_filter:
        filters.append({"term": {"province.keyword": province_filter}})
    if organization_list:
        filters.append({
            "bool": {
                "should": [
                    {
                        "bool": {
                            "should": [
                                {
                                    "wildcard": {
                                        "contact.organisation.en.keyword": {
                                            "value": f"*{org}*"
                                        }
                                    }
                                },
                                {
                                    "wildcard": {
                                        "contact.organisation.fr.keyword": {
                                            "value": f"*{org}*"
                                        }
                                    }
                                }
                            ],
                            "minimum_should_match": 1
                        }
                    }
                    for org in organization_list
                ],
                "minimum_should_match": 1
            }
        })
    if metadata_source_filter:
        filters.append({"term": {"metadata_source.keyword": metadata_source_filter}})
    
    # If no filters are specified, set filters to None
    filters = filters if filters else None
    
    if event['method'] == 'SemanticSearch':
        print(f'This is payload {payload}')
        
        features = invoke_sagemaker_endpoint(sagemaker_endpoint, payload, region)
        print(sagemaker_endpoint)
        print(f"Features retrieved from SageMaker: {features}")
        
        semantic_search = semantic_search_neighbors(
            features=features,
            os_client=os_client,
            k_neighbors=k,
            idx_name=model_name,
            filters=filters
        )
        
        print(f'Type of the semantic response is {type(json.dumps(semantic_search))}')
        print(json.dumps(semantic_search))
        
        return {
            "method": "SemanticSearch", 
            "response": semantic_search
        }
          
    else:
        search = text_search_keywords(payload, os_client, k,idx_name=model_name)

        return {
            "statusCode": 200,
            "body": json.dumps({"keyword_response": search}),
        }
