import json
import boto3

from os import environ
from datetime import datetime
from urllib.parse import urlparse
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
from requests_aws4auth import AWS4Auth

from filter_builder import *

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
        

def semantic_search_neighbors(features, os_client, sort_param, k_neighbors=30, from_param=0, idx_name=model_name, filters=None, size=10):
    """
    Perform semantic search and get neighbots using the cosine similarity of the vectors 
    output: a list of json, each json contains _id, _score, title, and uuid 
    """
    #print("Filters:", json.dumps(filters, indent=2))
    query = {
        "query": {
            "bool": {
                "must": [],
                "filter": filters if filters else []  # Apply filters
            }
        },
        "size": size,
        "from": from_param,
        "sort": sort_param
    }

    # Include the knn (i.e., vectors) only if features are provided in case of empty keyword query
    if features:
        query["query"]["bool"]["must"].append({
            "knn": {
                "vector": {
                    "vector": features,
                    "k": k_neighbors
                }
            }
        })
    
    print(json.dumps(query, indent=2))
    
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

    total_hits = search_results['hits']['total']['value'] if 'total' in search_results['hits'] else 0
    returned_hits = len(search_results['hits']['hits'])

    response = {

        "total_hits": total_hits,        # Total docs matching the query
        "returned_hits": returned_hits,  # Number of docs returned (limited by size)
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

# Load configuration file
def load_config(file_path="filter_config.json"):
    """
        API Gateway
        "north" : "$input.params('north')",
        "east" : "$input.params('east')",
        "south" : "$input.params('south')",
        "west" : "$input.params('west')",
        "keyword" : "$input.params('keyword')",
        "keyword_only" : "$input.params('keyword_only')",
        "lang" : "$input.params('lang')",
        "theme" : "$input.params('theme')",
        "type": "$input.params('type')",
        "org": "$input.params('org')",
        "min": "$input.params('min')",
        "max": "$input.params('max')",
        "foundational": "$input.params('foundational')" ,
        "sort": "$input.params('sort')",
        "source_system": "$input.params('sourcesystemname')" ,
        "eo_collection": "$input.params('eocollection')" ,
        "polarization": "$input.params('polarization')" ,
        "orbit_direction": "$input.params('orbit')",
        "begin": 2024-11-11T20:30:00.000Z
        "end": 2024-11-11T20:30:00.000Z
        "bbox": 48|-121|61|-109
    """
    with open(file_path, "r") as file:
        return json.load(file)
    
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
    
    k = 10
    payload = event['q']
    
    # Debug event
    #print("event", event)
    
    filter_config = load_config()

    # Extract response variables
    from_param = event.get('from', 0)
    size = 10
    size_param = event.get('size', 10)    
    if int(size_param) == 0:
        size = 10
    else:
        size = int(size_param)
    sort_param = event.get('sort', "relevancy")
    order_param = event.get('order', "desc")

    # Extract filters from the event input
    organization_filter = event.get('org', None)
    metadata_source_filter = event.get('metadata_source', None)

    # Temporal filters
    start_date_filter = event.get('begin', None)
    end_date_filter = event.get('end', None)

    # Spatial filters
    spatial_filter = event.get('bbox', None)
    relation = event.get('relation', None)

    # Convert filter string into list (handle multi-selection of filters) 
    #organization_list = [org.strip() for org in organization_filter.split(",")]
    
    filters = []
    if organization_filter:
        organization_field = filter_config["org"]  # Get field paths from config
        print(organization_field)
        filters.append(build_wildcard_filter(organization_field, organization_filter))
    if metadata_source_filter:
        filters.extend({"term": {"metadata_source.keyword": metadata_source_filter}})

    print("filters : ", filters)
    
    # Temporal filters
    if start_date_filter and end_date_filter:
        begin_field = filter_config["begin"][0]
        end_field = filter_config["end"][0]
        filters.extend(build_date_filter(begin_field, end_field, start_date=start_date_filter, end_date=end_date_filter))
    elif start_date_filter:
        begin_field = filter_config["begin"][0]
        filters.extend(build_date_filter(begin_field, start_date=start_date_filter))
    elif end_date_filter:
        end_field = filter_config["end"][0]
        filters.extend(build_date_filter(end_field, end_date=end_date_filter))
    
    # Spatial filters
    if spatial_filter:
        spatial_field = filter_config["bbox"][0]
        filters.append(build_spatial_filter(spatial_field, spatial_filter, relation))
    
    # Sort param
    if sort_param and order_param:
        sort_param = build_sort_filter(sort_field=sort_param, sort_order=order_param)
        #filters.append(build_sort_filter(sort_field=sort_param, sort_order=order_param))
    
    # If no filters are specified, set filters to None
    filters = filters if filters else None

    print("filters : ", filters)
    
    if event['method'] == 'SemanticSearch':
        #print(f'This is payload {payload}')
        
        features = invoke_sagemaker_endpoint(sagemaker_endpoint, payload, region)
        #print(sagemaker_endpoint)
        #print(f"Features retrieved from SageMaker: {features}")
        
        semantic_search = semantic_search_neighbors(
            features=features,
            os_client=os_client,
            k_neighbors=10,
            from_param=from_param,
            idx_name=model_name,
            filters=filters,
            sort_param=sort_param,
            size=size
        )
        
        #print(f'Type of the semantic response is {type(json.dumps(semantic_search))}')
        #print(json.dumps(semantic_search))
        
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
