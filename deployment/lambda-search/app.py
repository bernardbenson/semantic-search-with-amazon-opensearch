import json
import boto3


from os import environ
from datetime import datetime
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
        

def semantic_search_neighbors(features, os_client, sort_param, k_neighbors=30, from_param=0, idx_name=model_name, filters=None):
    """
    Perform semantic search and get neighbots using the cosine similarity of the vectors 
    output: a list of json, each json contains _id, _score, title, and uuid 
    """
    #print("Filters:", json.dumps(filters, indent=2))
    query={
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
        },
        "size": k_neighbors,
        "from": from_param,
        "sort": sort_param
    }

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
    payload = event['searchString']
    
    # Debug event
    #print("event", event)
    
    filter_config = load_config()

    # Extract response variables
    from_param = event.get('from', 0)
    size_param = event.get('size', 10)
    if int(size_param) == 0:
        size = 10
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
            k_neighbors=size_param,
            from_param=from_param,
            idx_name=model_name,
            filters=filters,
            sort_param=sort_param
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

def build_wildcard_filter(field_paths, values):
    """
    Builds a wildcard OR filter for multiple field paths and values.

    Args:
        field_paths (list): List of field paths from configuration.
        values (str): A comma-separated string of values to filter.

    Returns:
        dict: A bool query with should clauses for logical OR.
    """
    value_list = [val.strip() for val in values.split(",") if val.strip()]

    should_clauses = [
        {"wildcard": {field_path: {"value": f"*{value}*"}}}
        for value in value_list
        for field_path in field_paths
    ]

    print(should_clauses)

    return {
        "bool": {
            "should": should_clauses,
            "minimum_should_match": 1
        }
    }

def build_date_filter(begin_field=None, end_field=None, start_date=None, end_date=None):
    """
    Builds a string-based range filter for date fields supporting partial dates, 'null', 'not available; indisponible', and 'current'.

    Args:
        begin_field (str): Field name for the start date.
        end_field (str): Field name for the end date.
        start_date (str): The start date as a string.
        end_date (str): The end date as a string.

    Returns:
        list: A list of range queries for the provided date fields.
    """
    date_filters = []
    current_date = datetime.now().strftime('%Y-%m-%d')  # Current date in YYYY-MM-DD format

    # Handle start date for `gte`
    if start_date and start_date.lower() not in ['null', 'not available; indisponible']:
        if len(start_date) == 7:  # YYYY-MM
            start_date = f"{start_date}-01"  # Assume first day of the month
        elif len(start_date) == 4:  # YYYY
            start_date = f"{start_date}-01-01"  # Assume January 1st

        date_filters.append({
            "range": {
                begin_field: {
                    "gte": start_date
                }
            }
        })

    # Handle end date for `lte`
    if end_date:
        if end_date.lower() in ['null', 'not available; indisponible']:
            # Optionally exclude these
            date_filters.append({
                "term": {
                    end_field: "null"
                }
            })
        elif end_date.lower() == "present":
            # Treat 'present' as the current date and handle it separately
            end_date = current_date
            date_filters.append({
                "range": {
                    end_field: {
                        "lte": end_date
                    }
                }
            })

            """
            date_filters.append({
                "bool": {
                    "should": [
                        {
                            "range": {
                                end_field: {
                                    "lte": current_date  # Current date
                                }
                            }
                        },
                        {
                            "term": {
                                end_field: "Present"  # Exact "Present" match
                            }
                        }
                    ]
                }
            })
            """
        elif len(end_date) == 7:  # YYYY-MM
            end_date = f"{end_date}-31"  # Assume the last day of the month
        elif len(end_date) == 4:  # YYYY
            end_date = f"{end_date}-12-31"  # Assume December 31st

        # Avoid duplicate queries if "present" is handled
        if end_date.lower() != "present":
            date_filters.append({
                "range": {
                    end_field: {
                        "lte": end_date
                    }
                }
            })

    return date_filters

def build_spatial_filter(geo_field, bbox, relation=None):
    """
    Builds a spatial filter for geo_shape fields based on a bounding box (bbox).

    Args:
        geo_field (str): The geo_shape field name from the mapping configuration.
        bbox (list): A list of four coordinates defining the bounding box [min_lon, min_lat, max_lon, max_lat].
        relation (str): The spatial relation for the filter. Defaults to 'intersects'.

    Returns:
        dict: A geo_shape query using the 'envelope' type.

    Raises:
        ValueError: If the bbox is invalid or the relation is unsupported.
    """
    if relation is None:
        relation = "intersects"

    supported_relations = ["intersects", "disjoint", "within", "contains"]
    if relation not in supported_relations:
        raise ValueError(f"Unsupported relation '{relation}'. Must be one of {supported_relations}.")

    try:
        bbox = [float(val.strip()) for val in bbox.split("|") if val.strip()]
    except ValueError:
        raise ValueError("Invalid bbox format. Ensure it is a '|' separated string of numeric values.")    

    if not bbox or len(bbox) != 4:
        raise ValueError("Invalid bbox. Expected four coordinates: \n"
                         "min_lon (west) |min_lat (south) | max_lon (east) |max_lat (north)")

    min_lon, min_lat, max_lon, max_lat = bbox

    # Validate ranges
    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
        raise ValueError("Longitude values must be between -180 and 180.")
    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError("Latitude values must be between -90 and 90.")

    return {
        "geo_shape": {
            geo_field: {
                "shape": {
                    "type": "envelope",
                    "coordinates": [[min_lon, max_lat], [max_lon, min_lat]]
                },
                "relation": relation
            }
        }
    }

def build_sort_filter(sort_field="relevancy", sort_order="desc"):
    """
    Builds a dynamic sort parameter for OpenSearch based on a single field and order.

    Args:
        sort_field (str): The field to sort by (e.g., "relevancy", "date", "popularity", "title").
                          Defaults to "relevancy" (_score) if not provided.
        sort_order (str): The sort order ("asc" for ascending, "desc" for descending).
                          Defaults to "desc".

    Returns:
        list: A sort parameter for OpenSearch or an empty list if no sort field is specified.

    Raises:
        ValueError: If the provided sort_field is unsupported.
    """
    # Valid sort fields
    supported_sort_fields = ["_score", "date", "popularity", "title", "relevancy"]

    # Map user-friendly "relevancy" to "_score"
    if sort_field == "relevancy":
        sort_field = "_score"

    # Ensure the sort field is supported
    if sort_field not in supported_sort_fields:
        raise ValueError(
            f"Unsupported sort field '{sort_field}'. "
            f"Must be one of {supported_sort_fields}. Default is '_score' (relevancy)."
        )

    # Force relevance sorting to descending order
    if sort_field == "_score":
        sort_order = "desc"

    return [
        {sort_field: {"order": sort_order}}
    ]
