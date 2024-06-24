import json
import time
import boto3
from tqdm import tqdm
from urllib.parse import urlparse
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth


def get_awsauth_from_secret(region, secret_id):
    """
    Retrieves AWS opensearh credentials stored in AWS Secrets Manager.
    """

    client = boto3.client('secretsmanager', region_name=region)
    
    try:
        response = client.get_secret_value(SecretId=secret_id)
        secret = json.loads(response['SecretString'])

        master_username = secret['MASTER_USERNAME']
        master_password = secret['MASTER_PASSWORD']

        return (master_username, master_password)
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

def create_opensearch_connection(aos_host, awsauth):
    try:
        # Create the OpenSearch client
        aos_client = OpenSearch(
            hosts=[{'host': aos_host, 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        # Print the client to confirm the connection
        print("Connection to OpenSearch established:", aos_client)
        return aos_client
    except Exception as e:
        print("Failed to connect to OpenSearch:", e)
        return None

def delete_aos_index_if_exists(aos_client, index_to_delete):
    """
    Deletes the specified index if it exists.

    :param aos_client: An instance of OpenSearch client.
    :param index_to_delete: The name of the index to delete.
    """
    # List all indexes and check if the specified index exists
    all_indices = aos_client.cat.indices(format='json')
    existing_indices = [index['index'] for index in all_indices]
    print("Current indexes:", existing_indices)

    if index_to_delete in existing_indices:
        # Delete the specified index
        try:
            response = aos_client.indices.delete(index=index_to_delete)
            print(f"Deleted index: {index_to_delete}")
            print("Response:", response)
        except Exception as e:
            print(f"Error deleting index {index_to_delete}:", e)
    else:
        print(f"Index {index_to_delete} does not exist.")

    # List all indexes again to confirm deletion
    all_indices_after_deletion = aos_client.cat.indices(format='json')
    existing_indices_after_deletion = [index['index'] for index in all_indices_after_deletion]
    print("Indexes after deletion attempt:", existing_indices_after_deletion)


def load_data_to_opensearch_index(df_en, aos_client, index_name, log_level="INFO"):
    """
    Index data from a pandas DataFrame to an OpenSearch index.

    Parameters:
    - df_en: DataFrame containing the data to index.
    - aos_client: OpenSearch client.
    - index_name: Name of the OpenSearch index to which the data will be indexed.
    - log_level: Logging level, defaults to "INFO". Set to "DEBUG" for detailed logs.
    """
    start_time = time.time()

    # Convert DataFrame to a list of dictionaries (JSON)
    json_en = df_en.to_dict("records")

    # Index the data
    for x in tqdm(json_en, desc="Indexing Records"):
        try:
            bounding_box = json.loads(x.get('features_geometry_coordinates', '[]'))
            coordinates = {
                "type": "polygon",
                "coordinates": bounding_box
            }

            document = {
                'id': x.get('features_properties_id', ''),
                'coordinates': coordinates,
                'title': x.get('features_properties_title_en', ''),
                'description': x.get('features_properties_description_en', ''),
                'published': x.get('features_properties_date_published_date', ''),
                'keywords': x.get('features_properties_keywords_en', ''),
                'options': json.loads(x.get('features_properties_options', '[]')),
                'contact': json.loads(x.get('features_properties_contact', '[]')),
                'topicCategory': x.get('features_properties_topicCategory', ''),
                'created': x.get('features_properties_date_created_date', ''),
                'spatialRepresentation': x.get('features_properties_spatialRepresentation', ''),
                'type': x.get('features_properties_type', ''),
                'temporalExtent': x.get('temporalExtent', ''),
                'graphicOverview': json.loads(x.get('features_properties_graphicOverview', '[]')),
                'language': x.get('features_properties_language', ''),
                'organisation': x.get('organisation_en', ''),
                'popularity': int(x.get('features_popularity', '0')),
                'systemName': x.get('features_properties_sourceSystemName', ''),
                'eoCollection': x.get('features_properties_eoCollection', ''),
                'eoFilters': json.loads(x.get('features_properties_eoFilters', '[]')),
                "vector":x.get("vector", "")
            }

            if log_level == "DEBUG":
                print((json.dumps(document, indent=4)))

            aos_client.index(index=index_name, body=document)

        except Exception as e:
            print(e)

    # Final record count check (Optional, can slow down the script if the index is large)
    res = aos_client.search(index=index_name, body={"query": {"match_all": {}}})
    total_time = time.time() - start_time
    print(f"Completed indexing. Records loaded into the index {index_name}: {res['hits']['total']['value']}. Total time taken: {total_time:.2f} seconds.")