# import json
# import time
# import boto3

# from tqdm import tqdm
# from urllib.parse import urlparse
# from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
from opensearch import get_awsauth_from_secret, create_opensearch_connection, delete_aos_index_if_exists, load_data_to_opensearch_index
from Preprocess_and_embed_text import read_parquet_from_s3_as_df
import argparse


def main(region, aos_host, os_secret_id, bucket, filename):
    awsauth = get_awsauth_from_secret(region, secret_id=os_secret_id)
    aos_client = create_opensearch_connection(aos_host, awsauth)

    if aos_client is None:
        print("Unable to create OpenSearch client.")
        return

    index_name = "mpnet-mpf-knn"
    knn_index = {
        "settings": {
            "index.knn": True,
            "index.knn.space_type": "cosinesimil",
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "standard",
                        "stopwords": "_english_"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "vector": {
                    "type": "knn_vector",
                    "dimension": 768,
                    "store": True
                },
                "coordinates": {
                    "type": "geo_shape",
                    "store": True
                }
            }
        }
    }

    #Read the embedding data from the S3 bucket 
    df_en = read_parquet_from_s3_as_df(region, bucket, filename)


    #Delete index if it exists 
    delete_aos_index_if_exists(aos_client, index_to_delete=index_name)

    #Create a index 
    response = aos_client.indices.create(index=index_name,body=knn_index,ignore=400)
    print(f'Index creation response: {response}')

    #Load data to OpenSearch Index 
    load_data_to_opensearch_index(df_en, aos_client, index_name)
    res = aos_client.search(index=index_name, body={"query": {"match_all": {}}})
    print(f"Records loaded into the index {index_name} is {res['hits']['total']['value']}.")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create OpenSearch index.')
    parser.add_argument('--region', type=str, required=True, help='AWS region')
    parser.add_argument('--aos_host', type=str, required=True, help='OpenSearch host')
    parser.add_argument('--os_secret_id', type=str, required=True, help='OpenSearch Secret ID')
    parser.add_argument('--bucekt', type=str, required=True, help='embedding data S3 bucket')
    parser.add_argument('--filename', type=str, required=True, help='embedding data filename')

    args = parser.parse_args()

    main(region=args.region, aos_host=args.aos_host, os_secret_id=args.os_secret_id, bucket=args.bucket, filename=args.filename)

#bucekt ='webpresence-nlp-data-preprocessing-dev'
#filename='semantic_search_embeddings.parquet'
#region = "ca-central-1"
#aos_host = "search-semantic-search-dfcizxxxuj62dusl5skmeu3czu.ca-central-1.es.amazonaws.com"
#os_secret_id = "dev/OpenSearch/SemanticSearch"
