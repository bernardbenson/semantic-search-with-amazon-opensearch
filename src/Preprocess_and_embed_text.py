import pandas as pd 
from sentence_transformers import SentenceTransformer, util
import boto3
import torch
import argparse
from tqdm import tqdm
from io import BytesIO
from inference import model_fn, predict_fn

# Load metadata
def read_parquet_from_s3_as_df(region, s3_bucket, s3_key):
    """
    Load a Parquet file from an S3 bucket into a pandas DataFrame.

    Parameters:
    - region: AWS region where the S3 bucket is located.
    - s3_bucket: Name of the S3 bucket.
    - s3_key: Key (path) to the Parquet file within the S3 bucket.

    Returns:
    - df: pandas DataFrame containing the data from the Parquet file.
    """

    # Setup AWS session and clients
    session = boto3.Session(region_name=region)
    s3 = session.resource('s3')

    # Load the Parquet file as a pandas DataFrame
    object = s3.Object(s3_bucket, s3_key)
    body = object.get()['Body'].read()
    df = pd.read_parquet(io.BytesIO(body))
    return df


# Upload the duplicate date to S3 as a parquet file 
def upload_df_to_s3_as_parquet(df, bucket_name, file_key):
    # Save DataFrame as a Parquet file locally
    parquet_file_path = 'temp.parquet'
    df.to_parquet(parquet_file_path)

    # Create an S3 client
    s3_client = boto3.client('s3')

    # Upload the Parquet file to S3 bucket
    try:
        response = s3_client.upload_file(parquet_file_path, bucket_name, file_key)
        os.remove(parquet_file_path)
        print(f'Uploading {file_key} to {bucket_name} as parquet file')
        # Delete the local Parquet file
        return True
    except ClientError as e:
        logging.error(e)
        return False

#Extract organization from contact json, English only 
def extract_organisation_en(contact_str):
    try:
        # Parse the stringified JSON into Python objects
        contact_data = json.loads(contact_str)
        # If the parsed data is a list, iterate through it
        if isinstance(contact_data, list):
            for item in contact_data:
                # Check if 'organisation' and 'en' keys exist
                if 'organisation' in item and 'en' in item['organisation']:
                    return item['organisation']['en']
        elif isinstance(contact_data, dict):
            # If the data is a dictionary, extract 'organisation' in 'en' directly
            return contact_data.get('organisation', {}).get('en', None)
    except json.JSONDecodeError:
        # Handle cases where the contact string is not valid JSON
        return None
    except Exception as e:
        # Catch-all for any other unexpected errors
        return f"Error: {str(e)}"

# Text preprocess
def preprocess_records_into_text(df):
    selected_columns = ['features_properties_title_en','features_properties_description_en','features_properties_keywords_en']
    df = df[selected_columns]
    return df.apply(lambda x: f"{x['features_properties_title_en']}\n{x['features_properties_description_en']}\nkeywords:{x['features_properties_keywords_en']}",axis=1 )


def main(region, bucket, model_directory, output_bucket, output_key):
    # Step 1: Load the data
    df_parquet = read_parquet_from_s3_as_df(region, bucket, 'records.parquet')
    df_sentinel1 = read_parquet_from_s3_as_df(region, bucket, 'sentinel1.parquet')
    df = pd.concat([df_parquet, df_sentinel1], ignore_index=True)

    # Step 2: Clean the data
    col_names_list = [
        'features_properties_id', 'features_geometry_coordinates', 'features_properties_title_en',
        'features_properties_description_en', 'features_properties_date_published_date',
        'features_properties_keywords_en', 'features_properties_options', 'features_properties_contact',
        'features_properties_topicCategory', 'features_properties_date_created_date',
        'features_properties_spatialRepresentation', 'features_properties_type',
        'features_properties_temporalExtent_begin', 'features_properties_temporalExtent_end',
        'features_properties_graphicOverview', 'features_properties_language', 'features_popularity',
        'features_properties_sourceSystemName', 'features_properties_eoCollection',
        'features_properties_eoFilters'
    ]
    df_en = df[col_names_list]
    df_en['organisation_en'] = df_en['features_properties_contact'].apply(extract_organisation_en)

    values_to_replace = {'Present': None, 'Not Available; Indisponible': None}
    columns_to_replace = ['features_properties_temporalExtent_begin', 'features_properties_temporalExtent_end']
    df_en[columns_to_replace] = df_en[columns_to_replace].replace(values_to_replace)

    df_en['temporalExtent'] = df_en.apply(lambda row: {'begin': row['features_properties_temporalExtent_begin'], 'end': row['features_properties_temporalExtent_end']}, axis=1)
    df_en = df_en.drop(columns=['features_properties_temporalExtent_begin', 'features_properties_temporalExtent_end'])

    values_to_replace = {'Not Available; Indisponible': None}
    columns_to_replace = ['features_properties_date_published_date', 'features_properties_date_created_date']
    df_en[columns_to_replace] = df_en[columns_to_replace].replace(values_to_replace)

    # Step 3: Preprocess text
    df_en['text'] = preprocess_records_into_text(df_en)

    # Step 4: Embedding text
    tqdm.pandas()
    model = model_fn(model_directory)
    df_en['vector'] = df_en['text'].progress_apply(lambda x: predict_fn({"inputs": x}, model))

    # Step 5: Upload the embeddings as a Parquet file to S3 bucket
    upload_df_to_s3_as_parquet(df=df_en, bucket_name=output_bucket, file_key=output_key)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process and upload data to S3.')
    parser.add_argument('--region', type=str, required=True, help='AWS region')
    parser.add_argument('--bucket', type=str, required=True, help='Raw data S3 bucket name')
    parser.add_argument('--model_directory', type=str, required=True, help='Model directory')
    parser.add_argument('--output_bucket', type=str, required=True, help='Output S3 bucket name')
    parser.add_argument('--output_key', type=str, required=True, help='Output S3 file key')
    
    args = parser.parse_args()
    
    main(
        region=args.region,
        bucket=args.bucket,
        model_directory=args.model_directory,
        output_bucket=args.output_bucket,
        output_key=args.output_key
    )