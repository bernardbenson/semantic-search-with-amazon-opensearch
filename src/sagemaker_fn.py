import boto3
import json
import sagemaker
from sagemaker import get_execution_role
from sagemaker.huggingface.model import HuggingFaceModel

# Initialize a boto3 client for SageMaker
sagemaker_client = boto3.client('sagemaker', region_name='ca-central-1')  # Specify the AWS region

def list_sagemaker_endpoints():
    """List all SageMaker endpoints"""
    try:
        # Get the list of all SageMaker endpoints
        response = sagemaker_client.list_endpoints(SortBy='Name')
        print("Listing SageMaker Endpoints:")
        for endpoint in response['Endpoints']:
            print(f"Endpoint Name: {endpoint['EndpointName']}, Status: {endpoint['EndpointStatus']}")
    except Exception as e:
        print(f"Error listing SageMaker endpoints: {e}")


def invoke_sagemaker_endpoint_ft(endpoint_name, payload, finetuned=True):
    """Invoke a SageMaker endpoint to get predictions with ContentType='application/json'."""
    # Initialize the runtime SageMaker client
    runtime_client = boto3.client('runtime.sagemaker', region_name='ca-central-1')  
    if finetuned: 
        ContentType='application/json',
    else: 
        ContentType='text/plain',

    try:
        """
        if not isinstance(payload, str):
            payload = str(payload)
        """
        # Invoke the SageMaker endpoint
        response = runtime_client.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType=ContentType,
            Body=json.dumps(payload)
        )
        # Decode the response
        result = json.loads(response['Body'].read().decode())
        return (result)
        #print(f"Prediction from {endpoint_name}: {result}")
    except Exception as e:
        print(f"Error invoking SageMaker endpoint {endpoint_name}: {e}")


def deploy_huggingface_model(model_path, key_prefix, transformers_version="4.26", pytorch_version="1.13", py_version="py39", instance_type="ml.t2.medium", endpoint_name="all-mpnet-base-v2-mpf-huggingface-test"):
    """
    Deploys a Hugging Face model to SageMaker.

    Parameters:
    model_path (str): Path to the model tar.gz file.
    key_prefix (str): S3 key prefix where the model will be uploaded.
    transformers_version (str): Version of the Transformers library.
    pytorch_version (str): Version of PyTorch.
    py_version (str): Python version.
    instance_type (str): Type of instance to deploy the model on.
    endpoint_name (str): Name of the endpoint.
    """
    try: 
        # Create a SageMaker session
        sagemaker_session = sagemaker.Session()

        # Upload model data to S3
        inputs = sagemaker_session.upload_data(path=model_path, key_prefix=key_prefix)
        print(f"Response from model upload: {inputs}") 

        # Get execution role
        role = get_execution_role()

        # Define environment for the Hugging Face model
        hub = {
            'HF_TASK': 'feature-extraction'
        }

        # Create Hugging Face Model Class
        huggingface_model = HuggingFaceModel(
            model_data=inputs,  # Path to your trained SageMaker model
            role=role,  # IAM role with permissions to create an endpoint
            transformers_version=transformers_version,  # Transformers version used
            pytorch_version=pytorch_version,  # PyTorch version used
            py_version=py_version,  # Python version used
            env=hub
        )

        # Deploy model to SageMaker Inference
        predictor = huggingface_model.deploy(
            initial_instance_count=1,
            instance_type=instance_type,
            endpoint_name=endpoint_name
        )
        print(f"Model deployed successfully to endpoint: {endpoint_name}")
        return predictor
    except Exception as e:
        print(f"Failed to deploy model: {e}")
        return None

    


