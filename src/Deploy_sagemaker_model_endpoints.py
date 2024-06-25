import boto3
import json
import sagemaker
from sagemaker import get_execution_role
from sagemaker.huggingface.model import HuggingFaceModel
from sagemaker_fn import deploy_huggingface_model

def main():
    parser = argparse.ArgumentParser(description="Deploy Hugging Face Model to SageMaker")
    
    parser.add_argument('--model_path', type=str, required=True, help="Path to the model tar.gz file")
    parser.add_argument('--key_prefix', type=str, required=True, help="S3 key prefix where the model will be uploaded")
    parser.add_argument('--transformers_version', type=str, default="4.26", help="Transformers version used")
    parser.add_argument('--pytorch_version', type=str, default="1.13", help="PyTorch version used")
    parser.add_argument('--py_version', type=str, default="py39", help="Python version used")
    parser.add_argument('--instance_type', type=str, default="ml.t2.medium", help="Type of instance to deploy the model on")
    parser.add_argument('--endpoint_name', type=str, default="all-mpnet-base-v2-mpf-huggingface-test", help="Name of the endpoint")

    args = parser.parse_args()

    deploy_huggingface_model(
        model_path=args.model_path,
        key_prefix=args.key_prefix,
        transformers_version=args.transformers_version,
        pytorch_version=args.pytorch_version,
        py_version=args.py_version,
        instance_type=args.instance_type,
        endpoint_name=args.endpoint_name
    )

if __name__ == "__main__":
    main()
    