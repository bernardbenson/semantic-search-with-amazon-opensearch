# Create Semantic Search Engine with Amazon Sagemaker and Amazon OpenSearch
This repository provides the infrustracture to create a semantic search engine using Amazon SageMaker and Amazon OpenSearch Services. The overall goal is to leverage the power of Large Language Models (both pretrained and finetuned) to enhance the relevancy and accuracy of search results on geo.ca.

## Model Fine-Tuning 
Semantic search engines go beyond mere keyword matching to interpret the intent and contextual meaning of search queries. Unlike traditional keyword search, semantic search can handle natural language queries and complex requests, recognizing synonyms and variations. We fine-tuned the [Sentence-Transformer models](https://huggingface.co/sentence-transformers) to improve search relevancy on geospatial metadata search. Details on model fine-tuning are available in the repository [semantic-search-model-evaluation](https://github.com/Canadian-Geospatial-Platform/semantic-search-model-evaluation).

## Semantic Search Architecture 
The Semantic Search Architecture is a serverless setup consisting of the following steps:

1. Finetuning the chosen sentence-transformer models on HPC.
2. Saving the finetuned models in an AWS S3 bucket.
3. Loading the model to SageMaker and hosting a SageMaker Endpoint using the finetuned model.
4. Creating a Vector Index in the Amazon OpenSearch domain, embedding the text dataset (in this case, geospatial metadata) into vectors using the finetuned models, and loading the vectors into the Vector Index using a Lambda function.
5. Creating a Lambda function to call SageMaker Endpoints to generate embeddings from user search queries, performing  K-Nearest Neighbors (KNN) search on the OpenSearch Vector Index and sending the query results back to the API gateway.
6. The API gateway sends the search results to the frontend and returns search results to the users.


![Semantic_search_finetune_fullstack](image/Semantic_search_finetune_fullstack.png)


## CloudFormation Deployment 
Detailes to be added. 
