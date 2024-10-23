# Generate and print an embedding with Amazon Titan Text Embeddings V2.

import boto3
import json
from opensearchpy import OpenSearch, RequestsHttpConnection
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

index_name = "movies-embedding-234"
model_id = "amazon.titan-embed-text-v2:0"


bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1")
model_id = "amazon.titan-embed-text-v2:0"

# Load secrets from your secrets management file or environment variables
def get_secrets(secrets_file='secrets.txt'):
    secrets = {}
    with open(secrets_file, 'r') as file:
        for line in file:
            key, value = line.strip().split('=', 1)
            secrets[key.strip()] = value.strip()
    return secrets

# Load secrets from the secrets.txt file
secrets = get_secrets()

# Check if SCYLLADB_HOSTS exists and is not empty
scylla_hosts = secrets.get("SCYLLADB_HOSTS")
if not scylla_hosts:
    raise ValueError("SCYLLADB_HOSTS is missing or empty in the secrets file.")

# Split SCYLLADB_HOSTS by commas
SCYLLADB_HOSTS = scylla_hosts.split(",")
SCYLLA_USERNAME = secrets.get("SCYLLA_USERNAME")
SCYLLA_PASSWORD = secrets.get("SCYLLA_PASSWORD")

# OpenSearch configuration
OPENSEARCH_HOST = secrets.get("OPENSEARCH_HOST")
OPENSEARCH_USER = secrets.get("OPENSEARCH_USER")
OPENSEARCH_PASSWORD = secrets.get("OPENSEARCH_PASSWORD")

# ScyllaDB Connection Setup
auth_provider = PlainTextAuthProvider(username=SCYLLA_USERNAME, password=SCYLLA_PASSWORD)
cluster = Cluster(SCYLLADB_HOSTS, auth_provider=auth_provider)
session = cluster.connect()

# AWS OpenSearch Configuration
region = 'us-east-1'  # AWS region
service = 'es'


# OpenSearch Client Setup for AWS OpenSearch Service
opensearch_client = OpenSearch(
    hosts=[{'host': secrets.get("OPENSEARCH_HOST"), 'port': 443}],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def get_embedding_from_titan(text):
    # Prepare request for Titan
    native_request = {"inputText": text}
    request = json.dumps(native_request)

    # Invoke Titan model for text embedding
    response = bedrock_client.invoke_model(modelId=model_id, body=request)

    # Parse the response
    response_body = json.loads(response.get("body").read())
    embedding = response_body["embedding"]
    print(embedding)

    return embedding

def run_query(query_embedding):
    query = {
        "size": 250,
        "_source": ["pk_id"],  # Specify fields to return
        "query": {
            "knn": {
                "plot_embedding": {
                    "vector": query_embedding,
                    "k": 5
                }
            }
        }
    }

    response = opensearch_client.search(index=index_name, body=query)
    print(response['hits'])
    # Process and print the results
    # for hit in response['hits']['hits']:
    #     print(f"Show ID: {hit['_source']['show_id']}, Embedding: {hit['_source'].get('embedding')}")
    # response = opensearch_client.search(index=index_name, body=query)


# Create a Bedrock Runtime client in the AWS Region of your choice.
client=boto3.client("bedrock-runtime", region_name="us-east-1")

# Set the model ID, e.g., Titan Text Embeddings V2.

# this is the primary key that should be stored as another attribute with the embedding in opensearch

# The text to convert to an embedding we would want to store in opensearch
input_text = "movies that have veterans"

question_embedding = get_embedding_from_titan(input_text)

print("\nYour input:")
print(input_text)
run_query(question_embedding)


