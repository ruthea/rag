import boto3
import json
from opensearchpy import OpenSearch, RequestsHttpConnection
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from utils.embedding import get_embedding
from utils.secrets import get_secrets



index_name = "movies-embedding-234"
KEYSPACE = 'movies'
TABLE = 'movies'

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


def run_query(query_embedding):
    query = {
        "size": 5,
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
    # for hit in response['hits']['hits']:
    #   print(f"Show ID: {hit['_source']['pk_id']}")

    pk_id_list = [hit['_source']['pk_id'] for hit in response['hits']['hits']]
    comma_separated_ids = ','.join(str(pk_id) for pk_id in pk_id_list)


    # Return the comma-separated string
    return comma_separated_ids

# Create a Bedrock Runtime client in the AWS Region of your choice.
client=boto3.client("bedrock-runtime", region_name="us-east-1")

# Set the model ID, e.g., Titan Text Embeddings V2.

# this is the primary key that should be stored as another attribute with the embedding in opensearch

# The text to convert to an embedding we would want to store in opensearch
input_text = "movies that have veterans"

question_embedding = get_embedding(input_text)

print("\nYour input:")
print(input_text)
hits_pk = run_query(question_embedding)

# Use f-string to include hits_pk in the SQL query
sql_query = f"SELECT title FROM {KEYSPACE}.{TABLE} WHERE id IN ({hits_pk})"
rows = session.execute(sql_query)
for row in rows:
    print(row.title)
