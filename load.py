import json
import boto3
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from opensearchpy import OpenSearch, RequestsHttpConnection
from utils.embedding import get_embedding
from utils.secrets import get_secrets


import logging

# Keyspace and table configuration
KEYSPACE = 'movies'
TABLE = 'movies'
INDEX_NAME = "movies-embedding-234"

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
logger.addHandler(handler)

# Load secrets
secrets = get_secrets()

# Validate ScyllaDB connection details
scylla_hosts = secrets.get("SCYLLADB_HOSTS")
if not scylla_hosts:
    raise ValueError("SCYLLADB_HOSTS is missing or empty in the secrets file.")

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

# OpenSearch Client Setup
opensearch_client = OpenSearch(
    hosts=[{'host': OPENSEARCH_HOST, 'port': 443}],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

# AWS Bedrock Setup (Amazon Titan Text Embeddings V2)

### MATCH THE SETTINGS TO YOUR OPENSEARCH SERVER!!!
def create_index_if_not_exists(index_name):
    """Create the index if it doesn't exist."""
    if not opensearch_client.indices.exists(index=index_name):
        index_body = {
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "index.knn": True
  },
  "mappings": {
    "properties": {
      "pk_id": {
        "type": "keyword"
      },
      "plot_embedding": {
        "type": "knn_vector",
        "dimension": 1024,
        "method": {
          "engine": "nmslib",
          "space_type": "cosinesimil",
          "name": "hnsw",
          "parameters": {"ef_construction": 512, "m": 16},
        },
      }
    }
  }
}
        try:
            response = opensearch_client.indices.create(index=index_name, body=index_body)
            logger.info(f"Index '{index_name}' created with response: {response}")
        except Exception as e:
            logger.error(f"Failed to create index '{index_name}': {e}")

def delete_index(index_name):
    """Delete the index if it exists."""
    if opensearch_client.indices.exists(index=index_name):
        try:
            response = opensearch_client.indices.delete(index=index_name)
            logger.info(f"Index '{index_name}' deleted successfully: {response}")
        except Exception as e:
            logger.error(f"Failed to delete index '{index_name}': {e}")
    else:
        logger.info(f"Index '{index_name}' does not exist.")

# Manage index
delete_index(INDEX_NAME)  # Clean up existing index if necessary
create_index_if_not_exists(INDEX_NAME)

# Fetch rows from ScyllaDB and generate embeddings
rows = session.execute(f"SELECT id, Plot FROM {KEYSPACE}.{TABLE}")

for count, row in enumerate(rows, start=1):
    plot_text = row.plot
    primary_key = row.id

    if plot_text:
        try:
            embedding = get_embedding(plot_text)
            document = {
                "pk_id": primary_key,
                "plot_embedding": embedding
            }
            opensearch_client.index(index=INDEX_NAME, body=document)
            # logger.info(f"Indexed document with primary key {primary_key}.")
        except Exception as e:
            logger.error(f"Failed to process row {count}: {e}")
    else:
        logger.warning(f"No plot text for primary key {primary_key}.")

logger.info("Data processing complete!")
