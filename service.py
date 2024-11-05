from flask import Flask
from flask import request
import json
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from utils.embedding import get_embedding
from utils.secrets import get_secrets

app = Flask(__name__)

# Load secrets
secrets = get_secrets()
INDEX_NAME = "movies-embedding-234"


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



# OpenSearch Client Setup
opensearch_client = OpenSearch(
    hosts=[{'host': OPENSEARCH_HOST, 'port': 443}],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


@app.route('/embed/movie', methods=['POST'])
def embed():
    print(request.data)
    embed_request = json.loads(request.data)
    print("movie plot is " + embed_request['plot'])

    print('embedding movie')
    embedding = get_embedding(embed_request['plot'])
    document = {
                "pk_id": embed_request['id'],
                "plot_embedding": embedding
            }
    opensearch_client.index(index=INDEX_NAME, body=document)


    error = None
    return {
        "status": "ok"
    }