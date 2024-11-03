# Import libraries
import numpy as np

from usearch.index import Index, Matches
from sentence_transformers import SentenceTransformer

sentence = "I like movies about puppies and dogs."

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
embedding_matcher = model.encode(sentence)

index = Index(ndim=embedding_matcher.shape[0])


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

# ScyllaDB Connection Setup
auth_provider = PlainTextAuthProvider(username=SCYLLA_USERNAME, password=SCYLLA_PASSWORD)
cluster = Cluster(SCYLLADB_HOSTS, auth_provider=auth_provider)
session = cluster.connect()


def addToIndex(pk,text):
    index.add(pk, text)
    print("added index")

# Create an index with dimension matching the embedding size
print("Creating Index...")


# Fetch rows from ScyllaDB and generate embeddings
rows = session.execute(f"SELECT id, Plot FROM {KEYSPACE}.{TABLE}")

for count, row in enumerate(rows, start=1):
    plot_text = row.plot
    primary_key = row.id

    if plot_text:
        try:
            embedding = get_embedding(plot_text)
            addToIndex(primary_key, embedding)
        except Exception as e:
            logger.error(f"Failed to process row {count}: {e}")
    else:
        logger.warning(f"No plot text for primary key {primary_key}.")

logger.info("Data processing complete!")

index.save("my_index.usearch")


