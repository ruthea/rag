# Import libraries
import numpy as np

from usearch.index import Index, Matches
from sentence_transformers import SentenceTransformer

sentence = "I like movies about puppies and dogs."

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
embedding_matcher = model.encode(sentence)

index = Index(ndim=embedding_matcher.shape[0])

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


