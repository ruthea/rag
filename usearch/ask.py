# Import libraries
import numpy as np
import datetime


from usearch.index import Index, Matches
from sentence_transformers import SentenceTransformer
print("Loading...")
print(datetime.datetime.now())
index = Index.restore("./my_index.usearch", view=True)
print("Loading Done...")
print(datetime.datetime.now())


sentence = "I like movies about puppies and dogs."

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
print(datetime.datetime.now())
embedding_matcher = model.encode(sentence)
print("Embedding Done...")
print(datetime.datetime.now())
print("Starting search...")
print(datetime.datetime.now())
matches: Matches = index.search(embedding_matcher, 4)
print("Search Done...")
print(datetime.datetime.now())

for match in matches:
    print(match.key, '{:.5f}'.format(match.distance))
print()




