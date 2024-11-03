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


print("Adding to Index...")
pk=1
# Add the text and its embedding to the index
sentence = "Fighter Pilots have incredible stamina"
sentence_embedding = model.encode(sentence)
addToIndex(pk,sentence_embedding)

pk=pk+1
sentence = "This is a dog and cat movie thats really funny"
sentence_embedding = model.encode(sentence)
addToIndex(pk,sentence_embedding)

pk=pk+1
sentence = "I love the hockey game"
sentence_embedding = model.encode(sentence)
addToIndex(pk,sentence_embedding)

pk=pk+1
sentence = "Golden retrievers when small have very sharp teeth and the cutest breath"
sentence_embedding = model.encode(sentence)
addToIndex(pk,sentence_embedding)

vector = embedding_matcher
matches: Matches = index.search(embedding_matcher, 4)

for match in matches:
    print(match.key, '{:.5f}'.format(match.distance))
print()

index.save("my_index.usearch")


