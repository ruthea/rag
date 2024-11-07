import argparse
import boto3
import json
from opensearchpy import OpenSearch, RequestsHttpConnection
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from utils.embedding import get_embedding
from utils.secrets import get_secrets
import sys

bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1")

prompt = "Instruction: Please answer the question by the user. The response should be professional and kind. Use only the data provided in the Context. If you can't answer the question with the context provided merely say that you can't help with that one. Movie titles listed should each be in a new line with a bullet."

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

def get_movie_titles(comma_separated_ids):
    """
    Retrieves movie titles based on a comma-separated string of IDs.

    Args:
        comma_separated_ids (str): A comma-separated string of movie IDs.

    Returns:
        list: A list of movie titles.
    """

    sql_query = f"SELECT title FROM {KEYSPACE}.{TABLE} WHERE id IN ({comma_separated_ids})"
    rows = session.execute(sql_query)
    movie_titles = [row.title for row in rows]

    return movie_titles

def ask_question(question, model_id):
  """
  Asks a question to a Bedrock LLM.

  Args:
      question (str): The question to ask.
      model_id (str): The ID of the Bedrock model to use.

  Returns:
      str: The response from the LLM.
  """

  bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1")

  native_request = {"inputText": question}
  request = json.dumps(native_request)

  response = bedrock_client.invoke_model(modelId=model_id, body=request)
  response_body = json.loads(response.get("body").read())
  return response_body['results'][0]['outputText']

def modify_a_movie(movie_id, movie_plot):
  """
  Modify a movie.

  Args:
      movie_id (str): Id of the movie to modify
      movie_plot (str): Plot of the movie 

  """
  sql_query = f"UPDATE  {KEYSPACE}.{TABLE} set plot='{movie_plot}' where id={movie_id}"
  print(sql_query)
  session.execute(sql_query)
  return


def add_a_movie(movie_id, movie_title, movie_plot):
  """
  Adds a movie.

  Args:
      movie_id (str): Id of the movie to add
      movie_title (str): Title of the movie
      movie_plot (str): Plot of the movie 

  """
  sql_query = f"INSERT INTO  {KEYSPACE}.{TABLE}(id, title, plot) values({movie_id},'{movie_title}','{movie_plot}')"
  print(sql_query)
  session.execute(sql_query)
  return

input_text = "What movies are about history?"
#  Parse command line arguments
parser = argparse.ArgumentParser(description="Find similar movies based on text input.")
parser.add_argument("text", nargs="?", type=str, default=input_text,
                    help="Text to search for similar movies (default: movies that have veterans)")
args = parser.parse_args()


id_top = 99999999
while True:
    options = "1: Add a Movie \n2: Modify a plot\n3: Explore by plot \n4: Quit"
    print(options)
    option = input("Choose an option: ")


    if option == '4':
        print("Goodbye!")
        sys.exit(0)
    elif option == '1':
        id_top = id_top - 1
        movie_name = input("Provide the Movie title: ")
        movie_plot = input("Provide the Movie plot: ")
        add_a_movie(id_top, movie_name, movie_plot)
    elif option == '2':
        movie_id = input("Provide the Movie Id: ")
        movie_plot = input("Provide the Movie plot: ")
        modify_a_movie(movie_id, movie_plot)
    else :
        input_text = input("Choose a topic: ")
        # get embedding from bedrock
        question_embedding = get_embedding(input_text)

        print("\nYour Question:")
        print(input_text)
        # do vector search against OpenSearch
        hits_pk = run_query(question_embedding)
        movie_titles = get_movie_titles(hits_pk)

        # Get movie titles based on retrieved IDs
        movie_titles = get_movie_titles(hits_pk)

        # Construct the RAG prompt with retrieved titles as context
        context = "\n".join(movie_titles)  # Join titles with newlines for readability

        full_prompt = f"{prompt}\nContext:\n{context}"
        answer = ask_question(full_prompt,"amazon.titan-text-lite-v1")
        print(f"\nAnswer: {answer}")


    # add
    # modify (new language)
    # delete