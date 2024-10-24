import json
import boto3

def get_embedding(text, model_id="amazon.titan-embed-text-v2:0"):
    """
    Gets an embedding from a model (defaults to titan-embed-text-v2).

    Args:
        text (str): The text to convert to an embedding.
        model_id (str, optional): The ID of the Titan model to use. Defaults to "amazon.titan-embed-text-v2:0".

    Returns:
        list: The embedding as a list of floats.
    """
    bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1")

    # Prepare request for Titan
    native_request = {"inputText": text}
    request = json.dumps(native_request)

    # Invoke Titan model for text embedding
    response = bedrock_client.invoke_model(modelId=model_id, body=request)

    # Parse the response
    response_body = json.loads(response.get("body").read())
    embedding = response_body["embedding"]

    return embedding
