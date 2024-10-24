import json
import boto3

def get_embedding(text):
    bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1")
    model_id = "amazon.titan-embed-text-v2:0"

    # Prepare request for Titan
    native_request = {"inputText": text}
    request = json.dumps(native_request)

    # Invoke Titan model for text embedding
    response = bedrock_client.invoke_model(modelId=model_id, body=request)

    # Parse the response
    response_body = json.loads(response.get("body").read())
    embedding = response_body["embedding"]
    
    return embedding
