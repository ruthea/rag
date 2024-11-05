# Load secrets from your secrets management file or environment variables
def get_secrets(secrets_file='secrets.txt'):
    """Load secrets from a given file."""
    secrets = {}
    with open(secrets_file, 'r') as file:
        for line in file:
            key, value = line.strip().split('=', 1)
            secrets[key.strip()] = value.strip()
    return secrets
