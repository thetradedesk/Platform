import requests

# Define the GraphQL Platform API endpoint URLs.
sandbox_url = 'https://ext-api.sb.thetradedesk.com/graphql'

# Replace the placeholder value with your actual API token.
token = 'AUTH_TOKEN_PLACEHOLDER'

# Define the GraphQL query.
query = """
query GetCampaign($campaignId: ID!) {
    campaign(id: $campaignId) {
        id
        name
        version
    }
}
"""

# Define the variables in the query.
variables = {
    "campaignId": "NEW_CAMPAIGN_ID"  # Use the new campaignID created from CreateCampaign.py 
}

# Create a dictionary for the GraphQL request
data = {
    'query': query,
    'variables': variables
}

# Create headers with the authorization token
headers = {
    'TTD-Auth': token
}

# Send the GraphQL request
# The url param is used only for demonstration purposes. Be sure to replace it with the GraphQL platform API URL you want to target.
response = requests.post(url=sandbox_url, json=data, headers=headers)

# If call was unsuccessful, output the error
if not response.ok:
    print(f"Request failed with status code: {response.status_code}")
    print(response.text)
else:
    response_data = json.loads(response.content)
    print(response_data)
    #
    #  API Campaign Load Successful!
    #