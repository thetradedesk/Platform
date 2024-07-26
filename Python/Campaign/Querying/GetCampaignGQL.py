##################################################################
# This script calls GQL to get the name and version of a Campaign.
##################################################################

import json
import requests
from typing import Any, List, Tuple

###########
# Constants
###########

# Define the GQL Platform API endpoint URLs.
EXTERNAL_SB_GQL_URL = 'https://ext-desk.sb.thetradedesk.com/graphql'
PROD_GQL_URL = 'https://desk.thetradedesk.com/graphql'

#############################
# Variables for YOU to define
#############################

# Define the GraphQL Platform API endpoint URL this script will use.
gql_url = EXTERNAL_SB_GQL_URL

# Replace the placeholder value with your actual API token.
token = 'AUTH_TOKEN_PLACEHOLDER'

# Replace the placeholder with the ID of the campaign you want to query.
target_campaign_id = 'TARGET_CAMPAIGN_ID_PLACEHOLDER'

################
# Helper Methods
################

# Represents a response from the GQL server.
class GqlResponse:
  def __init__(self, data: dict[Any, Any], errors: List[Any]) -> None:
    # This is where the return data from the GQL operation is stored.
    self.data = data
    # This is where any errors from the GQL operation are stored.
    self.errors = errors

# Executes a GQL request to the specified gql_url, using the provided body definition and associated variables.
# This indicates if the call was successful and returns the `GqlResponse`.
def execute_gql_request(body, variables) -> Tuple[bool, GqlResponse]:
  # Create headers with the authorization token.
  headers: dict[str, str] = {
    'TTD-Auth': token
  }

  # Create a dictionary for the GraphQL request.
  data: dict[str, Any] = {
    'query': body,
    'variables': variables
  }

  # Send the GraphQL request.
  response = requests.post(url=gql_url, json=data, headers=headers)
  content = json.loads(response.content) if len(response.content) > 0 else {}

  if not response.ok:
    print('GQL request failed!')
    # For more verbose error messaging, uncomment the following line:
    #print(response)

  # Parse any data if it exists, otherwise, return an empty dictionary.
  resp_data = content.get('data', {})
  # Parse any errors if they exist, otherwise, return an empty error list.
  errors = content.get('errors', [])

  return (response.ok, GqlResponse(resp_data, errors))

# Queries a given campaign by ID and prints the result.
def query_campaign(campaign_id: str) -> None:
  # Define the GraphQL query.
  query = """
  query GetCampaign($campaignId: ID!) {
    campaign(id: $campaignId) {
      id
      name
      version
    }
  }"""

  # Define the variables in the query.
  variables = {
    "campaignId": campaign_id
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  # If the call was unsuccessful, output the error.
  if not request_success:
    print(response.errors)
    raise Exception(f'Could not query campaign ID {campaign_id}')
  else:
    print('Campaign successfully queried. Data below:')
    print(response.data)

###########################################################
# Execution Flow:
#  1. Query the campaign ID specified and print the result.
###########################################################
query_campaign(target_campaign_id)