################################################################
# This script queries the budget settings of a campaign via GQL.
################################################################

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

# Replace the placeholder with the ID of the campaign that has the budget settings you want to retrieve.
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

# Retreives the first page of budget data for a campaign.
def retrieve_campaign_budget_data(campaign_id: str) -> dict[Any, Any]:
  # Define the GraphQL query.
  query = """
  query Campaign($campaignId: ID!) {
    campaign(id: $campaignId) {
      budget {
        total
      }
      pacingMode
      timeZone
      budgetInImpressions
      flights {
        totalCount
        edges {
          cursor
          node {
            budgetInAdvertiserCurrency
            budgetInImpressions
            dailyTargetInAdvertiserCurrency
            dailyTargetInImpressions
            id
            isCurrent
            startDateInclusiveUTC
            adGroupFlights {
              totalCount
              edges {
                cursor
                node {
                  adGroupId
                  budgetInAdvertiserCurrency
                  budgetInImpressions
                  minimumSpendInAdvertiserCurrency
                }
              }
            }
          }
        }
      }
    }
  }
  """

  # Define the variables in the query.
  variables: dict[str, Any] = {
      'campaignId': campaign_id
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if not request_success:
    print(response.errors)
    raise Exception('Could not retrieve budget settings.')

  return response.data

#########################################################################################
# Execution Flow:
#  1. Call GQL to retrieve the campaign budget settings.
#  2. Print the budget settings.
#########################################################################################
budget = retrieve_campaign_budget_data(target_campaign_id)
print(budget)