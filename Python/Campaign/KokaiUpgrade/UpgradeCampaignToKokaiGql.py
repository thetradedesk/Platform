##################################################################################################################
# This script upgrades a Campaign to Kokai if it's eligible and outputs a subset of verified data upon completion.
##################################################################################################################

import json
import requests
from typing import Any, List, Tuple

###########
# Constants
###########

ext_sb_gql_url = 'https://ext-api.sb.thetradedesk.com/graphql'
prod_gql_url = 'https://desk.thetradedesk.com/graphql'
prod_azure_url = 'https://desk.dsp.walmart.com/graphql'

#############################
# Variables for YOU to define
#############################

# Define the GraphQL Platform API endpoint URL this script will use.
gql_url = ext_sb_gql_url

# Replace the placeholder value with your actual API token.
token = 'AUTH_TOKEN_PLACEHOLDER'

# The ID of the Campaign to upgrade to Kokai, if eligible.
target_campaign_id = 'TARGET_CAMPAIGN_ID_PLACEHOLDER'

# The Seed to assign to the upgraded campaign. If none is provided, defaults to the Campaign's existing Seed. If that does not exist, defaults to the Advertiser's default Seed. One of these three must exist or be provided.
seed_id = ''

################
# Helper Methods
################

# Represents a response from the GQL server.
class GqlResponse:
  def __init__(self, data: dict[Any, Any], errors: List[Any]):
    # This is where return data from the GQL operation is stored.
    self.data = data
    # This is where any errors from the GQL operation are stored.
    self.errors = errors

# Executes a GQL request against `gql_url`, given its body definition and accompanying variables.
# This returns whether the call was successful, paired with the `GqlResponse` returned.
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
    # Uncomment to see the verbose response to the bad request.
    #print(response)

  # Parse any data (if it exists), otherwise return an empty dict.
  resp_data = content.get('data', {})
  # Parse any errors (if they exist), otherwise return an empty error list.
  errors = content.get('errors', [])

  return (response.ok, GqlResponse(resp_data, errors))

# Determines if a given Campaign is eligible for upgrade to Kokai based on its version.
def is_campaign_eligible_for_upgrade(campaign_id: str) -> bool:
  # Define the GraphQL query.
  query = """
  query GetCampaignUpgradeCandidate($campaignId: ID!) {
      campaign(id: $campaignId) {
          id
          version
      }
  }
  """

  # Define the variables in the query.
  variables: dict[str, str] = {
      'campaignId': campaign_id
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  # If the request succeeded and the Campaign is not already Kokai, it is eligible.
  if request_success:
    try:
      is_kokai = response.data['campaign']['version'] == 'KOKAI'
      return not is_kokai
    except:
      print("Could not read Campaign's version from response.")
      print(response.errors)
      return False
  else:
    print(response.errors)
    raise Exception(f"Campaign ID '{campaign_id}' could not be queried.")

# Upgrades an eligible Campaign to Kokai with an optional Seed to assign to it.
# Returns true if upgraded, false if not.
def upgrade_campaign(campaign_id: str, seed_id: str) -> bool:
  # Define the GraphQL query.
  query = """
  mutation UpgradeCampaignCandidate($campaignId: String!, $seedId: String) {
      campaignVersionUpgrade(input: {
        campaigns: [
          {
            campaignId: $campaignId
            seedId: $seedId
          }
        ]
      })
      {
          data {
            wasUpgraded
          }
          userErrors {
            field
            message
          }
      }
  }
  """

  # Define the variables in the query.
  variables: dict[str, str] = {
      'campaignId': campaign_id
  }

  if seed_id is not None:
    variables['seedId'] = seed_id

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if request_success:
    if response.data['campaignVersionUpgrade']['data'][0]['wasUpgraded']:
      print(f"Campaign ID '{target_campaign_id}' was upgraded!")
      return True
    else:
      print(f"Campaign ID '{target_campaign_id}' failed to upgrade!")
      print(response.data['campaignVersionUpgrade']['userErrors'])
      return False
  else:
    print(f"Campaign ID '{target_campaign_id}' failed to upgrade!")
    print(response.errors)
    return False

# Outputs a portion of Campaign data that we expect from an upgrade.
def print_expected_campaign_data(campaign_id: str) -> None:
  # Define the GraphQL query.
  query = """
  query VerifyUpgradeData($campaignId: ID!) {
      campaign(id: $campaignId) {
        isMarketplaceEnabledByDefault
        version
        seed {
          id
        }
      }
  }
  """

  # Define the variables in the query.
  variables: dict[str, str] = {
      'campaignId': campaign_id
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if request_success:
    print(f"isMarketplaceEnabledByDefault: {response.data['campaign']['isMarketplaceEnabledByDefault']}")
    print(f"version: {response.data['campaign']['version']}")
    print(f"seedId: {response.data['campaign']['seed']['id']}")

#################################################################
# Upgrade Execution
#
# Flow:
#  1. Get the target Campaign's version from GQL.
#  2. If the Campaign version is Solimar, upgrade to Kokai.
#  3. Verify expected Campaign data if the upgrade was successful.
#################################################################

if is_campaign_eligible_for_upgrade(target_campaign_id):
  seed_to_assign = seed_id if len(seed_id) > 0 else None
  if upgrade_campaign(target_campaign_id, seed_to_assign):
    print_expected_campaign_data(target_campaign_id)
else:
   print(f"Campaign ID '{target_campaign_id}' is not eligible for upgrade to Kokai.")
