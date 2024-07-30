##########################################################################################################
# This script outlines how to query for report metadata. Here's what you need to know:
# - Report metadata shows what kind of reports are available for ad groups, campaigns, and advertisers.
# - This script also provides information about whether a report is immediately available (download) or scheduled and emailed to you.
##########################################################################################################

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

# Replace the placeholders with the IDs of the ad group, campaign, and advertiser of which you want to retrieve the report metadata.
# NOTE: The IDs you provide determine which mutation will be called. If more than one ID is provided, then the mutation for the entity with the highest priority will be executed, in the following order: adgroup > campaign > advertiser.
target_adgroup_id = 'ADGROUP_ID_PLACEHOLDER'
target_campaign_id = 'CAMPAIGN_ID_PLACEHOLDER'
target_advertiser_id = 'ADVERTISER_ID_PLACEHOLDER'

# This is the kokai tile of which you'd like to request information the available reports for.
# Replace the placeholder with the tile abbreviation (Af, Ag, Ca, and so on.) from the programmatic table in the platform UI to retrieve a list of available reports.
# NOTE: This is used in combination with the target entity ID.
kokai_tile = 'KOKAI_TILE_PLACEHOLDER'

################
# Helper Methods
################

# Represents a response from the GQL server.
class GqlResponse:
  def __init__(self, data: dict[Any, Any], errors: List[Any]) -> None:
    # This is where return data from the GQL operation is stored.
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

# Queries the metadata of a report .
def query_metadata(adgroup_id: str, campaign_id: str, advertiser_id: str, tile: str) -> Tuple[bool, GqlResponse]:

  # Define the GraphQL query.
  query = """
    query( $adGroupId: ID, $campaignId: ID, $advertiserId: ID, $tile: ID! ){
        programmaticTileReportMetadata(input: {
            adGroupId: $adGroupId,
            campaignId: $campaignId,
            advertiserId: $advertiserId,
            tile: $tile
        }) {
            data {
                available
                schedule
                type
            }
            userErrors {
                field
                message
            }
        }
    }
  """

  # Define the variables in the query.
  variables = {
    "adGroupId": adgroup_id,
    "campaignId": campaign_id,
    "advertiserId": advertiser_id,
    "tile": tile
  }

  # Send the GraphQL request.
  return execute_gql_request(query, variables)



#########################################################################
# Execution Flow:
#  1. Query for reports metadata using the specified ID and print the result.
#########################################################################
request_success, response = query_metadata(target_adgroup_id, target_campaign_id, target_advertiser_id, kokai_tile)

# If the call was unsuccessful, output the error.
if not request_success:
    print(response.errors)
    raise Exception(f'Could not query for the report metadata.')
else:
    print('Metadata successfully queried. Data below:')
    print(response.data)