#################################################################
# This script will retrieve all advertiser deltas for a partner.
#################################################################

import json
import requests
import time
from typing import Any, List, Tuple

###########
# Constants
###########

# Define the GQL Platform API endpoint URLs.
EXTERNAL_SB_GQL_URL = 'https://ext-api.sb.thetradedesk.com/graphql'
PROD_GQL_URL = 'https://desk.thetradedesk.com/graphql'

#############################
# Variables for YOU to define
#############################

# Define the GraphQL Platform API endpoint URL this script will use.
gql_url = EXTERNAL_SB_GQL_URL

# Replace the placeholder value with your actual API token.
token = 'AUTH_TOKEN_PLACEHOLDER'

# Partner ID to retrive data for.
target_partner_id = 'PARTNER_ID_PLACEHOLDER'

# The minimum tracking version to start queying with. If 0, the current minimum tracking version will be fetched.
starting_minimum_tracking_version = 0

#############################
# Output variables
#############################

# This is the tracking version for the next iteration of fetching data.
next_change_tracking_version = 0

# A list holding the advertisers that have been updated and should be processed by your system.
changed_advertisers_list = []

################
# Helper Methods
################

show_timings = False

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
    # print(response)

  # Parse any data if it exists, otherwise, return an empty dictionary.
  resp_data = content.get('data', {})
  # Parse any errors if they exist, otherwise, return an empty error list.
  errors = content.get('errors', [])

  return (response.ok, GqlResponse(resp_data, errors))

def log_timing(text:str, start_time, end_time) -> str:
  if show_timings:
    print(f'{text}: {(end_time - start_time):.2f} seconds')

# A GQL query to retrieve the current minimum tracking version for a partner.
def get_current_minimum_tracking_version(partner_id: str) -> Any:
  query = """
  query GetAdvertisersDeltaMinimumVersion($partnerIds: [ID!]!) {
    advertiserDelta(
      input: {
        changeTrackingVersion: 0
        ids: $partnerIds
      }
    ) {
      currentMinimumTrackingVersion
    }
  }"""

  # Define the variables in the query.
  variables: dict[str, Any] = {
    'partnerIds': [partner_id]
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if not request_success:
    print(response.errors)
    raise Exception('Failed to retrieve current minimum tracking version.')

  return response.data['advertiserDelta']['currentMinimumTrackingVersion']


# A GQL query to retrieve the advertisers delta for a partner.
def get_advertisers_delta(partner_id: str, change_tracking_version: int) -> Any:
  query = """
  query GetAdvertisersDelta($changeTrackingVersion: Long!, $partnerIds: [ID!]!) {
    advertiserDelta(
      input: {
        changeTrackingVersion: $changeTrackingVersion
        ids: $partnerIds
      }
    ) {
      nextChangeTrackingVersion
      moreAvailable
      advertisers {
        id
        name
        partner{
            id
        }
        isArchived
      }
    }
  }"""

  # Define the variables in the query.
  variables: dict[str, Any] = {
    'changeTrackingVersion': change_tracking_version,
    'partnerIds': [partner_id]
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if not request_success:
    print(response.errors)
    raise Exception('Failed to retrieve advertiser delta.')

  return response.data['advertiserDelta']


########################################################
# Execution Flow:
#  1. Get the minimum tracking version.
#  2. Retrieve all the advertiser deltas for the particular partner.
########################################################
start_time = time.time()

# Get the current minimum tracking version if a `starting_minimum_tracking_version` is not specified.
minimum_tracking_version = get_current_minimum_tracking_version(target_partner_id) if starting_minimum_tracking_version == 0 else starting_minimum_tracking_version
print(f'Minimum tracking version: {minimum_tracking_version}')

i = 0

more_available = True
next_page_minimum_tracking_version = minimum_tracking_version
print(f'Processing chunk {i}')
i += 1
while (more_available):
  # Get advertisers for this partner.
  data = get_advertisers_delta(target_partner_id, next_page_minimum_tracking_version)

  for advertiser in data['advertisers']:
    changed_advertisers_list.append(advertiser)

  more_available = data['moreAvailable']
  next_page_minimum_tracking_version = data['nextChangeTrackingVersion']

  # Ensure that we capture the maximum next change tracking version to report at the end of this.
  # Only do this once we have gone through all the pages of ad groups for this advertiser
  if not more_available:
    next_change_tracking_version = max(next_change_tracking_version, data['nextChangeTrackingVersion'])

end_time = time.time()

# Output data.
print()
print('Output data:')
print(f'Next minimum change tracking version: {next_change_tracking_version}')
print(f'Changed advertiser count: {len(changed_advertisers_list)}')
log_timing('Total processing time', start_time, end_time)
