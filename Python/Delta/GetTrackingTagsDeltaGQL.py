#################################################################
# This script will retrieve all tracking tag delta for a partner.
#################################################################

import json
import requests
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

# The minimum (earliest) tracking version to start queying with. If 0, the current minimum tracking version will be fetched.
starting_minimum_tracking_version = 0

#############################
# Output variables
#############################

# This is the tracking version for the next iteration of fetching data.
next_change_tracking_version = 0

# A list holding the tracking tags that have been updated and should be processed by your system.
changed_tracking_tags_list = []

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
    # print(response)

  # Parse any data if it exists, otherwise, return an empty dictionary.
  resp_data = content.get('data', {})
  # Parse any errors if they exist, otherwise, return an empty error list.
  errors = content.get('errors', [])

  return (response.ok, GqlResponse(resp_data, errors))

# A GQL query to retrieve all advertisers globally.
def get_all_advertisers(partner_id: str, cursor: str) -> Any:
  after_clause = f'after: "{cursor}",' if cursor else ''

  query = f"""
  query GetAdvertisers($partnerId: String!) {{
    advertisers(
      where: {{
        partnerId: {{ eq: $partnerId }}
      }}
      {after_clause}
      first: 1000) {{
      nodes {{
        id
      }}
      pageInfo {{
        endCursor
        hasNextPage
      }}
    }}
  }}"""

  # Define the variables in the query.
  variables: dict[str, Any] = {
    'partnerId': partner_id
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query,variables)

  if not request_success:
    print(response.errors)
    raise Exception('Failed to fetch advertisers.')

  return response.data


# A GQL query to retrieve the current minimum (earliest) tracking version for an advertiser.
def get_current_minimum_tracking_version(advertiser_id: str) -> Any:
  query = """
  query GetTrackingTagDelta($advertiserIds: [ID!]!) {
    trackingTagDelta(
      input: {
        advertiser: {
          changeTrackingVersion: 0
          ids: $advertiserIds
        }
      }
    ) {
      currentMinimumTrackingVersion
    }
  }"""

  # Define the variables in the query.
  variables: dict[str, Any] = {
    'advertiserIds': [advertiser_id]
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if not request_success:
    print(response.errors)
    raise Exception('Failed to retrieve current minimum tracking version.')

  return response.data['trackingTagDelta']['currentMinimumTrackingVersion']


# A GQL query to retrieve the tracking tag delta for an advertiser.
def get_tracking_tag_delta(advertiser_ids: list[str], change_tracking_version: int) -> Any:
  query = """
  query GetTrackingTagDeltas($changeTrackingVersion: Long!, $advertiserIds: [ID!]!) {
    trackingTagDelta(
      input: {
        advertiser: {
          changeTrackingVersion: $changeTrackingVersion
          ids: $advertiserIds
        }
      }
    ) {
      nextChangeTrackingVersion
      moreAvailable
      trackingTags {
        id
        name
        type
        isArchived
        advertiser {
          id
        }
      }
    }
  }"""

  # Define the variables in the query.
  variables: dict[str, Any] = {
    'changeTrackingVersion': change_tracking_version,
    'advertiserIds': advertiser_ids
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if not request_success:
    print(response.errors)
    raise Exception('Failed to retrieve tracking tag delta.')

  return response.data['trackingTagDelta']


########################################################
# Execution Flow:
#  1. Retrieve advertisers IDs (limit to 100 at a time).
#  2. Get the minimum (earliest) tracking version.
#  3. Retrieve all the tracking tag deltas.
########################################################
advertiser_ids = []
has_next = True
cursor = None

while has_next:
  print(f"Retrieving advertisers after cursor: {cursor}")
  advertiser_data = get_all_advertisers(target_partner_id, cursor)

  # Retrieve advertiser IDs.
  for node in advertiser_data['advertisers']['nodes']:
    advertiser_ids.append(node['id'])

  # Update pagination information.
  has_next = advertiser_data['advertisers']['pageInfo']['hasNextPage']
  cursor = advertiser_data['advertisers']['pageInfo']['endCursor']

print(f'Number of advertiserIds: {len(advertiser_ids)}')

# Get the current minimum (earliest) tracking version if a `starting_minimum_tracking_version` is not specified.
minimum_tracking_version = get_current_minimum_tracking_version(advertiser_ids[0]) if starting_minimum_tracking_version == 0 else starting_minimum_tracking_version
print(f'Minimum tracking version: {minimum_tracking_version}')

# Retrieve tacking tags. Splitting advertisers list into chunks of 100.
advertiser_chunks = [advertiser_ids[i:i + 100] for i in range(0, len(advertiser_ids), 100)]

i = 0
first_advertiser = True
for chunk in advertiser_chunks:
  more_available = True
  next_page_minimum_tracking_version = minimum_tracking_version

  print(f'Processing chunk {i}')
  i += 1

  while (more_available):
    # Retrieves the tracking tags for this chunk of advertisers.
    data = get_tracking_tag_delta(chunk, next_page_minimum_tracking_version)

    for trackingTag in data['trackingTags']:
      changed_tracking_tags_list.append(trackingTag)

    more_available = data['moreAvailable']
    next_page_minimum_tracking_version = data['nextChangeTrackingVersion']

    # Captures the maximum (latest) change-tracking version.
    # Do this at the end of the first advertiser we finish going through.
    if not more_available and first_advertiser:
      next_change_tracking_version = data['nextChangeTrackingVersion']
      first_advertiser = False

# Output data
print()
print('Output data:')
print(f'Next minimum change tracking version: {next_change_tracking_version}')
print(f'Changed tracking tags count: {len(changed_tracking_tags_list)}')
