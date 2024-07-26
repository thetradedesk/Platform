#################################################################
# This script creates a new Seed and assigns it to an advertiser.
#################################################################

from enum import Enum
import json
import requests
from typing import Any, List, Tuple

###########
# Constants
###########

# Define the GQL Platform API endpoint URLs.
EXTERNAL_SB_GQL_URL = 'https://ext-desk.sb.thetradedesk.com/graphql'
PROD_GQL_URL = 'https://desk.thetradedesk.com/graphql'

# Define the REST Platform API endpoint URLs.
EXTERNAL_SB_REST_URL = 'https://ext-api.sb.thetradedesk.com/v3'
PROD_REST_URL = 'https://api.thetradedesk.com/v3'

# Represents which REST operation to execute.
class RestOperation(Enum):
  GET = 1
  POST = 2
  PUT = 3

#############################
# Variables for YOU to define
#############################

# Define the GraphQL Platform API endpoint URL this script will use.
gql_url = EXTERNAL_SB_GQL_URL

# Define the GraphQL Platform API endpoint URL this script will use.
rest_url = EXTERNAL_SB_REST_URL

# Replace the placeholder value with your actual API token.
token = 'AUTH_TOKEN_PLACEHOLDER'

# The headers to pass as part of the REST requests.
rest_headers = {
  "TTD-Auth": token,
  "Content-Type": "application/json"
}

# Replace the placeholder with the ID of the advertiser associated with the ad groups you want to query.
target_advertiser_id = 'ADVERTISER_ID_PLACEHOLDER'

# Replace the placeholder with the name of the seed you want to create.
new_seed_name = 'SEED_NAME_PLACEHOLDER'

# Set a maximum limit on the number of first-party IDs that can be added to the seed you want to create. This affects page size when making REST calls to retrieve the data of the new seed.
limit_first_party_ids = 3

# Alternative first-party data IDs to set after the seed has been created.
# NOTE: This replaces the first-party data IDs that already exist on the seed.
# If set to 1, replaces the current first-party data IDs in the seed with alternative IDs from `POST /v3/dmp/firstparty/advertiser`.
# If set to 0, no IDs will be updated after the seed is created.
alternative_first_party_ids = 1

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

# Represents a response from the REST server.
class RestResponse:
  def __init__(self, data: Any, errors: Any) -> None:
    # This is where the data returned from the REST operation is stored.
    self.data = data
    # This is where any errors from the REST operation are stored.
    self.errors = errors

# Executes a REST request to the specified rest_url, using the provided body definition and associated variables.
# This indicates if the call was successful and returns the `RestResponse`.
def execute_rest_request(operation: RestOperation, url: str, body: Any) -> Tuple[bool, RestResponse]:
  if operation == RestOperation.GET:
    response = requests.get(url, headers = rest_headers)
  elif operation == RestOperation.POST:
    response = requests.post(url, headers = rest_headers, json = body)
  elif operation == RestOperation.PUT:
    response = requests.put(url, headers = rest_headers, json = body)
  else:
    raise Exception(f'Unrecognized operation type: {operation}')

  # Check if the response returned a 200.
  if response.status_code != 200:
    error_info = response.json()
    # For more verbose error messaging, uncomment the following line:
    #print(error_info)
    error_message = error_info.get('Message', 'REST call failed. No error message provided.')
    return (False, RestResponse(None, error_message))
  else:
    data = response.json()
    return (True, RestResponse(data, None))

# This method makes a `POST /v3/dmp/firstparty/advertiser` call to retrieve the first-party data of the advertiser that you want to add to the seed.
def get_first_party_data_rest(advertiser_id: str, start_index: int, page_size: int) -> Tuple[bool, RestResponse]:
  # Defines the payload for the `POST /v3/dmp/firstparty/advertiser` call.
  first_party_data_body = {
    'AdvertiserId': advertiser_id,
    'PageStartIndex': start_index,
    'PageSize': page_size

    # To filter the first party data that you want to create your seed with, use the following fields:
    #'SearchTerms': [],
    #'SortFields': [{ }]
  }

  url = rest_url + "/dmp/firstparty/advertiser"

  return execute_rest_request(RestOperation.POST, url, first_party_data_body)

# This method uses the `SeedCreate` GraphQL mutation to create a seed with the specified segments.
def create_seed_gql(advertiser_id: str, seed_name: str, first_party_data_inclusion_ids: List[int]) -> Tuple[bool, GqlResponse]:
  # Define the GraphQL query.
  query = """
  mutation($advertiserId: ID!, $name: String!, $firstPartyDataInclusionIds: [ID!]){
    seedCreate(input: {
      advertiserId: $advertiserId,
      name: $name,
      targetingData: {
        firstPartyDataInclusionIds: $firstPartyDataInclusionIds
      }
    }) {
      data {
        id
      }
      userErrors {
        field
        message
      }
    }
  }"""

  # Define the variables in the query.
  variables = {
    'advertiserId': advertiser_id,
    'name': seed_name,
    'firstPartyDataInclusionIds': first_party_data_inclusion_ids
  }

  # Send the GraphQL request.
  return execute_gql_request(query, variables)

# This method uses `AdvertiserSetDefaultSeed` GraphQL mutation to set the advertiser default seed to the one specified.
def set_advertiser_default_seed_gql(advertiser_id: str, seed_id: str)-> Tuple[bool, GqlResponse]:
  # Define the GraphQL query.
  query = """
  mutation($advertiserId:ID!, $seedId:ID!){
    advertiserSetDefaultSeed(input: { advertiserId: $advertiserId, seedId: $seedId }) {
      data {
        defaultSeed {
          id
        }
      }
      userErrors {
        field
        message
      }
    }
  }"""

  # Define the variables in the query.
  variables = {
    'advertiserId': advertiser_id,
    'seedId': seed_id
  }

  # Send the GraphQL request.
  return execute_gql_request(query, variables)

# This method uses the `SeedUpdate` GraphQL mutation to update the seed's first party data.
def update_seed_gql(seed_id: str, first_party_data_inclusion_ids: List[int])-> Tuple[bool, GqlResponse]:
  # Define the GraphQL query.
  query = """
  mutation($id:ID!, $firstPartyDataInclusionIds: [ID!]){
    seedUpdate(input: {
      id: $id,
      targetingData: { firstPartyDataInclusionIds: $firstPartyDataInclusionIds }
    }) {
      data {
        id
      }
      userErrors {
        field
        message
      }
    }
  }"""

  # Define the variables in the query.
  variables: dict[str, Any] = {
    'id': seed_id,
    'firstPartyDataInclusionIds': first_party_data_inclusion_ids
  }

  # Send the GraphQL request.
  return execute_gql_request(query, variables)

# This method parses the first party inclusion IDs from the `POST /v3/dmp/firstparty/advertiser` call into a list.
def parse_first_party_data(rest_response: RestResponse) -> List[int]:
  first_party_inclusion_ids = []

  first_party_data_entries = rest_response.data['Result']
  for entry in first_party_data_entries:
    first_party_inclusion_ids.append(entry["FirstPartyDataId"])

  return first_party_inclusion_ids

###############################################################################################################################################################
# Execution Flow:
#  1. Create a seed using first-party data.
#  2. If the seed is successfully created, it is set as the default seed for the advertiser.
#  3. If the seed is successfully set as the default seed for the advertiser, any updates to its name or additionally provided first-party IDs will be applied.
###############################################################################################################################################################
first_party_inclusion_ids = []
request_success, rest_response = get_first_party_data_rest(target_advertiser_id, 0, limit_first_party_ids)

if request_success:
  first_party_inclusion_ids = parse_first_party_data(rest_response)
else:
  print(rest_response.errors)
  raise Exception('Error occured while sending rest request to retrieve first party data.')

# Call the GQL `SeedCreate` mutation.
gql_request_success, gql_response = create_seed_gql(target_advertiser_id, new_seed_name, first_party_inclusion_ids)
seed_id = ''
if gql_request_success and gql_response.errors == []:
  # If the response returns a 200 and there are no `UserErrors`, then the seed was created successfully.
  seed_id = gql_response.data['seedCreate']['data']['id']
  print(f'Successfully created the seed with id {seed_id}')
else:
  print(gql_response.errors)
  raise Exception('Error occurred while sending GQL request to create the seed.')

# Call the GQL `AdvertiserSetDefaultSeed` mutation.
gql_request_success, gql_response = set_advertiser_default_seed_gql(target_advertiser_id, seed_id)
if gql_request_success and gql_response.errors == []:
  # If the response returns a 200 and there are no `UserErrors`, then the new advertiser default seed was successfully set.
  print(gql_response.data)
  print("Successfully applied seed as the default!")
else:
  print(gql_response.errors)
  raise Exception('Error occurred while sending GQL request to default the seed.')

should_make_seed_update_gql_call = False
first_party_inclusion_ids = []

if alternative_first_party_ids > 0:
  # To prevent assigning duplicate IDs in the `FirstPartyDataId` property, the IDs in `limit_first_party_ids` are used as the starting index.
  # If `alternative_first_party_ids` was set, then `POST /v3/dmp/firstparty/advertiser` will be called to query another set of first-party IDs and will replace the current IDs in `FirstPartyDataId`.
  request_success, rest_response = get_first_party_data_rest(target_advertiser_id, limit_first_party_ids + 1, alternative_first_party_ids)

  if request_success:
    first_party_inclusion_ids = parse_first_party_data(rest_response)
    should_make_seed_update_gql_call = True
  else:
    print(rest_response.errors)
    raise Exception('Error occured while sending rest request to retrieve first party data.')

if should_make_seed_update_gql_call:
  # Call the GQL `SeedUpdate` mutation.
  gql_request_success, gql_response = update_seed_gql(seed_id, first_party_inclusion_ids)
  if gql_request_success and gql_response.errors == []:
    # If the default seed was set and there are no `UserErrors`, then the seed was updated successfully.
    print(gql_response.data)
    print("Successfully updated the seed!")
  else:
    print(gql_response.errors)
    raise Exception('Error occurred while sending GQL request to default the seed.')