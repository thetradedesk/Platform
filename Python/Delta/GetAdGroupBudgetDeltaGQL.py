#############################################################################################################################
# This script will use `LastChangeTrackingVersion` to retrieve all the budgets of the affected ad groups under an advertiser.
#############################################################################################################################

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

# Advertiser to the delta queries on.
target_advertiser_id = 'ADVERTISER_ID_PLACEHOLDER'

# The last change tracking version. If there is no last change tracking version, leave the value as `None`.
# If set to `None`, the script will update this value to the latest change tracking version.
last_change_tracking_version = None

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

# This calls POST /delta/adgroup/query/advertiser to retrieve delta information for the advertiser.
def run_delta_query(advertiser_id: str, change_tracking_version: int) -> Any:
  body = {
    'AdvertiserId': advertiser_id,
    'LastChangeTrackingVersion': change_tracking_version,
    'IncludeTemplates': False,
    'ReturnEntireAdGroup': False
  }

  url = rest_url + '/delta/adgroup/query/advertiser'

  # Send the REST request.
  request_success, response = execute_rest_request(RestOperation.POST, url, body)

  if not request_success:
   print(response.errors)
   raise Exception(f'Failed calling ad group delta for advertiser {advertiser_id}')

  return response.data

# This calls POST /delta/adgroup/query/advertiser to handle cases when no `LastChangeTrackingVersion` value is provided.
# This enables us to retrieve the `LastChangeTrackingVersion` value for the first time.
# Returns the new change tracking version number.
def run_delta_query_first_time(advertiser_id: str) -> int:
  delta_rest_response = run_delta_query(advertiser_id, None)

  # This updates the last change tracking version.
  new_last_change_tracking_version = delta_rest_response['LastChangeTrackingVersion']

  return new_last_change_tracking_version

# This calls POST /delta/adgroup/query/advertiser to handle cases when no LastChangeTrackingVersion value is provided.
# This enables us to retrieve ad groups.
# Returns the new change tracking version number and a list of element IDs returned from the previous delta endpoint call.
def run_delta_query_get_all(advertiser_id: str, change_tracking_version: int) -> Tuple[List[str], int]:
  delta_rest_response = run_delta_query(advertiser_id, change_tracking_version)
  new_change_tracking_version = delta_rest_response['LastChangeTrackingVersion']

  # This returns the ad group IDs and the new change tracking version.
  return delta_rest_response['ElementIds'], new_change_tracking_version

# A GQL query to retrieve a paginated list of ad group budgets.
def get_budget_with_campaign_version(ad_groups: List[str], cursor: str) -> Any:
  # IMPORTANT: Be sure to use double quotes (") instead of single quotes (') for strings.
  ad_groups = format(ad_groups).replace("'",'"')

  # Construct the GraphQL query dynamically based on cursor availability.
  after_clause = f'after: "{cursor}",' if cursor else ''

  query = f"""
  query {{
    adGroups({after_clause}
      where: {{ id: {{ in: {ad_groups} }} }}
    ) {{
      nodes {{
        id
        budget {{
          currentFlightBudget
        }}
        campaign{{
          budgetMigrationStatus {{
            currentBudgetingVersion
          }}
        }}
      }}
      pageInfo{{
        hasNextPage
        endCursor
      }}
    }}
  }}"""

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query,{})

  if not request_success:
    print(response.errors)
    raise Exception('Failed to retrieve ad group budgets.')

  return response.data

########################################################################################################################
# Execution Flow:
#  1. Retrieve updated ad groups with one of the Delta REST endpoints and include the `LastChangeTrackingVersion` value.
#  2. Retrieve ad group budgets, and separate them between Kokai and Solimar budget versions.
########################################################################################################################

# If the `last_change_tracking_version` value hasn't been set,  we'll run an initial delta REST call to set it.
if(last_change_tracking_version is None):
  last_change_tracking_version = run_delta_query_first_time(target_advertiser_id)

# We need to save the `last_change_tracking_version` value for the next run. This enables us to track any changes since the last run.
ad_groups, last_change_tracking_version = run_delta_query_get_all(target_advertiser_id, last_change_tracking_version)

print('Here are the ad group IDs returned by the delta query:')
print(ad_groups)

# This is the start of the paginated GraphQL query for retrieving ad group budgets.
# It is called until there are no more pages left.
cursor = None
has_more_pages = True
kokai_adgroup_results = []
solimar_adgroup_results = []

# While there are more pages to query, keep making calls.
while has_more_pages:
  graphql_result = get_budget_with_campaign_version(ad_groups, cursor)

  has_more_pages = graphql_result['adGroups']['pageInfo']['hasNextPage']

  cursor = graphql_result['adGroups']['pageInfo']['endCursor']

  for ad_group in graphql_result['adGroups']['nodes']:
    version = ad_group['campaign']['budgetMigrationStatus']['currentBudgetingVersion']
    ad_group_id = ad_group['id']
    budget = ad_group['budget']['currentFlightBudget']

    # Check the version to verify that it is a Kokai ad group.
    if version == 'KOKAI':
      kokai_adgroup_results.append((ad_group_id, budget))
    else:
      # If it's not a Kokai ad group, it's a Solimar ad group.
      solimar_adgroup_results.append((ad_group_id, budget))

print('Here are the Kokai ad group budgets:')
print(kokai_adgroup_results)

print('Here are the Solimar ad group budgets:')
print(solimar_adgroup_results)