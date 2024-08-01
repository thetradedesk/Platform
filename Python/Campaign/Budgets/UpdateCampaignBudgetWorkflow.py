#############################################################################
# This script checks the campaign version and updates its budget accordingly.
#############################################################################

from datetime import datetime
from datetime import timezone
from enum import Enum
import json
import requests
from typing import Any, List, Tuple

###########
# Constants
###########

# Define the GQL Platform API endpoint URLs.
EXTERNAL_SB_GQL_URL = 'https://ext-api.sb.thetradedesk.com/graphql'
PROD_GQL_URL = 'https://desk.thetradedesk.com/graphql'

# Define the REST Platform API endpoint URLs.
EXTERNAL_SB_REST_URL = 'https://ext-api.sb.thetradedesk.com/v3'
PROD_REST_URL = 'https://api.thetradedesk.com/v3'

# Represents the REST operation to execute.
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

# Replace the placeholder value with the ID of the campaign you want to update.
campaign_id = 'CAMPAIGN_ID_PLACEHOLDER'

# Define the new budget amount (in your advertiser's currency) for the campaign.
campaign_budget = 2000

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

# Executes a REST request to the specified `rest_url` using the provided body definition and associated variables.
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

# Encapsulates the budgeting data for a campaign.
class CampaignBudgetMetadata:
  def __init__(self, version: str, flight_id: int) -> None:
    # Check if the version of the budget is set to Kokai.
    self.is_kokai: bool = version == "Kokai"
    # The ID of the current flight for the campaign.
    self.current_flight_id: int = flight_id

# Gets the campaign's budgeting data for subsequent processing decisions.
def get_campaign_budgeting_metadata(campaign_id: str) -> CampaignBudgetMetadata:
  url = rest_url + '/campaign/' + campaign_id
  # Send the REST request.
  request_success, response = execute_rest_request(RestOperation.GET, url)

  if not request_success:
    print(response.errors)
    raise Exception("Could not retrieve campaign's budget settings.")

  version = ''
  if 'BudgetingVersion' in response.data.keys():
    version = response.data['BudgetingVersion']

  # Retrieve the current campaign flight.
  now = datetime.now(timezone.utc)
  current_flight = None
  current_flight_id = None
  for flight in response.data['CampaignFlights']:
    start_time = datetime.fromisoformat(flight['StartDateInclusiveUTC']).astimezone(timezone.utc)
    end_date = None
    if flight['EndDateExclusiveUTC'] is not None:
      end_date = datetime.fromisoformat(flight['EndDateExclusiveUTC']).astimezone(timezone.utc)
    if start_time < now and (end_date is None or end_date > now):
      current_flight = flight
      current_flight_id = flight['CampaignFlightId']
      break

  if current_flight is None:
    raise Exception('Campaign has no active flight to modify.')

  return CampaignBudgetMetadata(version, current_flight_id)

# Distribute the solimar budget. In this example, the budget is split evenly among all ad groups in the campaign.
def distribute_solimar_budget(campaign_id: str, budget_in_advertiser_currency: float, current_flight_id: int) -> None:
  # Defines the payload for the `PUT /campaignflight` endpoint.
  flight_body = {
    'CampaignFlightId': current_flight_id,
    'BudgetInAdvertiserCurrency': budget_in_advertiser_currency
  }

  flight_url = rest_url + '/campaignflight'

  # Update the campaign flight budget.
  request_success, flight_response = execute_rest_request(RestOperation.PUT, flight_url, flight_body)

  if not request_success:
    print(flight_response.errors)
    raise Exception('Failed to update campaign flight.')

  # Defines the payload for the `POST /adgroup/query/campaign` endpoint.
  adgroup_body = {
    'CampaignId': campaign_id,
    'PageSize': 10000,
    'PageStartIndex': 0
  }

  adgroup_url = rest_url + '/adgroup/query/campaign'

  # Distribute ad group flight budgets.
  request_success, adgroup_response = execute_rest_request(RestOperation.POST, adgroup_url, adgroup_body)

  if not request_success:
    print(adgroup_response.errors)
    raise Exception('Failed to ')

  updated_ad_groups = []
  for ad_group in adgroup_response.data['Result']:
    ad_group_id = ad_group['AdGroupId']
    updated_ad_groups.append(ad_group_id)
    body = {
      'AdGroupId': ad_group_id,
      'RTBAttributes': {
        'BudgetSettings': {
          'AdGroupFlights': [{
            'CampaignFlightId': current_flight_id,
            # For fluid Solimar budgets, we set the ad group budgets as equal to the campaign budget.
            'BudgetInAdvertiserCurrency': budget_in_advertiser_currency
          }]
        }
      }
    }

    url = rest_url + '/adgroup'

    # Update the ad group budget.
    request_success, response = execute_rest_request(RestOperation.PUT, url, body)

    if not request_success:
      print(response.errors)
      print(f'Failed to update budget for ad group ID: {ad_group_id}')

    print(f"The ad groups updated with a budget of {budget_in_advertiser_currency} each: {updated_ad_groups}")

# Distribute the Kokai budget. This is done automatically based on the ad group rankings. If set to `True`, indicates that the budget was set succesfully.
def distribute_kokai_budget(campaign_id: str, budget_in_advertiser_currency: float, current_flight_id: int) -> bool:
  # Define the GraphQL query.
  query = """
  mutation UpdateKokaiBudgetSettings($campaignId: ID!, $currentFlightId: Long!, $budget: Decimal!) {
    campaignBudgetSettingsUpdate(input: { campaignId : $campaignId,
      campaignFlights : [{
        campaignFlightId:  $currentFlightId,
        budgetInAdvertiserCurrency: $budget
      }]
    })
    {
      data {
        wasBudgetUpdated
      }
      userErrors {
        field
        message
      }
    }
  }
  """

  # Define the variables in the query.
  variables: dict[str, Any] = {
    'campaignId': campaign_id,
    'currentFlightId': current_flight_id,
    'budget': budget_in_advertiser_currency
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if not request_success:
    print(response.data)
    raise Exception("Could not modify campaign's budget.")

  return response.data['wasBudgetUpdated']

#############################################################################################################################################################################################################
# Execution Flow:
#  1. Retrieve the campaign via REST.
#  2. Use REST to determine whether the campaign version is Kokai or Solimar.
#  3. To create a fully fluid budget for your campaign, set the total campaign budget (in this example, we set it to $2,000) and, depending on the campaign version, use GraphQL or REST to do the following:
#    - For a Solimar campaign, set the budget for all ad groups to match the campaign budget. For example, all ad groups would have a budget of $2,000.
#    - For a Kokai campaign, distribute the campaign budget across the current campaign flight and ad groups.
#############################################################################################################################################################################################################

budget_data = get_campaign_budgeting_metadata(campaign_id)

if not budget_data.is_kokai:
    distribute_solimar_budget(campaign_id, campaign_budget, budget_data.current_flight_id)
    print('Budget set for Solimar ad groups.')
else:
    if distribute_kokai_budget(campaign_id, campaign_budget, budget_data.current_flight_id):
      print('Budget distributed for Kokai ad groups.')
    else:
      raise Exception("Failed to update the campaign budget to Kokai.")