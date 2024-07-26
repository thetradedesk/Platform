###################################################################
# This script calls REST to get the name and version of a Campaign.
###################################################################

from enum import Enum
import json
import requests
from typing import Any, List, Tuple

###########
# Constants
###########

# Define the REST Platform API endpoint URLs.
EXTERNAL_SB_REST_URL = 'https://ext-desk.sb.thetradedesk.com/v3'
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
rest_url = EXTERNAL_SB_REST_URL

# Replace the placeholder value with your actual API token.
token = 'AUTH_TOKEN_PLACEHOLDER'

# The headers to pass as part of the REST requests.
rest_headers = {
  "TTD-Auth": token,
  "Content-Type": "application/json"
}

# Replace the placeholder with the ID of the campaign you want to query.
target_campaign_id = 'TARGET_CAMPAIGN_ID_PLACEHOLDER'

################
# Helper Methods
################

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

# Queries a given campaign by ID and prints the result.
def query_campaign(campaign_id: str) -> None:
  # GET /campaign/{campaignId} Endpoint
  url = rest_url + '/campaign/' + campaign_id

  # Send the REST request.
  request_success, response = execute_rest_request(RestOperation.GET, url, None)

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