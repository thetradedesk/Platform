##############################################################
# This script calls REST API to create 3 clones of a Campaign.
##############################################################

from enum import Enum
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

# If the campaign you're cloning is Solimar, should its clones be upgraded to Kokai?
upgrade_solimar_to_kokai = True

# The ID of the campaign to clone.
source_campaign_id = 'SOURCE_CAMPAIGN_ID_PLACEHOLDER'

# Provide names for the newly cloned campaigns. A separate campaign copy will be created for each clone name.
clone_names = ['CLONE_NAME_PLACEHOLDER_1', 'CLONE_NAME_PLACEHOLDER_2', 'CLONE_NAME_PLACEHOLDER_3']

# The maximum amount of time to wait for a clone job to complete (in seconds).
max_completion_time_seconds = 60 * 10

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

# Clones a Campaign for each given clone name. Returns the ID of the clone job that is initiated.
def clone_campaign(campaign_id: str, clones_names: List[str]) -> List[int]:
  clone_job_ids: List[int] = []

  for clone_name in clones_names:
    # Defines the payload for calling POST `/campaign/clone`.
    body = {
      'CampaignId': campaign_id,
      'CampaignName': clone_name
    }

    if upgrade_solimar_to_kokai:
      body['Version'] = 'Kokai'

    url = rest_url + "/campaign/clone"

    # Send the REST request.
    request_success, response = execute_rest_request(RestOperation.POST, url, body)

    if request_success:
      try:
        clone_job_id = response.data['ReferenceId']
        print(f"Campaign ID '{source_campaign_id}' was submitted for cloning!")
        clone_job_ids.append(clone_job_id)
      except:
        print(response.errors)
        raise Exception(f"Campaign ID '{source_campaign_id}' failed to clone!")
    else:
      print(response.errors)
      raise Exception(f"Campaign ID '{source_campaign_id}' failed to clone!")

  return clone_job_ids

# Polls a given cloning job until it is completed and returns the IDs of the successful clones.
def poll_clone_jobs_until_complete(job_ids: List[int]) -> List[str]:
  # Track each job's status and poll until they are complete.
  job_statuses: dict[int, str] = {}
  for job_id in job_ids:
    job_statuses[job_id] = "InProgress"

  jobs_in_flight = len(job_statuses)
  completion_time = 0
  successful_clone_ids = []

  while True:
    if completion_time > max_completion_time_seconds:
      raise Exception(f'Maximum allowed completion time of {max_completion_time_seconds}s has elapsed. Aborting...')

    # Iterate over each job in the list until they've all been processed.
    jobs_in_flight = len(job_statuses)
    for job_id, job_status in job_statuses.items():
      # If the job has been processed, no need to keep polling it.
      if job_status != "InProgress":
        jobs_in_flight -= 1
        continue

      url = rest_url + f'/campaign/clone/status/{job_id}'

      # Send the REST request.
      request_success, response = execute_rest_request(RestOperation.GET, url, None)

      if not request_success:
        print('Polling the clone job status failed.')
        print(response.errors)
        break

      # Update the job status and add successful jobs to the the list of job successes.
      status = response.data['Status']
      job_statuses[job_id] = status

      if status == 'Completed':
        successful_clone_ids.append(response.data['CampaignId'])
        jobs_in_flight -= 1
        continue
      elif status == 'Failed':
        jobs_in_flight -= 1
        print(f'Cloning job {job_id} did not succeed.')
        continue

    # Wait 10 seconds before the next poll, or exit if completed.
    if jobs_in_flight <= 0:
      break
    completion_time += 10
    print(f'Job status is {status}. Waiting 10s before next poll...')
    time.sleep(10)

  return successful_clone_ids

# Represents a clone in its Kokai verion.
class VerifiedClone:
  def __init__(self, id: str, version: str, budgetVersion: str):
    # The ID of the cloned campaign.
    self.campaign_id: str = id
    # Check if the version of the cloned campaign is set to Kokai.
    self.is_kokai: bool = version == "KOKAI"
    # Check if the budget version of the cloned campaign is set to Kokai.
    self.is_kokai_budget: bool = budgetVersion == "KOKAI"

# Verifies that the cloned campaigns' versions are now set to Kokai and that their budgets have been upgraded (if elected).
def verify_cloned_campaigns(cloned_campaign_ids: List[str]) -> None:
  # Define the GraphQL query.
  query = """
  query VerifyCloneCampaignsAreKokai($campaignIds: [String!]!) {
    campaigns(where: { id: { in: $campaignIds } })
    {
      nodes {
        id
        version
        budgetMigrationStatus {
          currentBudgetingVersion
        }
      }
    }
  }
  """

  # Define the variables in the query.
  variables: dict[str, Any] = {
    'campaignIds': cloned_campaign_ids
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  verified_clones: List[VerifiedClone] = []

  if request_success:
    for campaign_clone in response.data['campaigns']['nodes']:
      verified_clones.append(VerifiedClone(campaign_clone['id'], campaign_clone['version'], campaign_clone['budgetMigrationStatus']['currentBudgetingVersion']))
  else:
    print(response.errors)
    raise Exception('Could not verify clone states.')

  print(f"{len(verified_clones)} clones were created. Cloned Campaigns were{' ' if upgrade_solimar_to_kokai else ' NOT'} elected to be upgraded. Results are as follows:")
  for clone in verified_clones:
    print(f'ID: {clone.campaign_id}, isKokai: {clone.is_kokai}, isKokaiBudget: {clone.is_kokai_budget}')

#########################################################################################
# Execution Flow:
#  1. Clone a Campaign 3 times in one request.
#  2. Poll the job until complete and retrieve the cloned Campaign IDs.
#  3. Validate that the campaigns and their budgets are now updated to the Kokai version.
#########################################################################################
clone_job_ids = clone_campaign(source_campaign_id, clone_names)
clone_ids = poll_clone_jobs_until_complete(clone_job_ids)
verify_cloned_campaigns(clone_ids)