###################################################################
# This script calls GraphQL to create 3 Kokai clones of a Campaign.
###################################################################

import json
import requests
import time
from typing import Any, List, Tuple

###########
# Constants
###########

ext_sb_gql_url = 'https://ext-api.sb.thetradedesk.com/graphql'
prod_gql_url = 'https://desk.thetradedesk.com/graphql'

#############################
# Variables for YOU to define
#############################

# Define the GraphQL Platform API endpoint URL this script will use.
gql_url = ext_sb_gql_url

# Replace the placeholder value with your actual API token.
token = 'AUTH_TOKEN_PLACEHOLDER'

# The ID of the campaign to clone.
source_campaign_id = 'SOURCE_CAMPAIGN_ID_PLACEHOLDER'

# Provide the names of the clones for the campaign. A clone will be made for each name.
clone_names = ['CLONE_NAME_PLACEHOLDER_1', 'CLONE_NAME_PLACEHOLDER_2', 'CLONE_NAME_PLACEHOLDER_3']

# The maximum amount of time to wait for a clone job to complete (in seconds).
max_completion_time_seconds = 60 * 10

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
  # Create headers with the authorization token
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

# Clones a Campaign for each given clone name. Returns the ID of the clone job that is initiated.
def clone_campaign(campaign_id: str, clones_names: List[str]) -> int:
  # Define the GraphQL query.
  query = """
  mutation CloneCampaign($campaignId: String!, $numberOfClones: Int!, $cloneNames: [String!]!) {
      campaignClonesCreate(input: {
        campaignCloneData: [
          {
            campaignId: $campaignId
            numberOfClones: $numberOfClones
            cloneNames: $cloneNames
          }
        ]
      })
      {
        data {
          id
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
      'numberOfClones': len(clones_names),
      'cloneNames': clones_names
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if request_success:
    try:
      clone_job_id = response.data['campaignClonesCreate']['data'][0]['id']
      print(f"Campaign ID '{source_campaign_id}' was submitted for cloning!")
      return clone_job_id
    except:
      print(response.errors)
      raise Exception(f"Campaign ID '{source_campaign_id}' failed to clone!")
  else:
    print(response.errors)
    raise Exception(f"Campaign ID '{source_campaign_id}' failed to clone!")

# Polls a given cloning job until it is completed and returns the IDs of the successful clones.
def poll_clone_job_until_complete(job_id: int) -> List[str]:
  # Define the GraphQL query.
  query = """
  query GetCloneCampaignProgress($jobId: Long!) {
      campaignCloneProgress(id: $jobId)
      {
        status
        jobs {
          nodes {
            status
            cloneInfo {
              campaignClone {
                id
              }
            }
          }
        }
      }
  }
  """

  # Define the variables in the query.
  variables: dict[str, Any] = {
      'jobId': job_id
  }

  completion_time = 0
  successful_clone_ids = []

  while True:
    if completion_time > max_completion_time_seconds:
      raise Exception(f'Maximum allowed completion time of {max_completion_time_seconds}s has elapsed. Aborting...')

    # Send the GraphQL request.
    request_success, response = execute_gql_request(query, variables)

    if not request_success:
      print('Polling the clone job status failed.')
      print(response.errors)
      break

    status = response.data['campaignCloneProgress']['status']

    if status == "COMPLETED":
      clones = response.data['campaignCloneProgress']['jobs']['nodes']
      for clone in clones:
        successful_clone_ids.append(clone['cloneInfo']['campaignClone']['id'])
      break
    elif status == "FAILED":
      clones = response.data['campaignCloneProgress']['jobs']['nodes']
      for clone in clones:
        if clone['status'] == "COMPLETED":
          successful_clone_ids.append(clone['cloneInfo']['campaignClone']['id'])
      print(f'The cloning job did not fully succeed. The following clones (IDs) were successfully created: {successful_clone_ids}')
      break
    elif status == "IGNORED":
      raise Exception('The cloning job was ignored. Try submitting again.')
    else:
      print(f'Job status is {status}. Waiting 10s before next poll...')

    # Wait 10s before the next poll.
    completion_time += 10
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

# Verifies that the cloned campaigns' versions are now set to Kokai and that their budgets have been upgraded.
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

  print(f'{len(verified_clones)} clones were created. Results are as follows:')
  for clone in verified_clones:
    print(f'ID: {clone.campaign_id}, isKokai: {clone.is_kokai}, isKokaiBudget: {clone.is_kokai_budget}')

#########################################################################################
# Execution Flow:
#  1. Clone a Campaign 3 times in one request.
#  2. Poll the job until complete and retrieve the cloned Campaign IDs.
#  3. Validate that the campaigns and their budgets are now updated to the Kokai version.
#########################################################################################
clone_job_id = clone_campaign(source_campaign_id, clone_names)
clone_ids = poll_clone_job_until_complete(clone_job_id)
verify_cloned_campaigns(clone_ids)