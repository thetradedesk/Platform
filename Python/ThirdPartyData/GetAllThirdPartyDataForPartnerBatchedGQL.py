######################################################################
# This script calls GQL to get the third party data for a partner.
######################################################################

import json
import requests
import time
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
token = 'TOKEN_PLACEHOLDER'

# Replace the placeholder with the ID of the partner you want to query third party data for.
partner_id = 'PARTNER_ID_PLACEHOLDER'

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


# Retrieves all third party data providers a user has access to.
def get_user_third_party_data_provider_ids() -> set[Any]:
  query = """
  query GetThirdPartyDataProviders($partnerId: ID!) {
    partner(id: $partnerId) {
      thirdPartyDataProviders {
        nodes {
          id
        }
      }
    }
  }"""

  variables = {"partnerId": partner_id}

  print(f"\nRetrieving all provider IDs for: {partner_id}")

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if not request_success:
    print(response.errors)
    raise Exception("Failed to fetch providers.")

  # Extract provider IDs and deduplicate them
  nodes = response.data["partner"]["thirdPartyDataProviders"]["nodes"]
  provider_ids = {
      node["id"]
      for node in nodes
      if node.get("id")  # Check if thirdPartyDataproviderId exists
  }

  print(f"Total partner provider IDs: {len(provider_ids)}")

  return provider_ids

# Schedules a query job to retrieve the partner's third party data. This returns the ID for the created job.
def create_partner_third_party_data_job(provider_id: str) -> str:
  query = f'''query {{
    partner(id: "{partner_id}") {{
      thirdPartyData(where: {{ provider: {{ id: {{ eq: "{provider_id}" }} }} }}) {{
        nodes {{
          id
          name
          providerId
          providerElementId
          description
          allowCustomFullPath
          buyable
          dataAllianceExcluded
          defaultSortScore
          fullPath
          hierarchyString
          activeUniques {{
            idsCount
            householdCount
            idsConnectedTvCount
            idsInAppCount
            idsWebCount
            personsCount
            thirdPartyDataOverlapCount
            lastUpdated
          }}
        }}
      }}
    }}
  }}'''

  jobQuery = f'''
  mutation CreatePartnerThirdPartyDataBulkQuery {{
    createQueryBulk(
      input: {{
        query: """{query}"""
      }}
    ) {{
      errors {{
        ... on MutationError {{
          message
          field
        }}
        ... on BulkJobQueryValidationError {{
          message
          field
          queryErrors
        }}
      }}
      data {{
        id
      }}
    }}
  }}'''

  # Send the GraphQL request.
  request_success, response = execute_gql_request(jobQuery, {})

  if not request_success:
    print(response.errors)
    raise Exception('Failed to schedule 3PD retrieval job.')

  try:
    id = response.data['createQueryBulk']['data']['id']

    if not id:
      print(response.errors)
      raise('Could not create query job.')
  except:
    print(response.errors)
    raise('Could not create query job.')

  return id

# Queries a given Partner's third party data and prints the result file URL.
def query_partner_third_party_data() -> None:
  provider_list = get_user_third_party_data_provider_ids()
  cur_item = 0
  total_items = len(provider_list)

  for provider_id in provider_list:
    cur_item += 1
    job_id = None
    failedJobQueryCount = 0

    while job_id is None:
      try:
        job_id = create_partner_third_party_data_job(provider_id)
      except Exception as e:
        print('Error creating bulk job, retrying...')
        failedJobQueryCount += 1

      if failedJobQueryCount > 5:
        print('Failed to create job too many times. Exiting.')
        print(e)
        raise Exception('Failed to create 3PD retrieval job.')

    status_query = f"""query GetBulkJobStatus {{
      bulkJob(id: "{job_id}") {{
        id
        status
        url
        gqlErrors
      }}
    }}"""
    should_poll = True
    failedStatusCheckCount = 0

    print(f'Waiting on data retrieval job for provider {cur_item}/{total_items}...')

    while should_poll:
      time.sleep(10)

      try:
        # Check the job state.
        request_success, response = execute_gql_request(status_query, {})
        failedStatusCheckCount = 0

        if not request_success:
          print('Failed to query 3PD retrieval job. Will retry.')
          should_poll = True
          failedStatusCheckCount += 1
        else:
          status = response.data['bulkJob']['status']
          should_poll = status == 'QUEUED' or status == 'IN_PROGRESS'
      except:
        should_poll = True
        failedStatusCheckCount += 1

      if failedStatusCheckCount > 5:
        print('Failed to check job status too many times. Exiting.')
        raise Exception('Failed to query 3PD retrieval job.')

      # If the job completed:
      #   - In the case of success, print the URL.
      #   _ In the case of failure, print the errors.
      if not should_poll:
        url = response.data['bulkJob']['url']

        if not url:
          print('Query job failed with errors:')
          print(response.data['bulkJob']['gqlErrors'])
        else:
          download_output_file(url)

        break

  print('Downloaded 3PD to file tpd.jsonl')

# Downloads a given URL to a local file.
def download_output_file(url: str):
  local_filename = 'tpd.jsonl'

  # Download and parse JSON result.
  response = requests.get(url)
  response.raise_for_status()
  json_data = response.json()

  # Extract node data.
  nodes = json_data.get("data", {}) \
            .get("partner", {}) \
            .get("thirdPartyData", {}) \
            .get("nodes", [])

  # Append each node as a line.
  with open(local_filename, 'a', encoding='utf-8') as f:
    for node in nodes:
      f.write(json.dumps(node) + '\n')

###########################################################
# Execution Flow:
#  1. Query the partner ID specified for third party data.
#  2. Wait on the query job to finish.
#  3. Output the URL to the file containing the results.
###########################################################
query_partner_third_party_data()