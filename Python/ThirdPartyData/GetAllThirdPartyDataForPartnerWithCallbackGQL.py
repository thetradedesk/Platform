##################################################################################
# This script calls GQL to get the third party data for a partner using callbacks.
##################################################################################

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

# Replace the placeholder with the URL of your calbback POST endpoint.
callback_url = 'CALLBACK_URL_PLACEHOLDER'

# Replace with any headers you would like to send with the callback request.
callback_headers = {
  'HEADER_1_PLACEHOLDER': 'VALUE_1_PLACEHOLDER',
  'HEADER_2_PLACEHOLDER': 'VALUE_2_PLACEHOLDER'
}

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

# Schedules a query job to retrieve the partner's third party data with a callback URL to be hit when the job completes.
def create_partner_third_party_data_job_with_callback() -> None:
  query = f'''query {{
    partner(id: "{partner_id}") {{
      thirdPartyData {{
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

  formatted_headers = ', '.join(f'{{ key: "{k}", value: "{v}" }}' for k, v in callback_headers.items())

  jobQuery = f'''
  mutation CreatePartnerThirdPartyDataBulkQuery {{
    createQueryBulk(
      input: {{
        query: """{query}"""
        bulkJobCallback: {{
          callbackUrl: "{callback_url}"
          callbackHeaders: [
            {formatted_headers}
          ]
        }}
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


# Given parsed callback data, retrieves the job data.
def retrieve_job_results(callback_result: Any) -> None:
  status = callback_result['status']

  if status != 'Success' and status != 'PartialSuccess':
    raise Exception(f'No data to retrieve. Job ended in state {status}')

  status_query = f"""query GetBulkJobStatus {{
    bulkJob(id: "{callback_result['jobId']}") {{
      url
      gqlErrors
    }}
  }}"""

  # Get the job URL.
  request_success, response = execute_gql_request(status_query, {})

  if not request_success:
    raise Exception('Failed to query 3PD retrieval job.')

  url = response.data['bulkJob']['url']

  if not url:
    print('Query job failed with errors:')
    print(response.data['bulkJob']['gqlErrors'])
  else:
    download_output_file(url)

  print('Downloaded 3PD to file tpd_callback.jsonl')

# Downloads a given URL to a local file.
def download_output_file(url: str):
  local_filename = 'tpd_callback.jsonl'

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
#  2. Wait for a callback.
#  3. Output the URL to the file containing the results.
###########################################################
create_partner_third_party_data_job_with_callback()

# Stub: Callback server waiting for callback.
# Below, we process a callback response the server would expect to receive.

# Replace with your server. Sample data for now.
callback_result = {
  'jobId': 123,
  'status': 'Success',
  'completedAtUtc': '2025-04-25T22:02:24.34567'
}

retrieve_job_results(callback_result)