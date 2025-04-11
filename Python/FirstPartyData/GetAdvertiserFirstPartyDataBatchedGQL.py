######################################################################
# This script calls GQL to get the first party data for an advertiser.
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

# Replace the placeholder with the ID of the advertiser you want to query first party data for.
advertiser_id = 'ADVERTISER_ID_PLACEHOLDER'

# Replace the placerholder with a name filter you'd like to filter the return set on.
name_filter = 'NAME_PLACEHOLDER'

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


# Schedules a query job to retrieve the advertiser's first party data. This returns the ID for the created job.
def create_advertiser_first_party_data_job() -> str:
  query = f'''query {{
    advertiser(id: "{advertiser_id}") {{
      firstPartyData(where: {{ name: {{ contains: "{name_filter}" }} }}) {{
        nodes {{
            name
            id
            activeUniques {{
              householdCount
              idsConnectedTvCount
              idsCount
              idsInAppCount
              idsWebCount
              personsCount
            }}
        }}
      }}
    }}
  }}'''

  jobQuery = f'''
  mutation CreateAdvertiserFirstPartyDataBulkQuery {{
    createQueryBulk(
      input: {{
        query: """{query}"""
      }}
    ) {{
      errors {{
        ... on InSchemaError {{
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
    raise Exception('Failed to schedule 1PD retrieval job.')

  try:
    id = response.data['createQueryBulk']['data']['id']

    if not id:
      print(response.errors)
      raise('Could not create query job.')
  except:
    print(response.errors)
    raise('Could not create query job.')

  return id

# Queries a given Advertiser's first party data and prints the result file URL.
def query_advertiser_first_party_data() -> None:
  job_id = create_advertiser_first_party_data_job()
  status_query = f"""query GetBulkJobStatus {{
    bulkJob(id: "{job_id}") {{
        id
        status
        url
        gqlErrors
    }}
  }}"""
  should_poll = True

  print('Waiting on data retrieval job...')

  while should_poll:
    time.sleep(5)

    # Check the job state.
    request_success, response = execute_gql_request(status_query, {})

    if not request_success:
      print(response.errors)
      raise Exception('Failed to query 1PD retrieval job.')

    status = response.data['bulkJob']['status']
    should_poll = status == 'QUEUED' or status == 'IN_PROGRESS'

    # If the job completed:
    #   - In the case of success, print the URL.
    #   _ In the case of failure, print the errors.
    if not should_poll:
      url = response.data['bulkJob']['url']

      if not url:
        print('Query job failed with errors:')
        print(response.data['bulkJob']['gqlErrors'])
      else:
        print(f'Data can be accessed at: {url}')

      return


###########################################################
# Execution Flow:
#  1. Query the advertiser ID specified for first party data.
#  2. Wait on the query job to finish.
#  3. Output the URL to the file containing the results.
###########################################################
query_advertiser_first_party_data()