####################################################################
# This script creates multiple Kokai campaigns via GraphQL bulk API.
####################################################################

from datetime import datetime
from datetime import timezone
from datetime import timedelta
import json
import random
import requests
import time
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

# Replace the placeholder with the ID of the advertiser you want to associate with the new campaigns.
target_advertiser_id = 'ADVERTISER_ID_PLACEHOLDER'

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

# Outlines the creation of multiple campaigns and adds them to a JSONL file.
def create_campaigns_jsonl(advertiser_id: str) -> str:
  start_date = datetime.now(timezone.utc).replace(microsecond=0, second=0, minute=0) + timedelta(hours=2)
  end_date = start_date + timedelta(days=60)

  campaigns = ''

  for i in range(1, 10):
    campaign = {
      'CampaignName': 'Test_CampaignCreate_' + str(i) + '_' + str(random.randint(0, 10000000)),
      'Advertiser' : {
        'Id': advertiser_id
      },
      'TimeZoneId': 'Utc',
      'PacingMode': 'PaceEvenly',
      'Flights': [{
        'BudgetInAdvertiserCurrency' : 1000,
        'StartDateUtc': start_date.strftime("%Y-%m-%d %H:%M:%S"),
        'EndDateUtc': end_date.strftime("%Y-%m-%d %H:%M:%S")
      }]
    }

    if i > 1:
      campaigns += '\n'

    campaigns += json.dumps(campaign)

  return campaigns

# Retrieves an upload URL and file ID used to upload the JSON file.
def request_upload() -> Tuple[str, str]:
  query = """
  mutation {
    fileUpload {
      id
      uploadUrl
    }
  }"""

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, None)

  if not request_success:
     print(response.errors)
     raise Exception('Could not create an upload URL.')

  file_id = response.data['fileUpload']['id']
  upload_url = response.data['fileUpload']['uploadUrl']
  return file_id, upload_url

# Uploads the JSONL file to the upload URL.
def upload_file(contents: str, upload_url: str) -> None:
  # Create required header for sandbox - https://partner.thetradedesk.com/v3/portal/api/doc/GqlBulkOperations#url
  if gql_url.startswith('https://ext-api.sb.thetradedesk.com'):
    headers: dict[str, str] = {"x-ms-blob-type": "BlockBlob"}
  else:
    headers: dict[str, str] = {}
  response = requests.put(url=upload_url, data=contents, headers=headers)
  if not response.ok:
    print(f"Request failed with status code: {response.status_code}")
    print(response.text)
    raise RuntimeError(f"Failed to upload data to {upload_url}")

# Kicks off a bulk job to create campaigns. Returns the ID of the job underway.
def bulk_create_campaigns(advertiser_id: str, upload_id: str) -> str:
  query = """
  mutation BulkCampaignCreation($advertiserId: ID!, $uploadId: ID!) {
    bulkCreateCampaigns(
      input: {
        advertiserId: $advertiserId,
        fileId: $uploadId
       }
     ) {
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
    'advertiserId': advertiser_id,
    'uploadId': upload_id
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if not request_success:
    print(response.errors)
    raise Exception('Bulk campaign job could not be initiated.')

  return response.data['bulkCreateCampaigns']['data']['id']

# Queries the  progress of a bulk job by its ID and returns the job status and any validation erorrs.
def query_job_progress(job_id: str) -> Tuple[str, str]:
  query = """
  query GetBulkJobProgress($jobId: ID!) {
    jobProgress(id: $jobId) {
      jobStatus
      validationErrors
    }
  }"""

  # Define the variables in the query.
  variables: dict[str, Any] = {
    'jobId': job_id
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if not request_success:
    print(response.errors)
    raise Exception('Job progress could not be queried.')

  status = response.data['jobProgress']['jobStatus']
  validationErrors = response.data['jobProgress']['validationErrors']

  return status, validationErrors

# Checks the job status until it is complete.
def monitor_job_completion(job_id: str) -> None:
  job_status = "IN_PROGRESS"
  validation_errors = None

  # Keep querying the job status until it is no longer `IN_PROGRESS`.
  while(job_status == "IN_PROGRESS"):
    print('Job still in progress. Polling again in 30s...')
    time.sleep(30)
    job_status, validation_errors = query_job_progress(job_id)

  if job_status == "ERROR":
    print("We received an internal error. Nothing we can do here but fail and try again.")
  elif job_status == "VALIDATION_FAILURE":
    print(f"We received a validation error. Invalid data in the uploaded file. {validation_errors}")
  elif job_status == "COMPLETE":
    print(f"Campaign creation succceeded.")

###################################################################################################################
# Execution Flow:
#  1. Create a string for the JSONL file. Ensure each campaign in the JSONL file includes all required fields.
#  2. Call the `fileUpload` mutation to retrieve a file ID and URL.
#  3. Upload the JSONL file to the URL returned from step 2.
#  4. Call the `bulkCreateCampaigns` mutation and include the file ID to submit the job for creating the campaigns.
#  5. Monitor the job by its ID until it is complete.
###################################################################################################################

campaigns = create_campaigns_jsonl(target_advertiser_id)
file_id, upload_url = request_upload()
upload_file(campaigns, upload_url)
job_id = bulk_create_campaigns(target_advertiser_id, file_id)
result = monitor_job_completion(job_id)