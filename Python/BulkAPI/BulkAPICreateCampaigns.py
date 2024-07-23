##############################################################
# This script creates multiple Kokai campaigns via GraphQL bulk API.
##############################################################

import requests
import json
import random
from datetime import datetime
from datetime import timezone
from datetime import timedelta
import time


# Define the ID of the advertiser to use for the campaigns.
advertiser_id = 'ADVERTISER_ID_PLACEHOLDER'

# Define the GraphQL Platform API endpoint URLs.
sandbox_graphql_url = 'https://ext-api.sb.thetradedesk.com/graphql'

# Replace the placeholder value with your actual API token.
token = 'AUTH_TOKEN_PLACEHOLDER'

# Create headers with the authorization token.
headers = {
    'TTD-Auth': token
}


def query_graphql(query):
    response = requests.post(sandbox_graphql_url, headers=headers, json={'query':query})

    if not response.ok:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        raise RuntimeError(f"Failed request {sandbox_graphql_url}")

    content = json.loads(response.content)
    if "userErrors" in content.keys():
        print(f"Request had errors running the mutation")

    return content["data"]

# We create multiple campaigns and add them to a JSONL file.
def create_campaigns_jsonl():
    start_date = datetime.now(timezone.utc).replace(microsecond=0, second=0, minute=0) + timedelta(hours=2)
    end_date = start_date + timedelta(days=60)

    campaigns = ""

    for i in range(1, 10):
        campaign = { 
            "CampaignName": "Test_CampaignCreate_" + str(i) + "_" + str(random.randint(0, 10000000)),
            "Advertiser" : {"Id":advertiser_id},
            "TimeZoneId": "Utc",
            "PacingMode": "PaceEvenly",
            "Flights": [
                {
                    "BudgetInAdvertiserCurrency" : 1000,
                    "StartDateUtc": start_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "EndDateUtc": end_date.strftime("%Y-%m-%d %H:%M:%S")
                }
            ]
        }
        if i > 1:
            campaigns += "\n"
        campaigns += json.dumps(campaign)

    return campaigns

# 
def request_upload():
    file_upload_mutation = """mutation {
    fileUpload {
        id
        uploadUrl
    }
    }"""
    response = query_graphql(file_upload_mutation)
    file_id = response['fileUpload']['id']
    upload_url = response['fileUpload']['uploadUrl']
    return file_id, upload_url

def upload_file(contents, upload_url):
    response = requests.put(url=upload_url, data=contents)
    if not response.ok:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        raise RuntimeError(f"Failed to upload data to {upload_url}")
    
def bulk_create_campaigns(upload_id):
    response = query_graphql(f"""mutation {{
        bulkCreateCampaigns(input:{{advertiserId:"{advertiser_id}", fileId:"{upload_id}"}}) {{
            data {{
                id
            }}
            userErrors {{
                field
                message
            }}
        }}
    }}""")
    return response["bulkCreateCampaigns"]["data"]["id"]

def query_job_progress(job_id):
    response = query_graphql(f"""query{{
    jobProgress(id:"{job_id}") {{
        jobStatus
        validationErrors
    }}
}}""")
    status = response["jobProgress"]["jobStatus"]
    validationErrors = response["jobProgress"]["validationErrors"]

    return status, validationErrors

def monitor_job_completion(job_id):
    job_status = "IN_PROGRESS"
    validation_errors = None
    # Keep querying the job status until it is no longer `IN_PROGRESS`.
    while(job_status == "IN_PROGRESS"):
        "Need to make sure that we wait for a bit before making another request."
        time.sleep(30)
        job_status, validation_errors = query_job_progress(job_id)

    if job_status == "ERROR":
        print("We received an internal error.  Nothing we can do here but fail and try again.")

    elif job_status == "VALIDATION_FAILURE": 
        print(f"We received a validation error.  Invalid data in the uploaded file. {validation_errors}")

    elif job_status == "COMPLETE":
        print(f"Campaign creation succceeded")
    
# 1. Create a string for the JSONL file. Ensure each campaign in the JSONL file includes all required fields.
campaigns = create_campaigns_jsonl()

# 2. Call the `fileUpload` mutation to retrieve a file ID and URL.
file_id, upload_url = request_upload()

# 3. Upload the JSONL file to the URL returned from step 2. 
upload_file(campaigns, upload_url)

# 4. Call the `bulkCreateCampaigns` mutation and include the file ID to submit the job for creating the campaigns.
job_id = bulk_create_campaigns(file_id)

# 5. Monitor the job by its ID until it is complete.
result = monitor_job_completion(job_id)