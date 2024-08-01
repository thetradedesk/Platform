##############################################################
# This script calls REST API to create a Kokai campaign.
##############################################################
import requests
import pandas as pd
import json
import time
from enum import Enum
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

# Replace the placeholder with the ID of the advertiser you want to associate with the new campaign.
advertiser_id = 'ADVERTISER_ID_PLACEHOLDER'

# The Seed to assign to the campaign. If none is provided, defaults to the Advertiser's default Seed. One of these two must exist or be provided.
seed_id = 'SEED_ID_PLACEHOLDER'

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


# Creates a new kokai Campaign and returns the ID.
def create_kokai_campaign(advertiser_ID, seed_ID):
    # Defines the payload for calling `POST /campaign`.
    body = {
        "AdvertiserId": advertiser_ID,
        "CampaignName": "New Kokai API Test Campaign",
        "Version": "Kokai",  # Indicates that this is a Kokai Campaign
        "Budget": {
            "Amount": 1200000,
            "CurrencyCode": "USD"
        },
        "StartDate": "2024-11-01T00:00:00",
        "EndDate": "2024-12-31T23:59:00",
        "PacingMode": "PaceAhead",
        "CampaignConversionReportingColumns": [],
        "PrimaryGoal": {
            "MaximizeReach": True
        },
        "PrimaryChannel": "Video",
        "IncludeDefaultsFromAdvertiser": True,
        "SeedId": seed_ID  # Seed is required since there was no default seed ID on the campaign's Advertiser
    }

    url = rest_url + '/campaign'

    # Send the REST request.
    request_success, response = execute_rest_request(RestOperation.POST, url, body)

    if request_success:
      try:
        newCampaignId = response.data['CampaignId']  # Output new campaignID
        newCampaignVersion = response.data['Version']  # Output new campaignVersion -> "Kokai" indicates Kokai Campaign
        print()
        budget = response.data['Budget']['Amount'] # Output that verifies it has a budget

        print('New Campaign ID: ' + newCampaignId)
        print('New Campaign Version: ' + newCampaignVersion)
        print('Campaign budget amount: '+  str(budget))
        print(response.data)

        return newCampaignId
      except:
        print(response.errors)
        raise Exception(f"Campaign failed to create!")
    else:
      print(response.errors)
      raise Exception(f"Campaign failed to create!")

# Creates a new ad group and returns the ID.
def create_and_associate_adgroup(campaign_id):

    # Defines the payload for calling `POST /adgroup`.
    body = {
        "CampaignId":campaign_id,
        "AdGroupName":"Strategy 1",
        "IndustryCategoryId":292,
        "AdGroupCategory":{
            "CategoryId":8311
        },
        "IsEnabled": True,
        "PredictiveClearingEnabled":True,
        "FunnelLocation": "Awareness",
        "RTBAttributes":{
            "ROIGoal":{
                "CPAInAdvertiserCurrency":{
                    "Amount":0.2,
                    "CurrencyCode":"USD"
                }
            },
            "AudienceTargeting":{
                "CrossDeviceVendorListForAudience":[
                    {
                    "CrossDeviceVendorId":11,
                    "CrossDeviceVendorName":"Identity Alliance"
                    }
                ]
            },
            "BaseBidCPM":{
                "Amount":1.0,
                "CurrencyCode":"USD"
            },
            "MaxBidCPM":{
                "Amount":5.0,
                "CurrencyCode":"USD"
            },
            "CreativeIds":[
            ]
            #"AssociatedBidLists":[] If needed, add IDs of bid lists you want to associate with the ad group in the `AssociatedBidLists` array in the following format: `[ { "BidListId" : "id1" }, { "BidListId" : "id2" } , { "BidListId" : "id3" } ]`.
        }
    }

    url = rest_url + '/adgroup'
    
    # Send the REST request.
    request_success, response = execute_rest_request(RestOperation.POST, url, body)

    if request_success:
      try:
        newAdGroupId = response.data['AdGroupId']  # Output the new ad group ID.
        isEnabled = response.data['IsEnabled']

        print('New ad group ID: ' + newAdGroupId)
        print("This ad group is now " + str(isEnabled))
        return newAdGroupId
      except:
        print(response.errors)
        raise Exception(f"Ad Group failed to create!")
    else:
      print(response.errors)
      raise Exception(f"Ad Group failed to create!")

# Retrieves the new campaign and returns its version along with its budgeting version.
def get_campaign(campaign_id):
    url = rest_url + '/campaign/' + campaign_id

    # Send the REST request.
    request_success, response = execute_rest_request(RestOperation.GET, url, None)

    if request_success:
      try:
        # Since the budgeting version is returned if it's a Kokai campaign.
        budgetingVersion = ''
        try:
            budgetingVersion = response.data['BudgetingVersion']
        except Exception:
            budgetingVersion = 'Solimar'

        version = response.data['Version']

        return (budgetingVersion, version)
      except:
        print(response.errors)
        raise Exception(f"Campaign failed to be retrieved!")
    else:
      print(response.errors)
      raise Exception(f"Campaign failed to be retrieved!")

#########################################################################################
# Execution Flow:
#  1. Creates new Kokai campaign.
#  2. Creates and associates ad groups with the newly created campaign.
#  3. Validates that the new campaign and its budgeting version are set to `Kokai`.
#########################################################################################
campaign_id = create_kokai_campaign(advertiser_id, seed_id)
create_and_associate_adgroup(campaign_id)

budgetingVersion, version =  get_campaign(campaign_id)
print("Here is the budgeting version of the campaign: " + budgetingVersion)
print("Here is the version of the campaign: " + version)