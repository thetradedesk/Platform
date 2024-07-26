import requests
import pandas as pd
import json
import time

ROOT_URL_GQL = "https://ext-api.sb.thetradedesk.com/graphql"
ROOT_URL_REST = 'https://ext-api.sb.thetradedesk.com/v3'  # Use the SB environment roots
TTD_AUTH = ''



def create_kokai_campaign(advertiser_ID, seed_ID):
    # Create Campaign Body
    campaign_creation_body = {
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

    # Set up headers
    headers = {
        'Content-Type': 'application/json',
        'TTD-Auth': TTD_AUTH
    }

    # POST /campaign Endpoint
    constructed_url = ROOT_URL_REST + 'campaign'
    response = requests.post(constructed_url, headers=headers, json=campaign_creation_body)

    # If call was unsuccessful, output the error
    if not response.ok:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        exit
    else:
        response_data = json.loads(response.content)

        newCampaignId = response_data['CampaignId']  # Output new campaignID
        newCampaignVersion = response_data['Version']  # Output new campaignVersion -> "Kokai" indicates Kokai Campaign
        print()
        budget = response_data['Budget']['Amount'] # Output that verifies it has a budget


        print('New Campaign ID: ' + newCampaignId)
        print('New Campaign Version: ' + newCampaignVersion)
        print('Campaign budget amount: '+  str(budget))
        print(response_data)

        return newCampaignId

        #
        #  API Campaign Creation Successful!
        #

def create_and_associate_adgroup(campaign_id):

    # Create ad group body
    adgroup_creation_body = {
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
        "BudgetSettings": {
            "DailyBudget": {
                "Amount": 1,
                "CurrencyCode": "USD"
            },
            "PacingMode": "PaceToEndOfDay"
        },
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
        #"AssociatedBidLists":[] Associate bidlists here if needed
    }
    }

    # Set up headers
    headers = {
        'Content-Type': 'application/json',
        'TTD-Auth': TTD_AUTH
    }

    # POST /campaign Endpoint
    constructed_url = ROOT_URL_REST + 'adgroup'
    response = requests.post(constructed_url, headers=headers, json=adgroup_creation_body)



    # If call was unsuccessful, output the error
    if not response.ok:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        exit
    else:
        response_data = json.loads(response.content)
        newAdGroupId = response_data['AdGroupId']  # Output new ad group ID
        isEnabled = response_data['IsEnabled']

        print('New ad group ID: ' + newAdGroupId)
        print("This ad group is now " + str(isEnabled))
        return newAdGroupId


def get_campaign(campaign_id):

    # Set up headers
    headers = {
        'Content-Type': 'application/json',
        'TTD-Auth': TTD_AUTH
    }

    # POST /campaign Endpoint
    constructed_url = ROOT_URL_REST + 'campaign' + '/' + campaign_id
    response = requests.get(constructed_url, headers=headers)

    # If call was unsuccessful, output the error
    if not response.ok:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        exit
    else:
        response_data = json.loads(response.content)

        # Since Budgeting Version is only returned if it's Kokai
        budgetingVersion = ''
        try:
            budgetingVersion = response_data['BudgetingVersion']
        except Exception:
            budgetingVersion = 'Solimar'

        version = response_data['Version']

        return (budgetingVersion, version)

def start_workflow():

    # Specify required IDs to first create the Campaign
    advertiser_ID = ''
    seed_ID = ''

    #Creates campaign
    campaign_id = create_kokai_campaign(advertiser_ID, seed_ID)

    #Creates and associates ad groups with campaign id
    create_and_associate_adgroup(campaign_id)

    #Validates
    budgetingVersion, version =  get_campaign(campaign_id)
    print("Here is the budgeting version of the campaign: " + budgetingVersion)
    print("Here is the version of the campaign: " + version)

start_workflow()