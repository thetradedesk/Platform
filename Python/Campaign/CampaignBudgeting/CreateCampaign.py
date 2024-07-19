import requests
import json

# Specify required IDs
advertiser_ID = 'ADVERTISER_ID_PLACEHOLDER'
seed_ID = 'SEED_ID_PLACEHOLDER'  

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
    'TTD-Auth': 'PLACEHOLDER_AUTH_TOKEN'
}
ROOT_URL_REST = 'https://ext-api.sb.thetradedesk.com/v3/'  # Use the SB environment roots

# POST /campaign Endpoint
constructed_url = ROOT_URL_REST + 'campaign'
response = requests.post(constructed_url, headers=headers, json=campaign_creation_body)

# If call was unsuccessful, output the error
if not response.ok:
    print(f"Request failed with status code: {response.status_code}")
    print(response.text)
else:
    response_data = json.loads(response.content)

    newCampaignId = response_data['CampaignId']  # Output new campaignID
    newCampaignVersion = response_data['Version']  # Output new campaignVersion -> "Kokai" indicates Kokai Campaign

    print('New Campaign ID: ' + newCampaignId)
    print('New Campaign Version: ' + newCampaignVersion)
    print(response_data)
    #
    #  API Campaign Creation Successful!
    #
