import requests
import json

campaign_id = 'NEW_CAMPAIGN_ID'  # Use new Campaign ID that was created via CreateCampaign.py

# Set up headers
headers = {
    'Content-Type': 'application/json',
    'TTD-Auth': 'AUTH_TOKEN_PLACEHOLDER'
}
ROOT_URL_REST = 'https://ext-api.sb.thetradedesk.com/v3/'  # Use the SB environment roots

# GET /campaign/{campaignId} Endpoint
constructed_url = ROOT_URL_REST + 'campaign/' + campaign_id
response = requests.get(constructed_url, headers=headers)

# If call was unsuccessful, output the error
if not response.ok:
    print(f"Request failed with status code: {response.status_code}")
    print(response.text)
else:
    response_data = json.loads(response.content)

    receivedCampaignVersion = response_data['Version']  # Output new campaignVersion -> "Kokai" indicates Kokai Campaign

    print('New Campaign Version: ' + receivedCampaignVersion)
    print(response_data)
    #
    #  API Campaign Load Successful!
    #
