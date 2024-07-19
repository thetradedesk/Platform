# This script checks the campaign version and updates its budget accordingly.
#1. Get a campaign via REST.
#2. Determine whether its version is Kokai or Solimar via REST.
#3. Update the campaign budget: set the total campaign budget to $2,000 and do the following either via GraphQL or REST, depending on the campaign version:
# - For the Solimar campaign, set the budget for all ad groups to $2000 (fully fluid).
# - For the Kokai campaign, distribute the $2000 budget across the current campaign flight ad groups (fully fluid).


import requests
import json
from datetime import datetime
from datetime import timezone

# Define the REST Platform API endpoint URLs.
sandbox_rest_url = "https://ext-api.sb.thetradedesk.com/v3"

# Define the GraphQL Platform API endpoint URLs.
sandbox_graphql_url = 'https://ext-api.sb.thetradedesk.com/graphql'

# Define the authentication token.
token = 'AUTH_TOKEN_PLACEHOLDER'

# Specify the ID of the campaign to update.
campaign_id = 'CAMPAIGN_ID_PLACEHOLDER'

# New budget for the campaign.
campaign_budget = 2000


# Set up headers.
headers = {
    'Content-Type': 'application/json',
    'TTD-Auth': token
}


def query_rest(http_method: str, url: str, body: object = None):
    constructed_url = sandbox_rest_url + url
    response = requests.request(http_method, url=constructed_url, headers=headers, json=body)

    # If call was unsuccessful, output the error.
    if not response.ok:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        raise RuntimeError(f"Failed request {constructed_url}")

    return json.loads(response.content)

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


def get_campaign_budgeting(campaign_ID):
    campaign_data = query_rest(http_method='GET', url='/campaign/' + campaign_ID)

    is_kokai = False
    if "BudgetingVersion" in campaign_data.keys() and campaign_data["BudgetingVersion"] == "Kokai":
        is_kokai = True

    # Get the current flight.
    now = datetime.now(timezone.utc)
    current_flight = None
    current_flight_id = None
    for flight in campaign_data['CampaignFlights']:
        start_time = datetime.fromisoformat(flight["StartDateInclusiveUTC"]).astimezone(timezone.utc)
        end_date = None
        if flight["EndDateExclusiveUTC"] is not None:
            end_date = datetime.fromisoformat(flight["EndDateExclusiveUTC"]).astimezone(timezone.utc)
        if start_time < now and (end_date is None or end_date > now):
            current_flight = flight
            current_flight_id = flight["CampaignFlightId"]
            break

    if current_flight is None:
        raise RuntimeError("No active flight to modify")

    return is_kokai, current_flight_id

# Distribute the solimar budget.  We do that in this example by splitting the budget evenly among
# all ad groups in the campaign.
def distribute_solimar_budget(campaign_id, budget_in_advertiser_currency, current_flight_id):
    flight = {
        "CampaignFlightId" : current_flight_id,
        "BudgetInAdvertiserCurrency" : budget_in_advertiser_currency
    }

    # Update the campaign flight budget.
    flightResponse = query_rest(http_method='PUT', url="/campaignflight", body=flight)

    # Distribute ad group flight budgets.
    ad_groups = query_rest(http_method='POST', url='/adgroup/query/campaign', body={
        "CampaignId" : campaign_id,
        "PageSize": 10000,
        "PageStartIndex": 0
    })

    updated_ad_groups = []
    for ad_group in ad_groups['Result']:
        ad_group_id = ad_group["AdGroupId"]
        updated_ad_groups.append(ad_group_id)
        query_rest(http_method='PUT', url='/adgroup', body={
            'AdGroupId' : ad_group_id,
            'RTBAttributes' : {
                'BudgetSettings' : {
                    'AdGroupFlights' : [
                        {
                            'CampaignFlightId': current_flight_id,
                            # For fluid Solimar budgets, we set the ad group budgets as equal to the campaign budget.
                            'BudgetInAdvertiserCurrency': budget_in_advertiser_currency
                        }
                    ]
                }
            }
        })

    print(f"The ad groups updated with a budget of {budget_in_advertiser_currency} each: {updated_ad_groups}")

# Distribute the Kokai budget. This is done automatically based on the ad group rankings.
def distribute_kokai_budget(campaign_id, budget_in_advertiser_currency, current_flight_id):
    query = f"""mutation {{
    campaignBudgetSettingsUpdate(input: {{ campaignId : "{campaign_id}",
        campaignFlights : [{{
            campaignFlightId: {current_flight_id},
            budgetInAdvertiserCurrency: {budget_in_advertiser_currency}
        }}]
     }})
     {{
        data {{
            wasBudgetUpdated
        }}
        userErrors {{
            field
            message
        }}
    }}
}}
    """
    query_graphql(query)

is_kokai, campaign = get_campaign_budgeting(campaign_id)

if not is_kokai:
    distribute_solimar_budget(campaign_id, campaign_budget, campaign)
else:
    distribute_kokai_budget(campaign_id, campaign_budget, campaign)

print(is_kokai)