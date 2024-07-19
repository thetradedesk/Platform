import requests
import pandas as pd
import json
import time

ROOT_URL_GQL = "https://api.dev.gen.adsrvr.org/graphql"
ROOT_URL_REST = 'https://int-api.sb.thetradedesk.com/v3/'  # Use the SB environment roots
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
        "StartDate": "2024-07-01T00:00:00",
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
        budget = response_data['Budget']['Amount'] # Output that verifies it has a budget

        
        print('New Campaign ID: ' + newCampaignId)
        print('New Campaign Version: ' + newCampaignVersion)
        print('Campaign budget amount: ' + str(budget))
        print(response_data)

        return newCampaignId

        #
        #  API Campaign Creation Successful!
        #

def create_and_associate_adgroup(campaign_id):
   
    # Create AdGroup Body
    adgroup_creation_body = {
    "CampaignId":campaign_id,
    "AdGroupName":"Strategy 1",
    "IndustryCategoryId":292,
    "IsEnabled": True,
    "AdGroupCategory":{
        "CategoryId":8311
    },
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
        newAdGroupId = response_data['AdGroupId']  # Output new adGroupId
        isEnabled = response_data['IsEnabled']

        print("This AdGroup is now " + str(isEnabled))
        print('New AdGroup ID: ' + newAdGroupId)
        return newAdGroupId

        #
        #  API AdGroup Creation Successful!
        #


def make_graphql_call(mutation):
    url = ROOT_URL_GQL
    headers = {
        'Content-Type': 'application/json',
        'TTD-Auth': TTD_AUTH
        }
    return requests.post(url, json={'query': mutation}, headers=headers,verify=False)


def call_campaign_mutation(mutation):
    try:
        print("Attempting to make the GQL call.")
        response = make_graphql_call(mutation)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            with open('failed_campaign_mutations.txt', 'a') as file:
                file.write(mutation + '\n')
            print("Failed to make campaign mutation GraphQL call since code returned wasn't 200:", response.text)
            exit
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print("Failed to make GraphQL call as unknown error occured:", response.text)
        exit


def construct_get_migration_budget_campaign_mutation(campaign_id):
    return f"""
    query campaign {{
        campaign(id: "{campaign_id}") {{
            id
            budgetMigrationStatus(targetBudgetingVersion: KOKAI) {{
                migrationData {{
                    campaignFlights {{
                    adGroupFlights {{
                        adGroupId
                        budgetInImpressions
                        campaignFlightId
                        dailyTargetInAdvertiserCurrency
                        dailyTargetInImpressions
                        minimumSpendInAdvertiserCurrency
                    }}
                    originalCampaignFlight {{
                        id
                    }}
                    }}
                }}
                }}
            
        }}
    }}
    """

def parse_campaign_migration_budget(migration_response):

    # So we need two important pieces of information under the MigrationData field from the campaign query, the original
    # campaign flight id and the adgroup flights under that id
    campaign_id = ""
    campaign_flight_to_adgroup_flight_map = {}
    try:
        campaign_id = migration_response["data"]["campaign"]["id"]
        for campaign_flight in migration_response["data"]["campaign"]["budgetMigrationStatus"]["migrationData"]["campaignFlights"]:
            campaign_flight_to_adgroup_flight_map[campaign_flight["originalCampaignFlight"]["id"]] = campaign_flight["adGroupFlights"]
        print(json.dumps(campaign_flight_to_adgroup_flight_map, indent=4))
        return campaign_flight_to_adgroup_flight_map
    except Exception as e:
        print(f"An unexpected error occurred while parsing the migration budget query response: {e}")
        exit



def construct_upgrade_to_kokai_budget_mutation(campaign_id, campaign_flight_to_adgroup_flight_map):

    # Need to pass in the adgroup flights into the input
    campaign_flights = []

    # Go through the dictionary and construct the campaignFlights input field
    for campaign_flight_id, adgroup_flights in campaign_flight_to_adgroup_flight_map.items():
        adgroup_flights_formatted = [
            "{" +
            f'adGroupId: "{row["adGroupId"]}"' +
            (f', budgetInImpressions: {row["budgetInImpressions"]}' if pd.notna(row["budgetInImpressions"]) else "") +
            (f', campaignFlightId: {row["campaignFlightId"]}' if pd.notna(row["campaignFlightId"]) else "") +
            (f', dailyTargetInAdvertiserCurrency: {row["dailyTargetInAdvertiserCurrency"]}' if pd.notna(row["dailyTargetInAdvertiserCurrency"]) else "") +
            (f', dailyTargetInImpressions: {row["dailyTargetInImpressions"]}' if pd.notna(row["dailyTargetInImpressions"]) else "") +
            (f', minimumSpendInAdvertiserCurrency: {row["minimumSpendInAdvertiserCurrency"]}' if pd.notna(row["minimumSpendInAdvertiserCurrency"]) else "") +
            "}"
            for row in adgroup_flights
            ]
        adgroup_flights_formatted = ', '.join(adgroup_flights_formatted)

        # Create a formatted string for this part of the GraphQL query
        graphql_part = f"""
        {{
            campaignFlightId: {campaign_flight_id}
            adGroupFlights: [{adgroup_flights_formatted}]
        }}
        """

        campaign_flights.append(graphql_part)

    # Combine all parts into a single query string
    graphql_field = "\n".join(campaign_flights)
   

    return f"""
    mutation {{
        campaignBudgetSettingsUpdate(
            input: {{
            campaignId: "{campaign_id}"
            budgetingVersion: KOKAI
            campaignFlights: [{graphql_field}]
            }}
        ) {{
            data {{
                campaign {{
                    pacingMode
                    flights {{
                        edges {{
                            node {{
                                budgetInAdvertiserCurrency
                                dailyTargetInAdvertiserCurrency
                                startDateInclusiveUTC
                                endDateExclusiveUTC
                                id
                                adGroupFlights {{
                                    edges {{
                                        node {{
                                            adGroupId
                                            dailyTargetInAdvertiserCurrency
                                            minimumSpendInAdvertiserCurrency
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
            userErrors{{
                message
            }}
        }}
    }}
    """


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

        budgetingVersion = response_data['BudgetingVersion'] 
        version = response_data['Version']  

        return (budgetingVersion, version)
    
def start_workflow():

    # Specify required IDs to first create the Campaign
    advertiser_ID = ''
    seed_ID = ''  

    #Creates campaign
    campaign_id = create_kokai_campaign(advertiser_ID, seed_ID)

    #Creates and associates adgroups with campaign id
    create_and_associate_adgroup(campaign_id)

    #Creates and calls the query to get the relevent campaign budget migration data
    campaign_query = construct_get_migration_budget_campaign_mutation(campaign_id)
    print("Here is the constructed query:")
    print(campaign_query)
    returned_data = call_campaign_mutation(campaign_query)
    campaign_flight_to_adgroup_flight_map = parse_campaign_migration_budget(returned_data)

    #Creates and calls the mutation to set the budget setting to Kokai
    campaign_mutation = construct_upgrade_to_kokai_budget_mutation(campaign_id, campaign_flight_to_adgroup_flight_map)
    print("Here is the constructed campaign mutation:")
    print(campaign_mutation)

    returned_data = call_campaign_mutation(campaign_mutation)

    print("Here is the response")
    print(json.dumps(returned_data, indent=4))

    #Validates
    budgetingVersion, version =  get_campaign(campaign_id)
    print("Here is the budgeting version of the campaign: " + budgetingVersion)
    print("Here is the version of the campaign: " + version)

start_workflow()