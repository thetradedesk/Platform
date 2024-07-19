import pandas as pd
import json
import requests
import time

def make_graphql_call(mutation):
    url = 'url goes here'
    headers = {
        'Content-Type': 'application/json',
        'TTD-Auth': 'ttd-auth gpes here'
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
            userErrors {{
            field
            message
            }}
        }}
    }}
    """


def upgrade_campaign_budget_to_kokai():

    # Campaign id to query budget settings for. The campaign id should have a solimar budget setting
    campaign_id = "campaign id goes here"
    campaign_mutation = construct_get_migration_budget_campaign_mutation(campaign_id)

    print("Here is the constructed query:")
    print(campaign_mutation)

    returned_data = call_campaign_mutation(campaign_mutation)
    campaign_flight_to_adgroup_flight_map = parse_campaign_migration_budget(returned_data)

    campaign_mutation = construct_upgrade_to_kokai_budget_mutation(campaign_id, campaign_flight_to_adgroup_flight_map)
    print("Here is the constructed camapign mutation")
    print(campaign_mutation)

    print("Here is the response")
    print(json.dumps(returned_data, indent=4))
    
upgrade_campaign_budget_to_kokai()