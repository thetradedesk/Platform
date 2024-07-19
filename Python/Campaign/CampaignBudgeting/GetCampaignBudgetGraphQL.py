import pandas as pd
import json
import requests
import time

def make_graphql_call(mutation):
    url = ''
    headers = {
        'Content-Type': 'application/json',
        'TTD-Auth': ''
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


def construct_campaign_mutation(campaign_id):
    return f"""
    query Campaign {{
        campaign(id: "{campaign_id}") {{
            budget {{
                total
            }}
            pacingMode
            timeZone
            budgetInImpressions
            flights {{
                totalCount
                edges {{
                    cursor
                    node {{
                        budgetInAdvertiserCurrency
                        budgetInImpressions
                        dailyTargetInAdvertiserCurrency
                        dailyTargetInImpressions
                        id
                        isCurrent
                        startDateInclusiveUTC
                        adGroupFlights {{
                            totalCount
                            edges {{
                                cursor
                                node {{
                                    adGroupId
                                    budgetInAdvertiserCurrency
                                    budgetInImpressions
                                    minimumSpendInAdvertiserCurrency
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}
    }}
    """

def get_campaign_budget():

    #campaign id to query budget settings for
    campaign_id = ""
    campaign_mutation = construct_campaign_mutation(campaign_id)

    print("Here is the constructed mutation:")
    print(campaign_mutation)

    returned_data = call_campaign_mutation(campaign_mutation)

    print("Here is the response")
    print(json.dumps(returned_data, indent=4))
    

get_campaign_budget()