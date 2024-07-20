# This script will use `LastChangeTrackingVersion` to retrieve all the budgets of the affected adgroups under an advertiser.
# 1. Get changed adGroups via Delta REST with the `LastChangeTrackingVersion`.
# 2. Get budgets of adgroups, and seperated them between kokai budgeted and solimar budgeted.


import requests
import json
from datetime import datetime
from datetime import timezone

# Define the REST Platform API endpoint URLs.
sandbox_rest_url = "https://ext-api.sb.thetradedesk.com/v3"

# Define the GraphQL Platform API endpoint URLs.
sandbox_graphql_url = 'https://ext-desk.sb.thetradedesk.com/graphql'

# Define the authentication token
token = 'AUTHORIZATION_TOKEN_PLACEHOLDER'

# Advertiser to the delta queries on.
advertiser_id = 'ADVERTISER_ID_PLACEHOLDER'

# The last change tracking version. If there is no last change tracking version, leave the value as `None`.
# If `None`, we will identify the latest one and place it here instead. 
last_change_tracking_version = None


# Set up headers.
headers = {
    'Content-Type': 'application/json',
    'TTD-Auth': token
}

# Use this method to send and handle REST calls.
def query_rest(http_method: str, url: str, body: object = None):
    constructed_url = sandbox_rest_url + url

    response = requests.request(http_method, url=constructed_url, headers=headers, json=body)

    # If the call was unsuccessful, output the error.
    if not response.ok:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        raise RuntimeError(f"Failed request {constructed_url}")
    
    return json.loads(response.content)

# Use this method to send and handle GQL calls.
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

# This calls POST /delta/adgroup/query/advertiser to retrieve delta information for the advertiser.
def run_delta_query(advertiser_id, change_tracking_version):
    delta_rest_response = query_rest(http_method='POST', url='/delta/adgroup/query/advertiser', body={
        "AdvertiserId": advertiser_id,
        "LastChangeTrackingVersion": change_tracking_version,
        "IncludeTemplates": False,
        "ReturnEntireAdGroup": False
    })
    return delta_rest_response

# This calls POST /delta/adgroup/query/advertiserto handle cases when noLastChangeTrackingVersion value is provided.
# This enables us to retrieve the LastChangeTrackingVersion value for the first time.
def run_delta_query_first_time(advertiser_id):
    delta_rest_response = run_delta_query(advertiser_id, None)

    # This updates the last change tracking version.
    new_last_change_tracking_version = delta_rest_response["LastChangeTrackingVersion"]

    return new_last_change_tracking_version

# This calls POST /delta/adgroup/query/advertiserto handle cases when noLastChangeTrackingVersion value is provided.
# This enables us to retrieve ad groups.
def run_delta_query_get_all(advertiser_id, change_tracking_version):

    delta_rest_response = run_delta_query(advertiser_id, change_tracking_version)
    new_change_tracking_version = delta_rest_response["LastChangeTrackingVersion"]

    # This returns the ad group IDs and the new change tracking version.
    return delta_rest_response["ElementIds"], new_change_tracking_version

# A GQL query to retrieve a paginated list of ad group budgets.
def get_budget_with_campaign_version(ad_groups, cursor):
    # IMPORTANT: Be sure to use double quotes (") instead of single quotes (') for strings.
    ad_groups = format(ad_groups).replace("'",'"')

    # Construct the GraphQL query dynamically based on cursor availability.
    after_clause = f'after: "{cursor}",' if cursor else ''

    query = f"""
    query {{ 
        adGroups({after_clause} where: 
        {{ id:
            {{ in: 
                {ad_groups}
            }}
        }}
        ) {{
            nodes {{
                id
                budget {{
                    currentFlightBudget
                }}
                campaign{{
                    budgetMigrationStatus {{
                        currentBudgetingVersion
                    }}
                }}
                
            }}
            pageInfo{{
                hasNextPage
                endCursor
            }}
        }}
    }}"""
    print(query)
    return query_graphql(query)

    
    
# If the `last_change_tracking_version` value hasn't been set,  we'll run an initial delta REST call to set it.
if(last_change_tracking_version is None):
    last_change_tracking_version = run_delta_query_first_time(advertiser_id)

# We need to save the `last_change_tracking_version` value for the next run. This enables us to track any changes since the last run.
ad_groups, last_change_tracking_version = run_delta_query_get_all(advertiser_id, last_change_tracking_version)

print("Here are the adgroups returned by the delta query:")
print(ad_groups)

# This is the start of the paginated GraphQL query for retrieving ad group budgets.
# It is called until there are no more pages left.
cursor = None
has_more_pages = True
kokai_adgroup_results = []
solimar_adgroup_results = []

# While there are more pages to query, keep making calls.
while has_more_pages:
    graphql_result = get_budget_with_campaign_version(ad_groups, cursor)

    has_more_pages = graphql_result["adGroups"]["pageInfo"]["hasNextPage"]
    
    cursor = graphql_result["adGroups"]["pageInfo"]["endCursor"]

    for ad_group in graphql_result["adGroups"]["nodes"]:
        version = ad_group["campaign"]["budgetMigrationStatus"]["currentBudgetingVersion"]
        ad_group_id = ad_group["id"]
        budget = ad_group["budget"]["currentFlightBudget"]
      
        # Check the version to verify that it is a Kokai ad group.
        if version == 'KOKAI':
            kokai_adgroup_results.append((ad_group_id, budget))
        else:
            # If it's not a Kokai ad group, it's a Solimar ad group.
            solimar_adgroup_results.append((ad_group_id, budget))

print("Here are the Kokai AdGroup budgets:")
print(kokai_adgroup_results)

print("Here are the Solimar AdGroup budgets:")
print(solimar_adgroup_results)