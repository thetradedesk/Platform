##########################################################################################################################################################################
# This script outlines how to use GraphQL to configure and download dimension-specific performance reports for advertisers, campaigns, and ad groups from a generated URL.
###########################################################################################################################################################################

import json
import requests
from typing import Any, List, Tuple

###########
# Constants
###########

# Define the GQL Platform API endpoint URLs.
EXTERNAL_SB_GQL_URL = 'https://ext-desk.sb.thetradedesk.com/graphql'
PROD_GQL_URL = 'https://desk.thetradedesk.com/graphql'

#############################
# Variables for YOU to define
#############################

# Define the GraphQL Platform API endpoint URL this script will use.
gql_url = EXTERNAL_SB_GQL_URL

# Replace the placeholder value with your actual API token.
token = 'AUTH_TOKEN_PLACEHOLDER'

# Replace the placeholders with the IDs of the ad group, campaign, and advertiser of which you want to download the report.
# NOTE: The IDs you provide determine which mutation will be called. If more than one ID is provided, then the mutation for the entity with the highest priority will be executed, in the following order: adgroup > campaign > advertiser.
target_adgroup_id = "ADGROUP_ID_PLACEHOLDER"
target_campaign_id = "CAMPAIGN_ID_PLACEHOLDER"
target_advertiser_id = "ADVERTISER_ID_PLACEHOLDER"

# Replace the placeholder with the type of report you want to download. Here's what you need to know:
# - For possible values, use the GraphQL enum notation for the report types, for example `AD_FORMAT`.
# - If the `target_{entity}_id` value is provided and the `input_report_type` value is not provided, the script will set the `input_report_type` value to `AD_GROUP`.
input_report_type = "INPUT_REPORT_TYPE_PLACEHOLDER"


################
# Helper Methods
################

# Represents a response from the GQL server.
class GqlResponse:
    def __init__(self, data: dict[Any, Any], errors: List[Any]) -> None:
        # This is where return data from the GQL operation is stored.
        self.data = data
        # This is where any errors from the GQL operation are stored.
        self.errors = errors

# Executes a GQL request to the specified gql_url, using the provided body definition and associated variables.
# This indicates if the call was successful and returns the `GqlResponse`.
def execute_gql_request(body, variables) -> Tuple[bool, GqlResponse]:
    # Create headers with the authorization token.
    headers: dict[str, str] = {
        'TTD-Auth': token
    }

    # Create a dictionary for the GraphQL request.
    data: dict[str, Any] = {
        'query': body,
        'variables': variables
    }

    # Send the GraphQL request.
    response = requests.post(url=gql_url, json=data, headers=headers)
    content = json.loads(response.content) if len(response.content) > 0 else {}

    if not response.ok:
        print('GQL request failed!')
        # For more verbose error messaging, uncomment the following line:
        #print(response)

    # Parse any data if it exists, otherwise, return an empty dictionary.
    resp_data = content.get('data', {})
    # Parse any errors if they exist, otherwise, return an empty error list.
    errors = content.get('errors', [])

    return (response.ok, GqlResponse(resp_data, errors))

# Executes the mutation to download the report.
def execute_report( report_type: str, entity_id: str, entity_type: str ) -> Tuple[bool, GqlResponse]:
    # Determines the mutation for the report based on the entity type. For example, `adgroupReportExecute` mutation for an ad group report.
    mutation_name = ''
    report_type_enum = ''
    if entity_type == "ADGROUP":
        mutation_name = 'adGroupReportExecute'
        report_type_enum = 'AdGroupReportType'
    elif entity_type == "CAMPAIGN":
        mutation_name = "campaignReportExecute"
        report_type_enum = 'CampaignReportType'
    else:
        mutation_name = "advertiserReportExecute"
        report_type_enum = 'AdvertiserReportType'

    # Define the GraphQL query.
    query = f"""
        mutation( $entityId: ID!, $reportType: {report_type_enum}! ){{
        {mutation_name}(input: {{id: $entityId, report: $reportType}}) {{
            data {{
                id
                url
                hasSampleData
            }}
            userErrors{{
                field
                message
            }}
        }}
    }}
    """

    # Define the variables in the query.
    variables = {
        "entityId": entity_id,
        "reportType": report_type
    }
    print(variables)
    # Send the GraphQL request.
    return execute_gql_request(query, variables)

# Parses the metadata query response into a `ReportMetadata` object to use in the report execution mutation.
def parse_metadata_query_response(response: Any)-> str:
    report_type = response['programmaticTileReportMetadata']['data'][0]['type']
    return report_type

#########################################################################
# Execution Flow:
# 1. Check which IDs were provided to match the mutation being called.
# 2. Make the GraphQL calls, and verify that they were successful.
#########################################################################
entity_id = ''
report_type = ''
entity_type = ''

# Check to see which mutation to call.
if target_advertiser_id != "" and target_advertiser_id != "ADVERTISER_ID_PLACEHOLDER":
    entity_id = target_advertiser_id
    entity_type = "ADVERTISER"
if target_campaign_id != "" and target_campaign_id != "CAMPAIGN_ID_PLACEHOLDER":
    entity_id = target_campaign_id
    entity_type = "CAMPAIGN"
if target_adgroup_id != "" and target_adgroup_id != "ADGROUP_ID_PLACEHOLDER":
    entity_id = target_adgroup_id
    entity_type = "ADGROUP"

# If the input report type wasn't specified, defaults to downloading an ad group report for the specified entity.
if input_report_type == "" and input_report_type == "INPUT_REPORT_TYPE_PLACEHOLDER":
    report_type = "AD_GROUP"
else:
    report_type = input_report_type   

if entity_id == '' or entity_type == '':
    raise Exception('You must provide an entity ID.')

# Make the GraphQL call to download the report.
request_success, response = execute_report(report_type,entity_id, entity_type)
if not request_success:
    print(response.errors)
    raise Exception('Could not execute the report.')
else:
    print("Success executing the report.")
    print(response.data)