######################################################################
# This script calls GQL to get the third-party data for a 
# partner, handling pagination at the third-party data level.
######################################################################

import time
import json
import requests
from typing import Any, List, Tuple

###########
# Constants
###########

# Define the GQL Platform API endpoint URLs.
EXTERNAL_SB_GQL_URL = "https://ext-desk.sb.thetradedesk.com/graphql"
PROD_GQL_URL = "https://desk.thetradedesk.com/graphql"


#############################
# Variables for YOU to define
#############################

# Define the GraphQL Platform API endpoint URL this script will use.
gql_url = EXTERNAL_SB_GQL_URL

# Replace the placeholder value with your actual API token.
token = "REPLACE_WITH_API_TOKEN"

# Replace the placeholder with the ID of the partner you want to query advertisers for.
partner_id = "REPLACE_WITH_PARTNER_ID"

# This is the size of each individual request. We partition the total provider id output. EX: 154 providers is equal to 4 requests.
partition_total_call_size = 30

# This is the total size of each aliased query in a request. Complexity limits allow us to only have up to 4 aliases. Adjust to fit within the threshold.
partition_gql_alias_query_size = 10

# Replace the placeholder with the size(count) of the page size you"d like to return on a single query request.
third_party_data_count = 1000  # Number of third-party data entries per page

# Maximum number of retried for a single request.
max_retries = 3

total_third_party_data_list = []

################
# Helper Methods
################

# Represents a response from the GQL server.
class GqlResponse:
    def __init__(self, data: dict[Any, Any], errors: List[Any]) -> None:
        # This is where the return data from the GQL operation is stored.
        self.data = data
        # This is where any errors from the GQL operation are stored.
        self.errors = errors

# Executes a GQL request to the specified gql_url, using the provided body definition and associated variables.
# This indicates if the call was successful and returns the `GqlResponse`.
def execute_gql_request(body, variables) -> Tuple[bool, GqlResponse]:
    # Create headers with the authorization token.
    headers: dict[str, str] = {
        "TTD-Auth": token
    }

    # Create a dictionary for the GraphQL request.
    data: dict[str, Any] = {
        "query": body,
        "variables": variables
    }

    # Send the GraphQL request.
    response = requests.post(url=gql_url, json=data, headers=headers)
    content = json.loads(response.content) if len(response.content) > 0 else {}

    if not response.ok:
        print("GQL request failed!")
        # For more verbose error messaging, uncomment the following line:
        # print(response.content)

    # Parse any data if it exists, otherwise, return an empty dictionary.
    resp_data = content.get("data", {})
    # Parse any errors if they exist, otherwise, return an empty error list.
    errors = content.get("errors", [])

    return (response.ok, GqlResponse(resp_data, errors))

def execute_with_retries(query, variables, partner_id, max_retries=max_retries):
    """
    Executes a GraphQL query with retry logic for fetching partner data.
    
    Parameters:
        query (str): The GraphQL query string.
        variables (dict): The variables for the GraphQL query.
        partner_id (str): The ID of the partner for debugging purposes.
        max_retries (int): The maximum number of retry attempts.

    Returns:
        dict: The `partner` data if successful.

    Raises:
        Exception: If the GraphQL request fails after all retries.
    """
    try_count = 0
    while try_count <= max_retries:
        # Execute the GraphQL request
        request_success, response = execute_gql_request(query, variables)

        if request_success:
            try:
                partner_data = response.data.get("partner", {})
                return partner_data  # Return the data if found
            except AttributeError:
                print("Failed to retrieve 'partner' data. Retrying...")

        else:
            print(f"GraphQL request failed: {response.errors}")

        # Increment the retry count
        try_count += 1

        # If retries exceed the limit, raise an exception
        if try_count > max_retries:
            raise Exception(f"Failed to fetch data for partner {partner_id} after {max_retries} retries.")

        print(f"Retry attempt {try_count}/{max_retries} for partner {partner_id}.")
        print(query)

    # If we exit the loop without returning, raise an exception
    raise Exception(f"Unexpected error occurred for partner {partner_id}.")


# Retrieves all third party data providers a user has access to.
def get_user_third_party_data_providers() -> Any:
    query = """
    query GetThirdPartyDataProviders($partnerId: ID!) {
        partner(id: $partnerId) {
            thirdPartyDataProviders {
                nodes {
                    id
                }
            }
        }
    }"""

    variables = {"partnerId": partner_id}

    print(f"\nRetrieving all provider IDs for: {partner_id}")

    # Send the GraphQL request.
    request_success, response = execute_gql_request(query, variables)

    if not request_success:
        print(response.errors)
        raise Exception("Failed to fetch providers.")

    # Extract provider IDs and deduplicate them
    nodes = response.data["partner"]["thirdPartyDataProviders"]["nodes"]
    provider_ids = {
        node["id"]
        for node in nodes
        if node.get("id")  # Check if thirdPartyDataproviderId exists
    }

    print(f"Total partner provider IDs: {len(provider_ids)}")

    return {
        "providerIds": list(provider_ids)
    }


# # Creates a partitioned list of provider ids.
def partition_list(strings, partition_size):
    """
    Partitions an array of strings into a list of string arrays, based on the provided partition size.

    :param strings: List of strings to be partitioned
    :param partition_size: Size of each partition
    :return: List of partitioned string arrays
    """
    if partition_size <= 0:
        raise ValueError("Partition size must be greater than 0.")
    return [strings[i:i + partition_size] for i in range(0, len(strings), partition_size)]


def get_partner_third_party_data(partner_id, third_party_data_count, partitioned_provider_list):
    """
    Fetch all third-party data for a partner, dynamically handling pagination for multiple provider groups.
    """
    third_party_data_list = []  # Final flat list of all third-party data
    alias_has_more_map = {f"mygroup_{idx + 1}_alias": True for idx in range(len(partitioned_provider_list))}
    alias_cursor_map = {alias: None for alias in alias_has_more_map}  # Initialize cursors for each alias

    while any(alias_has_more_map.values()):  # Continue until no alias has more data
        # Construct the GraphQL query dynamically for aliases with data remaining
        active_aliases = [alias for alias, has_more in alias_has_more_map.items() if has_more]

        if not active_aliases:  # Break if no aliases have more data
            break

        query = """
        query GetPartnerThirdPartyData($partnerId: ID!, $thirdPartyDataCount: Int) {
          partner(id: $partnerId) {
        """
        for idx, alias in enumerate(active_aliases):
            group_index = int(alias.split("_")[1]) - 1
            provider_group = partitioned_provider_list[group_index]
            provider_id_filter = json.dumps(provider_group)  # Serialize the provider group for GraphQL
            after_cursor = json.dumps(alias_cursor_map[alias])  # Current cursor for the alias

            query += f"""     {alias}: thirdPartyData(
                first: $thirdPartyDataCount,
                after: {after_cursor},
                where: {{
                    provider: {{ id: {{ in: {provider_id_filter} }} }}
                }}
            ) {{
                nodes {{
                    id
                    name
                    idBrandId
                    providerId
                    providerElementId
                    description
                    buyable
                    fullPath
                    activeUniques {{
                        idsCount
                        householdCount
                        idsConnectedTvCount
                        idsInAppCount
                        idsWebCount
                        personsCount
                        thirdPartyDataOverlapCount
                        lastUpdated
                    }}
                }}
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }}\n"""
            
        query += "  }\n}"
        
        # Execute the GraphQL query
        variables = {"partnerId": partner_id, "thirdPartyDataCount": third_party_data_count}
        
        partner_data = execute_with_retries(query, variables, partner_id)


        # Process each alias's data from the response
        for alias in active_aliases:
            alias_data = partner_data.get(alias, {})
            nodes = alias_data.get("nodes", [])
            page_info = alias_data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            end_cursor = page_info.get("endCursor", None)

            # Append nodes to the result list
            third_party_data_list.extend(nodes)

            # Update alias state for pagination
            alias_has_more_map[alias] = has_next_page
            alias_cursor_map[alias] = end_cursor

    # Append the nodes to the total list
    total_third_party_data_list.extend(third_party_data_list)
    print(f"Total third-party data retrieved: {len(third_party_data_list)}")
    

def query_partner_third_party_data() -> None:
    partner_provider_data = get_user_third_party_data_providers()
    provider_list = partner_provider_data["providerIds"]
    
    partitioned_calls_list = partition_list(provider_list, partition_total_call_size)
    print(f"\n\nTotal Requests to be made: {len(partitioned_calls_list)}\n\n")
    
    begin_start_time = time.time()
    print(f"Retrieving third party data - Start Time: {time.strftime('%H:%M:%S', time.localtime(begin_start_time))}\n\n")
    
    for index, call_list in enumerate(partitioned_calls_list):
        partitioned_provider_list = partition_list(call_list, partition_gql_alias_query_size)

        start_time = time.time()
        print(f"Request {index+1} of {len(partitioned_calls_list)}\n")
        print(f"Request {index+1} Start Time: {time.strftime('%H:%M:%S', time.localtime(start_time))}")
        get_partner_third_party_data(partner_id, third_party_data_count, partitioned_provider_list)
        end_time = time.time()
        print(f"Request {index+1} End Time: {time.strftime('%H:%M:%S', time.localtime(end_time))}\n\n")
        elapsed_time = (end_time - start_time) / 60
        print(f"Request {index+1} Elapsed time: {elapsed_time} minutes\n\n")

    finish_end_time = time.time()
    total_elapsed_time = (finish_end_time - begin_start_time) / 60
    print(f"Finished Retrieving third party data - End Time: {time.strftime('%H:%M:%S', time.localtime(finish_end_time))}\n\n")
    print(f"Total - Request Elapsed time: {total_elapsed_time} minutes")

    # Write the results to a file
    with open(f"third_party_data.json", "w") as file:
        json.dump(total_third_party_data_list, file, indent=4)

###########################################################
# Execution Flow:
#  1. Query all providers under the specified partner ID.
#  2. Partition providers based on partition group size, query their third-party data.
#  3. Handle pagination at both provider and third-party data levels.
#  4. Write out the combined result to output.txt.
###########################################################
query_partner_third_party_data()
