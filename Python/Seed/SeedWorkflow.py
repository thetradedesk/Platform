import requests
from typing import Any, List, Tuple
import json

# -----------------------------------------------------------------------------------------------
# Variable Definitions
# -----------------------------------------------------------------------------------------------

# URL of where to direct rest endpoints to.
REST_URL = "https://ext-api.sb.thetradedesk.com/v3"
# URL if where to direct GQL requests to.
GRAPHQL_URL = "https://ext-api.sb.thetradedesk.com/graphql"
AUTH_TOKEN = "AUTH_TOKEN_PLACEHOLDER"
HEADERS = {
    "TTD-Auth": AUTH_TOKEN,
    "Content-Type": "application/json"
}

# Advertiser that you'd like to create the seed under.
ADVERTISER_ID = 'ADVERTISER_ID_PLACEHOLDER'

# Set the seed's name during creation.
SEED_NAME = 'SEED_NAME_PLACEHOLDER'

# Set the limit on the number of IDs to use for seed creation. This will affect page size when making REST calls to retrieve seed creation data.
LIMIT_FIRST_PARTY_IDS = 3

# Alternative first-party data IDs to set after the seed has been created. 
# The POST `/v3/dmp/firstparty/advertiser` endpoint will be called to retrieve the IDs.
# NOTE: This replaces the first-party data IDs that already exist on the seed.
# The GQL `SeedUpdate` mutation will be called to update the seed.
# If set to 0, no ids will be updated.
ALTERNATIVE_FIRST_PARTY_IDS = 1

# Also included with the `SeedUpdate`, this allows changing the seed name.
CHANGE_SEED_NAME = 'SEED_NAME_PLACEHOLDER'

# -----------------------------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------------------------

# Represents a response from the GQL server.
class GqlResponse:
    def __init__(self, data: Any, errors: List[Any]):
        # This is where return data from the GQL operation is stored.
        self.data = data
        # This is where any errors from the GQL operation are stored.
        self.errors = errors

class RestResponse:
    def __init__(self, data: Any, errors: Any):
        # This is where return data from the REST operation is stored.
        self.data = data
        # This is where any errors from the REST operation are stored.
        self.errors = errors

# Executes a GQL request against `GRAPHQL_URL`, given its body definition and accompanying variables.
# This returns whether the call was successful, paired with the `GqlResponse` returned.
def execute_gql_request(body, variables) -> Tuple[bool, GqlResponse]:

    # Create a dictionary for the GraphQL request.
    data: dict[str, Any] = {
        'query': body,
        'variables': variables
    }

    # Send the GraphQL request.
    response = requests.post(url=GRAPHQL_URL, json=data, headers=HEADERS)
    content = json.loads(response.content)

    if not response.ok:
        print('GQL request failed!')

    # Parse any errors (if they exist), otherwise return an empty error list.
    errors = content.get('errors', [])

    return (response.ok, GqlResponse(content['data'], errors))

# Executes a REST request against `REST_URL`, given its body definition and accompanying variables.
# This returns whether the call was successful, paired with the `RestResponse` returned.
def execute_rest_request(url, body) -> Tuple[bool, RestResponse]:

    response = requests.post(url, headers = HEADERS, json = body)
    # Check if the response returned a 200.
    if response.status_code != 200:
        error_info = response.json()  # Parse JSON to get more information from the `Message` field.
        print(error_info)
        error_message = error_info.get('Message', 'No error message provided')
        return (False, RestResponse(None, error_message))
    else:
        data = response.json()
        return (True, RestResponse(data, None))

# This method makes a call to POST /v3/dmp/firstparty/advertiser to retrieve the advertiser's first-party data for seed creation.
def get_first_party_data_rest( start_index, page_size) -> Tuple[bool, Any]:

    # Defines the payload for calling POST `/v3/dmp/firstparty/advertiser`.
    first_party_data_body = {
        "AdvertiserId": ADVERTISER_ID,
        "PageStartIndex": start_index, 
        "PageSize": page_size

        # Use these fields if you want to filter the first party data to create your seed with.
        #"SearchTerms": [], 
        #"SortFields": [{ }]
    }

    url = REST_URL + "/dmp/firstparty/advertiser"
    return execute_rest_request(url, first_party_data_body)
    
# This method calls into the `SeedCreate` GraphQL mutation to create a seed with the segments provided.
def create_seed_gql(variables) -> Tuple[bool, GqlResponse]:
    query = """
        mutation($advertiserId: ID!, $name: String!, $firstPartyDataInclusionIds: [ID!]){
            seedCreate(input: {
                advertiserId: $advertiserId,
                name: $name,
                targetingData: {
                firstPartyDataInclusionIds: $firstPartyDataInclusionIds
                }
            }) {
                data {
                    id
                }
                userErrors {
                    field
                    message
                }
            }
        }
    """
    # Send the GraphQL request.
    return execute_gql_request(query, variables)

# This method calls into the `AdvertiserSetDefaultSeed` GraphQL mutation to set the Advertiser's default seed to the one specified.
def set_advertiser_default_seed_gql( variables )-> Tuple[bool, GqlResponse]:

    query = """
        mutation($advertiserId:ID!, $seedId:ID!){
            advertiserSetDefaultSeed(input: {advertiserId: $advertiserId, 
            seedId: $seedId}) {
                data {
                defaultSeed {
                    id
                }
                }
                userErrors {
                field
                message
                }
            }
        }
    """

    # Send the GraphQL request.
    return execute_gql_request(query, variables)

# This method calls into the `SeedUpdate` GraphQL mutation to update the seed with specified variables.
def update_seed_gql( variables )-> Tuple[bool, GqlResponse]:

    query = """
        mutation($id:ID!, $name: String, $firstPartyDataInclusionIds: [ID!]){
            seedUpdate(input: {
                id: $id,
                name: $name, 
                targetingData: { firstPartyDataInclusionIds: $firstPartyDataInclusionIds } }
                ) 
                {
                    data {
                        id
                    }
                    userErrors {
                        field
                        message
                    }
            }
        }
    """

    # Send the GraphQL request.
    return execute_gql_request(query, variables)

# This method takes the response of the POST `/v3/dmp/firstparty/advertiser` REST call and parses it into a GQL variable for the seed mutation call.
def parse_first_party_data_into_gql_variables(variables, rest_response) -> None:
    first_party_data_entries = rest_response.data['Result']
    for entry in first_party_data_entries:
        variables["firstPartyDataInclusionIds"].append( str(entry["FirstPartyDataId"]) )


# -----------------------------------------------------------------------------------------------
# Main Workflow
# 1. Create a seed, using first-party data.
# 2.  If the seed is successfully created, it is set as the advertiser's default seed.
# 3. If the seed is successfully set as the default seed for the advertiser, any updates to its name or additionally provided first-party IDs will be applied.
# -----------------------------------------------------------------------------------------------
def main():

    # ------------------------------------------------------------------------------------------------------------------------
    # Mapping used to store the variables for passing into the GQL request.
    # advertiserId -> str: The advertiser ID.
    # name -> str: Name of the seed to be created.
    # firstPartyDataInclusionIds -> List[str]: A list of first-party data IDs.
    # ------------------------------------------------------------------------------------------------------------------------
    seed_creation_variables = {
            "advertiserId": ADVERTISER_ID,
            "name": SEED_NAME,
            "firstPartyDataInclusionIds": []
        }
    

    request_success, rest_response = get_first_party_data_rest(0, LIMIT_FIRST_PARTY_IDS)
    if request_success:
        parse_first_party_data_into_gql_variables(seed_creation_variables, rest_response)
    else:
        print("Error occured while sending rest request to retrieve first party data: ")
        print(rest_response.errors)
        exit()

    # Call the GQL `SeedCreate` mutation.
    gql_request_success, gql_response = create_seed_gql( seed_creation_variables )
    seed_id = ''
    if gql_request_success and gql_response.errors == []:
        # If the response was ok and and we have no `UserErrors`, then the seed was created successfully.
        seed_id = gql_response.data["seedCreate"]["data"]["id"]
        print("Successfully created the seed with id:" + seed_id)
        
    else:
        print("Error occurred while sending GQL request to create the seed: ")
        print(gql_response.errors)
        exit()
    
    # Create the variables dictionary for `AdvertiserSetDefaultSeed` mutation.
    advertiser_default_seed_variables = {
        "advertiserId": ADVERTISER_ID,
        "seedId": seed_id
    }

    # Call the GQL `AdvertiserSetDefaultSeed` mutation.
    gql_request_success, gql_response = set_advertiser_default_seed_gql(advertiser_default_seed_variables)
    if gql_request_success and gql_response.errors == []:
        # If the default went through and we have no `UserErrors`, than it's a successful advertiser seed default
        print(gql_response.data)
        print("Successfully applied seed as the default!")
    else:
        print("Error occurred while sending GQL request to default the seed: ")
        print(gql_response.errors)
        exit()

    should_make_seed_update_gql_call = False
    # Create the `SeedUpdate` variables dictionary.
    seed_update_variables = {
        "id": seed_id,
        "firstPartyDataInclusionIds": [],
        "name": ''
    }

    if ALTERNATIVE_FIRST_PARTY_IDS > 0:
        # To prevent assigning duplicate `FirstPartyDataId`s, `LIMIT_FIRST_PARTY_IDS` is used as the starting index.
        # If `ALTERNATIVE_FIRST_PARTY_IDS` was set, than POST `/v3/dmp/firstparty/advertiser` will be called to query another set of ids.
        # These ids will replace the `FirstPartyDataId`s.
        request_success, rest_response = get_first_party_data_rest(LIMIT_FIRST_PARTY_IDS + 1, ALTERNATIVE_FIRST_PARTY_IDS)
        if request_success:
            parse_first_party_data_into_gql_variables(seed_update_variables, rest_response)
            should_make_seed_update_gql_call = True
        else:
            print("Error occured while sending rest request to retrieve first party data: ")
            print(rest_response.errors)
            exit()
    
    if CHANGE_SEED_NAME != '':
        seed_update_variables["name"] =  CHANGE_SEED_NAME
        should_make_seed_update_gql_call =  True
    
    if should_make_seed_update_gql_call:
        # Call the GQL `SeedUpdate` mutation.
        gql_request_success, gql_response = update_seed_gql(seed_update_variables)
        if gql_request_success and gql_response.errors == []:
            # If the default went through and we have no `UserErrors`, than it's a successful seed update.
            print(gql_response.data)
            print("Successfully updated the seed!")
        else:
            print("Error occurred while sending GQL request to default the seed: ")
            print(gql_response.errors)
            exit()
    
if __name__ == "__main__":
    main()