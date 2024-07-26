##########################################################
# This script upgrades a given campaign's budget to Kokai.
##########################################################

import json
import pandas as pd
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

# Replace the placeholder with the ID of the campaign that has the Solimar budget you want to upgrade to Kokai.
target_campaign_id = 'TARGET_CAMPAIGN_ID_PLACEHOLDER'

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

# Represents the metadata of a specified ad group flight.
class KokaiAdGroupFlightMigrationData:
  def __init__(self, id: str, impression_budget: float = None, campaign_flight: int = None, daily_target: float = None, daily_impression_target: float = None, minimum_spend: float = None) -> None:
    # The ID of the ad group with which the flight is associated.
    self.adgroup_id: str = id
    # The impression budget of the flight.
    self.budget_in_impressions: float = impression_budget
    # The ID of the campaign flight to which the ad group flight is associated.
    self.campaign_flight_id: int = campaign_flight
    # The daily target of the ad group flight in advertiser currency.
    self.daily_target_in_advertiser_currency: float = daily_target
    # The daily impression target for the ad group flight.
    self.daily_target_in_impressions: float = daily_impression_target
    # The minimum spend of the ad group flight in advertiser currency.
    self.minimum_spend_in_advertiser_currency: float = minimum_spend
  def __str__(self):
    return f"AdGroupId: {self.adgroup_id}, BudgetInImpressions: {self.budget_in_impressions}, CampaignFlightId: {self.campaign_flight_id}, DailyTargetInAdvertiserCurrency: {self.daily_target_in_advertiser_currency}, DailyTargetInImpressions:{self.daily_target_in_impressions}, MinimumSpendInAdvertiserCurrency: {self.minimum_spend_in_advertiser_currency}"

# Maps ad group flight data to the data of a specified campaign flight.
class KokaiCampaignFlightToAdGroupFlights:
  def __init__(self, flight_id: int, adgroup_flights_data: List[KokaiAdGroupFlightMigrationData]) -> None:
    # The ID of the campaign flight.
    self.campaign_flight_id: int = flight_id
    # The ad group flights and their metadata that you want to map to the campaign flight.
    self.adgroup_flights: List[KokaiAdGroupFlightMigrationData] = adgroup_flights_data

# Retrieves the migration status for the campaign budget partitioned by the campaign flight.
def get_campaign_budget_migration_status(campaign_id: str) -> List[KokaiCampaignFlightToAdGroupFlights]:
  # Define the GraphQL query.
  query = """
  query GetCampaignBudgetMigrationStatus($campaignId: ID!) {
    campaign(id: $campaignId) {
      id
      budgetMigrationStatus(targetBudgetingVersion: KOKAI) {
        migrationData {
          campaignFlights {
            adGroupFlights {
              adGroupId
              budgetInImpressions
              campaignFlightId
              dailyTargetInAdvertiserCurrency
              dailyTargetInImpressions
              minimumSpendInAdvertiserCurrency
            }
            originalCampaignFlight {
              id
            }
          }
        }
      }
    }
  }
  """

  # Define the variables in the query.
  variables: dict[str, Any] = {
    'campaignId': campaign_id
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if not request_success:
    print(response.errors)
    raise Exception('Could not query the migration status of the campaign budget.')

  # Aggregate the ad group flight data by campaign flight.
  campaign_flight_to_adgroup_flight_map: List[KokaiCampaignFlightToAdGroupFlights] = []
  try:
    for campaign_flight in response.data['campaign']['budgetMigrationStatus']['migrationData']['campaignFlights']:
      original_campaign_flight_id = campaign_flight['originalCampaignFlight']['id']
      adgroup_flights = []

      # Each campaign flight can have multiple adgroup flights.
      for adgroup_flight in campaign_flight['adGroupFlights']:
        adgroup_flights.append(
          KokaiAdGroupFlightMigrationData(
            id = adgroup_flight['adGroupId']  ,
            impression_budget = adgroup_flight['budgetInImpressions'],
            campaign_flight = adgroup_flight['campaignFlightId'],
            daily_target = adgroup_flight['dailyTargetInAdvertiserCurrency'],
            daily_impression_target = adgroup_flight['dailyTargetInImpressions'],
            minimum_spend = adgroup_flight['minimumSpendInAdvertiserCurrency']
          )
        )

      # After the adgroup flight shave been parsed, add them to the campaign flight to adgroup flights map.
      campaign_flight_to_adgroup_flight_map.append(
        KokaiCampaignFlightToAdGroupFlights(
          original_campaign_flight_id,
            adgroup_flights
        )
      )
    return campaign_flight_to_adgroup_flight_map
  except Exception as e:
    raise Exception(f"An unexpected error occurred while parsing the migration budget query response: {e}")

# Upgrades the campaign budget to Kokai based on migration data input. If set to `True`, indicates that the budget was updated successfully.
def upgrade_to_kokai_budget(campaign_id: str, campaign_flight_to_adgroup_flight_map: List[KokaiCampaignFlightToAdGroupFlights]) -> bool:

  # Aggregate the ad group flight data into the input.
  campaign_flights = []

  # Go through the dictionary and construct the `campaignFlights` input field.
  for mapping in campaign_flight_to_adgroup_flight_map:
    campaign_flight_id = mapping.campaign_flight_id
    adgroup_flights = mapping.adgroup_flights

    adgroup_flights_formatted = [
      '{' +
      f'adGroupId: "{flight.adgroup_id}"' +
      (f', budgetInImpressions: {flight.budget_in_impressions}' if pd.notna(flight.budget_in_impressions) else '') +
      (f', campaignFlightId: {flight.campaign_flight_id}' if pd.notna(flight.campaign_flight_id) else '') +
      (f', dailyTargetInAdvertiserCurrency: {flight.daily_target_in_advertiser_currency}' if pd.notna(flight.daily_target_in_advertiser_currency) else '') +
      (f', dailyTargetInImpressions: {flight.daily_target_in_impressions}' if pd.notna(flight.daily_target_in_impressions) else '') +
      (f', minimumSpendInAdvertiserCurrency: {flight.minimum_spend_in_advertiser_currency}' if pd.notna(flight.minimum_spend_in_advertiser_currency) else '') +
      '}'
      for flight in adgroup_flights
    ]
    adgroup_flights_formatted = ', '.join(adgroup_flights_formatted)

    # Create a formatted string of the campaign flight ID and ad group flights.
    graphql_part = f"""
    {{
      campaignFlightId: {campaign_flight_id}
      adGroupFlights: [{adgroup_flights_formatted}]
    }}
    """

    campaign_flights.append(graphql_part)

  # Combine the data of the ad group flight and campaign flight into a single query string.
  graphql_field = "\n".join(campaign_flights)

  # Define the GraphQL query.
  query = f"""
  mutation UpgradeCampaignBudgetToKokai($campaignId: ID!) {{
    campaignBudgetSettingsUpdate(
      input: {{
        campaignId: $campaignId
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
  }}"""

  # Define the variables in the query.
  variables: dict[str, Any] = {
      'campaignId': campaign_id
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query, variables)

  if not request_success:
    print(response.errors)

  return request_success

#########################################################################################
# Execution Flow:
#  1. Retrieve the current campaign migration metdata, partitioned by campaign flight.
#  2. Print the migration data you retrieved.
#  3. Upgrade the campaign budget to Kokai.
#########################################################################################
migration_data = get_campaign_budget_migration_status(target_campaign_id)

print("Migration elements for the campaign:")
for data in migration_data:
  print("Campaign Flight Id: " + data.campaign_flight_id)
  print("AdGroup Flights:")
  for ag_flight in data.adgroup_flights:
    print(ag_flight)

if upgrade_to_kokai_budget(target_campaign_id, migration_data):
  print('Budget successfully migrated to Kokai.')
else:
  print('Budget could not be upgraded.')