"""Partially stolen from https://developers.google.com/google-ads/api/docs/samples/get-campaigns#python
Getting all campaigns for *A GIVEN customer_id* ... I don't have a customer_id...
(... nor valid and/or refreshed access token, nor cannot refresh them)
(@ 20220906: Now I have both customer_id & refresh_token :) )
LLVL @ 20220905
"""
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

customer_ids = ["4389555570"] # this could be harvested from argv's

# Get a client object, authenticate with .yaml
client = GoogleAdsClient.load_from_storage(version = 'v11', path = './google-ads.yaml')
ga_service = client.get_service("GoogleAdsService")

query = """
    SELECT
      campaign.id,
      campaign.name
    FROM campaign
    ORDER BY campaign.id"""

# Issues a search request using streaming.
for i, customer_id in enumerate(customer_ids):
    print("QUERYING FOR customer_id no.:", i)
    try:        
        stream = ga_service.search_stream(customer_id = customer_id, query = query)
        print("stream>", stream)
        print('\n', '-' * 80, '\n')
        for (i, batch) in enumerate(stream):
            print(i, '>', batch)
            for row in batch.results:
                print(
                    f"Campaign with ID {row.campaign.id} and name "
                    f'"{row.campaign.name}" was found.'
                )
    except GoogleAdsException as e:
        print("Exception: ", e)
