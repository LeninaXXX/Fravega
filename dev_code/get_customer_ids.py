"""Partially stolen from https://developers.google.com/google-ads/api/docs/samples/get-account-information#python
Getting account information for *A GIVEN customer_id*
LLVL @ 20220906
"""

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

customer_ids = ["2758672663", "3929247001"] # this could be harvested from argv's

# Get a client object, authenticate with .yaml
client = GoogleAdsClient.load_from_storage(version = 'v11', path = './google-ads.yaml')
ga_service = client.get_service('GoogleAdsService')

# pre-built query
query = """
    SELECT 
        customer.id, 
        customer.descriptive_name, 
        customer.currency_code, 
        customer.time_zone, 
        customer.tracking_url_template, 
        customer.auto_tagging_enabled 
    FROM 
        customer 
    LIMIT 
        50"""

for customer_id in customer_ids:
    print("Processing customer_id :", customer_id)

    request = client.get_type("SearchGoogleAdsRequest")
    request.query = query
    request.customer_id = customer_id   # the search criteria is according to customer_id

    try:
        response = ga_service.search(request = request)
    except Exception as e:
        print("+++ ----------------------------------------------------")
        print("+++ Something went wrong for customer_id :", customer_id)
        print("+++ EXCEPTION :\n", str(e))
        print("+++ ----------------------------------------------------")
        continue
    else:
        print("len(list(response)) :", len(list(response)))
        print("type(response) :", type(response))
        print("-" * 40)
        for i, resp in enumerate(response):
            print(i, '>', resp)

# -------------------------
# request = client.get_type("SearchGoogleAdsRequest")
# 
# request.query = query
# request.customer_id = CUSTOMER_ID
# 
# response = ga_service.search(request = request)
# # customer = list(response)[0].customer
# 
# import pprint
# for c in list(response):
#     pprint.pprint(c)
