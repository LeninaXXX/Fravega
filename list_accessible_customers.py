# list_accessible_customers.py
"""This example lists the resource names for the customers that the
authenticating user has access to.

The customer IDs retrieved from the resource names can be used to set
the login-customer-id configuration. For more information see this
documentation: https://developers.google.com/google-ads/api/docs/concepts/call-structure#cid
"""
import sys
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# [START list_accessible_customers]
def main(client):
    customer_service = client.get_service("CustomerService")

    accessible_customers = customer_service.list_accessible_customers()
    result_total = len(accessible_customers.resource_names)
    print(f"Total results: {result_total}")

    resource_names = accessible_customers.resource_names
    for resource_name in resource_names:
        print(f'Customer resource name: "{resource_name}"')
    # [END list_accessible_customers]


if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage(version = "v11", path = './google-ads.yaml')

    try:
        main(googleads_client)
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)
