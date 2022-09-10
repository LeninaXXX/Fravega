#!/usr/bin/env python
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Shows how to download in parallel a set of reports from a list of accounts.

If you need to obtain a list of accounts, please see the
account_management/get_account_hierarchy.py or
account_management/list_accessible_customers.py examples.
"""

import argparse
from itertools import product
import multiprocessing
import time

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Maximum number of processes to spawn.
MAX_PROCESSES = multiprocessing.cpu_count()
# Timeout between retries in seconds.
BACKOFF_FACTOR = 5
# Maximum number of retries for errors.
MAX_RETRIES = 0


def main(client, customer_ids):
    """The main method that creates all necessary entities for the example.

    Args:
        client: an initialized GoogleAdsClient instance.
        customer_ids: an array of client customer IDs.
    """

    # Define the GAQL query strings to run for each customer ID.

    # Keywords Performance is the old category 
    keywords_performance_query = { 
        "name": "keywords_performance_query",
        "query": (
            "SELECT "
                "customer.descriptive_name, "                                   # AccountDescriptiveName
                "segments.date, "                                               # Date
                "segments.device, "                                             # Device
                # XXX: According to the documentation in https://developers.google.com/google-ads/api/fields/v11/segments
                # segments.device cannot be SELECTed with metrics.average_page_views
                "campaign.name, "                                               # CampaignName
                "ad_group_criterion.keyword.text, "                             # Criteria
                "ad_group.name, "                                               # AdGroupName
                "ad_group_criterion.status, "                                   # Status  
                "ad_group_criterion.keyword.match_type, "                       # KeywordMatchType
                "ad_group_criterion.effective_cpc_bid_micros, "                 # CpcBid
                "metrics.clicks, "                                              # Clicks
                "metrics.impressions, "                                         # Impressions
                "metrics.average_cpc, "                                         # AverageCpc
                "metrics.ctr, "                                                 # Ctr
                "metrics.cost_micros, "                                         # Cost
              # (*DEPRECATED*)                                                  # AveragePosition <--------------- XXX
                "ad_group_criterion.quality_info.quality_score, "               # QualityScore
              # (REQUIRES `Select label.name from the resource ad_group_label`) # Labels <------------------------ XXX
                "metrics.search_impression_share, "                             # SearchImpressionShare
                "metrics.search_rank_lost_impression_share, "                   # SearchRankLostImpressionShare
                "metrics.search_exact_match_impression_share, "                 # SearchExactMatchImpressionShare
                "metrics.conversions, "                                         # Conversions
                "metrics.all_conversions, "                                     # AllConversions
                "metrics.cross_device_conversions, "                            # CrossDeviceConversions
                "metrics.conversions_value, "                                   # ConversionValue
                "metrics.all_conversions_value, "                               # AllConversionValue
                "metrics.video_quartile_p100_rate, "                            # VideoQuartile100Rate
                "metrics.video_quartile_p75_rate, "                             # VideoQuartile75Rate
                "metrics.video_quartile_p50_rate, "                             # VideoQuartile50Rate
              # "metrics.average_page_views, "                                  # AveragePageviews
                # XXX: THIS METRIC CANNOT COEXIST WITH segments.device?
                "metrics.video_views "              # VideoViews
            """
            FROM keyword_view
            WHERE segments.date DURING TODAY
            AND campaign.status = "ENABLED"
            ORDER BY metrics.clicks DESC
            """
        )
      }
      # Keywords: como vos haces campa#as en marketing para pegarle a palabras. Cuando la gente busca una palabra determinada. Campa#as para laburar con palabras especificas.
      # Ad_Performance: 

      # objetivo de minima:
      # -------------------
      # 

    ad_performance_query = {
        "name": "ad_performance_query", 
        "query": (
            "SELECT "
                "customer.descriptive_name, "               # AccountDescriptiveName
                "segments.date, "                           # Date
                "segments.device, "                         # Device
                "campaign.name, "                           # CampaignName
                "ad_group.name, "                           # AdGroupName
                "ad_group_ad.ad.id, "                       # Id
                "ad_group_ad.ad.type, "                     # AdType
                "ad_group_ad.ad.text_ad.headline, "         # Headline
                "ad_group_ad.ad.image_ad.name, "            # ImageCreativeName
                "metrics.clicks, "                          # Clicks
                "metrics.impressions, "                     # Impressions
                "metrics.ctr, "                             # Ctr
                "metrics.average_cpc, "                     # AverageCpc
                "metrics.average_cpm, "                     # AverageCpm
                "metrics.cost_micros, "                     # Cost
              # "(DEPRECATED), "                            # DEPRECATED # AveragePosition
                "ad_group_ad.ad.final_urls, "               # CreativeFinalUrls
                "customer.id, "                             # ExternalCustomerId
              # "(DEPRECATED), "                            # CreativeDestinationUrl
              # "(DEPRECATED), "                            # CreativeFinalMobileUrls
                "ad_group_ad.status, "                      # Status
                "metrics.conversions, "                     # Conversions
                "metrics.all_conversions_value, "           # AllConversionValue
                "metrics.cross_device_conversions, "        # CrossDeviceConversions
                "metrics.all_conversions, "                 # AllConversions
                "metrics.conversions_value, "               # ConversionValue
                "metrics.video_quartile_p100_rate, "        # VideoQuartile100Rate
                "metrics.video_quartile_p75_rate, "         # VideoQuartile75Rate
                "metrics.video_quartile_p50_rate, "         # VideoQuartile50Rate
                "metrics.video_views "                       # VideoViews

            """
            FROM ad_group_ad
            WHERE segments.date DURING TODAY
            AND campaign.status = "ENABLED"
            ORDER BY metrics.clicks DESC
            """
            )
        }
        # select only last 7 days
        # ... in descending order, by metric.clicks       

    inputs = generate_inputs(
        client, customer_ids, [keywords_performance_query, ad_performance_query]
    )
    with multiprocessing.Pool(MAX_PROCESSES) as pool:
        # Call issue_search_request on each input, parallelizing the work
        # across processes in the pool.
        results = pool.starmap(issue_search_request, inputs)

        # Partition our results into successful and failed results.
        successes = []
        failures = []
        for res in results:
            if res[0]:
                successes.append(res[1])
            else:
                failures.append(res[1])

        # Output results.
        print(
            f"Total successful results: {len(successes)}\n"
            f"Total failed results: {len(failures)}\n"
        )

        print("Successes:") if len(successes) else None
        for success in successes:
            # success["results"] represents an array of result strings for one
            # customer ID / query combination.
            result_str = ("\n" + "-" * 80 + "\n").join(success["results"])
            with open('out/' + str(success["customer_id"]) + " - " + str(success["query"]["name"]) + '.lst', 'w') as f:
                print(result_str, file = f)
                # print(result_str)

        print("Failures:") if len(failures) else None
        for failure in failures:
            ex = failure["exception"]
            print(
                f'Request with ID "{ex.request_id}" failed with status '
                f'"{ex.error.code().name}" for customer_id '
                f'{failure["customer_id"]} and query "{failure["query"]}" and '
                "includes the following errors:"
            )
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for (
                        field_path_element
                    ) in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")


def issue_search_request(client, customer_id, query):
    """Issues a search request using streaming.

    Retries if a GoogleAdsException is caught, until MAX_RETRIES is reached.

    Args:
        client: an initialized GoogleAdsClient instance.
        customer_id: a client customer ID str.
        query: a GAQL query str.
    """
    ga_service = client.get_service("GoogleAdsService")
    retry_count = 0
    # Retry until we've reached MAX_RETRIES or have successfully received a
    # response.
    while True:
        try:
            stream = ga_service.search_stream(
                customer_id=customer_id, query=query["query"]
            )
            # Returning a list of GoogleAdsRows will result in a
            # PicklingError, so instead we put the GoogleAdsRow data
            # into a list of str results and return that.
            result_strings = []
            for batch in stream:
                for row in batch.results:
                    ad_group_id = (
                        f"Ad Group ID {row.ad_group.id} in "
                        if "ad_group.id" in query
                        else ""
                    )
                    result_string = (
                        f"{ad_group_id} > "
                        # f"Campaign ID {row.campaign.id} "
                        # f"had {row.metrics.impressions} impressions "
                        # f"and {row.metrics.clicks} clicks."
                        f"Row: {row}"
                    )
                    result_strings.append(result_string)
            return (
                True, 
                {
                    "customer_id": customer_id,
                    "results": result_strings,
                    "query": query # XXX:
                }
            )
        except GoogleAdsException as ex:
            # This example retries on all GoogleAdsExceptions. In practice,
            # developers might want to limit retries to only those error codes
            # they deem retriable.
            if retry_count < MAX_RETRIES:
                retry_count += 1
                time.sleep(retry_count * BACKOFF_FACTOR)
            else:
                return (
                    False,
                    {
                        "exception": ex,
                        "customer_id": customer_id,
                        "query": query,
                    },
                )


def generate_inputs(client, customer_ids, queries):
    """Generates all inputs to feed into search requests.

    A GoogleAdsService instance cannot be serialized with pickle for parallel
    processing, but a GoogleAdsClient can be, so we pass the client to the
    pool task which will then get the GoogleAdsService instance.

    Args:
        client: An initialized GoogleAdsClient instance.
        customer_ids: A list of str client customer IDs.
        queries: A list of str GAQL queries.
    """
    return product([client], customer_ids, queries)


if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage(version="v11", path='.\google-ads.yaml')

    parser = argparse.ArgumentParser(
        description="Download a set of reports in parallel from a list of "
        "accounts."
    )
    # The following argument(s) should be provided to run the example.
    parser.add_argument(
        "-c",
        "--customer_ids",
        nargs="+",
        type=str,
        required=True,
        help="The Google Ads customer IDs.",
    )
    parser.add_argument(
        "-l",
        "--login_customer_id",
        type=str,
        help="The login customer ID (optional).",
    )
    args = parser.parse_args()
    # Override the login_customer_id on the GoogleAdsClient, if specified.
    if args.login_customer_id is not None:
        googleads_client.login_customer_id = args.login_customer_id

    main(googleads_client, args.customer_ids)