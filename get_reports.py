#!/usr/bin/env python
"""Download in parallel a set of reports from a list of accounts.
If you need to obtain a list of accounts, please see the 
account_management/get_account_hierarchy.py or
account_management/list_accessible_customers.py examples.
"""
import argparse, sys, multiprocessing, time, json
from collections import namedtuple
from datetime import date
from itertools import product

# Uncomment following line for Oracle Low Level debugging to stderr
# os.environ['DPI_DEBUG_LEVEL'] = '16'
DB_CONFIG_FILE = 'databases.json'
DB_DEFAULT = "DESA STG"

ORACLE_BATCH_SIZE = 1024        # Nice 2-round number. TODO: .executemany() in batches of size
# Valid fields for campaign.status field -- From Google Ads documentation
# https://developers.google.com/google-ads/api/fields/v11/campaign#campaign.status
CAMPAIGN_VALID_STATUSES = ('ENABLED', 'PAUSED', 'REMOVED', 'UNKNOWN', 'UNSPECIFIED')                                                                                            

import cx_Oracle
# NOTE: KEEP IN MIND THAT THIS IS Conditional on which underlying platform this is running? Windows 10 until now...
cx_Oracle.init_oracle_client(lib_dir = r'C:\code\fravega\Fravega_api_request_test\instantclient_21_6')

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf import json_format

# Max n of procs to spawn / Timeout between retries in secs / Max n of retries for errors
PROCS_PER_CPU = 1 # Given that most of the time processes are blocking, multiple workers could be assigned per-CPU
MAX_PROCESSES, BACKOFF_FACTOR, MAX_RETRIES = multiprocessing.cpu_count() * PROCS_PER_CPU, 5, 0

def main(client, customer_ids, date_range, campaign_status, database):
    """The main method that creates all necessary entities for the example.
    Args: client: an initialized GoogleAdsClient instance.
          customer_ids: an array of client customer IDs.
    """
    # Output some diagnostic information:
    printout("customer_ids:", ', '.join(customer_ids))
    printout("STARTING DATE: %s // ENDING DATE: %s" % (date_range.start.isoformat(), date_range.end.isoformat()))
    printout("WHERE campaign.status = %s" % (campaign_status, ))

    # DATABASE SCHEMAS: These allows to deal with queries, database rows, and their mutual correspondence in a more comfortable way.
    # Keep in mind that Oracle bindings reference column name starting at one while Python reference sequence stuff starting at 0
    # NOTE: schema columns whose first components equals None, shouldn't exist in the database. The tag is there to deal with
    #   the discrepancy between the Google Ads GAQL query, and the schema in our (outdated...) tables.
    keywords_performance_dbschema = (
        # how fields are refered to...
        # in GAQL ........................................... in OUR database
        ("customer.id"                                      , "CUSTOMER_ID"), 
        ("customer.descriptive_name"                        , "CUENTA"), 
        ("segments.date"                                    , "DIA"), 
        ("segments.device"                                  , "DEVICE"), 
        # XXX: According to the documentation in https://developers.google.com/google-ads/api/fields/v11/segments
        # segments.device cannot be SELECTed with metrics.average_page_views
        ("campaign.name"                                    , "CAMPAIGN"), 
        ("ad_group_criterion.keyword.text"                  , "KEYWORD"), 
        ("ad_group.name"                                    , "AD_GROUP"), 
        ("ad_group_criterion.status"                        , "KEYWORD_STATE"), 
        ("ad_group_criterion.keyword.match_type"            , "MATCH_TYPE"), 
        ("ad_group_criterion.effective_cpc_bid_micros"      , "MAX_CPC"), 
        ("metrics.clicks"                                   , "CLICKS"), 
        ("metrics.impressions"                              , "IMPRESSIONS"), 
        ("metrics.average_cpc"                              , "AVG_CPC"), 
        ("metrics.ctr"                                      , "CTR"), 
        ("metrics.cost_micros"                              , "COST"), 
        (None                                               , "AVG_POSITION"),         # *DEPRECATED!* # AVG_POSITION
        ("ad_group_criterion.quality_info.quality_score"    , "QUALITY_SCORE"), 
        (None                                               , "LABELS"),               # *DEPRECATED!* ...or requiring further work
        # (REQUIRES `Select label.name from the resource ad_group_label`)              # LABELS
        ("metrics.search_impression_share"                  , "SEARCH_IMPR_SHARE"),    # THESE COLS SHOULDN'T EXIST IN THE DATABASE
        ("metrics.search_rank_lost_impression_share"        , "SEARCH_LOST_IS_RANK"), 
        ("metrics.search_exact_match_impression_share"      , "SEARCH_EXACT_MATCH_IS"), 
        ("metrics.conversions"                              , "CONVERSIONS"), 
        ("metrics.all_conversions"                          , "ALL_CONV"), 
        ("metrics.cross_device_conversions"                 , "CROSS_DEVICE_CONV"), 
        ("metrics.conversions_value"                        , "TOTAL_CONV_VALUE"), 
        ("metrics.all_conversions_value"                    , "ALL_CONV_VALUE"), 
        ("metrics.video_quartile_p100_rate"                 , "VIDEO_PLAYED_TO_100"), 
        ("metrics.video_quartile_p75_rate"                  , "VIDEO_PLAYED_TO_75"), 
        ("metrics.video_quartile_p50_rate"                  , "VIDEO_PLAYED_TO_50"),
        (None                                               , "VIDEO_VIEWS")
    )

    ad_performance_dbschema = (
        ("customer.id"                             , "CUSTOMER_ID"), 
        ("customer.descriptive_name"               , "ACCOUNT"), 
        ("segments.date"                           , "DAY"), 
        ("segments.device"                         , "DEVICE"), 
        ("campaign.name"                           , "CAMPAIGN"), 
        ("ad_group.name"                           , "AD_GROUP"), 
        ("ad_group_ad.ad.id"                       , "AD_ID"), 
        ("ad_group_ad.ad.type"                     , "AD_TYPE"), 
        ("ad_group_ad.ad.text_ad.headline"         , "AD"), 
        ("ad_group_ad.ad.image_ad.name"            , "IMAGE_AD_NAME"), 
        ("metrics.clicks"                          , "CLICKS"), 
        ("metrics.impressions"                     , "IMPRESSIONS"), 
        ("metrics.ctr"                             , "CTR"), 
        ("metrics.average_cpc"                     , "AVG_CPC"), 
        ("metrics.average_cpm"                     , "AVG_CPM"), 
        ("metrics.cost_micros"                     , "COST"), 
        (None                                      , "AVG_POSITION"),       # *DEPRECATED!* AveragePosition
        ("ad_group_ad.ad.final_urls"               , "FINAL_URL"), 
        (None                                      , "DESTINATION_URL"),    # *DEPRECATED!* CreativeDestinationUrl
        (None                                      , "MOBILE_FINAL_URL"),   # *DEPRECATED!* CreativeFinalMobileUrls
        ("ad_group_ad.status"                      , "AD_STATE"), 
        ("metrics.conversions"                     , "CONVERSIONS"), 
        ("metrics.all_conversions_value"           , "ALL_CONV_VALUE"), 
        ("metrics.cross_device_conversions"        , "CROSS_DEVICE_CONV"), 
        ("metrics.all_conversions"                 , "ALL_CONVERSION_"), 
        ("metrics.conversions_value"               , "TOTAL_CONVERSION_VALUE"), 
        ("metrics.video_quartile_p100_rate"        , "VIDEO_PLAYED_TO_100"), 
        ("metrics.video_quartile_p75_rate"         , "VIDEO_PLAYED_TO_75"), 
        ("metrics.video_quartile_p50_rate"         , "VIDEO_PLAYED_TO_50"), 
        (None                                      , "VIDEO_VIEWS")         # *DEPRECATED!* VIDEO_VIEWS
    )

    # FIXME: This kruft exists because there are dangling columns in the database that shouldn't be there and DO NOT correspond 
    # to fields queried to Google Ads. Here only to eliminate those columns in the dbschemas tagged as `None`. Those columns,
    # while existing in our (legacy) database, have no corresponding values on Google Ads, so they cannot be SELECTed in the query
    ad_performance_select_fields = [i[0] for i in ad_performance_dbschema if i[0]]
    keywords_performance_select_fields = [i[0] for i in keywords_performance_dbschema if i[0]]
    # FIXME: ... ideally, the Google Ads GAQL queries should rely only on these string joins
    ad_performance_select_str = ', '.join((i for i in ad_performance_select_fields))
    keywords_performance_select_str = ', '.join((i for i in keywords_performance_select_fields))

    # Compute GAQL date range string
    date_range_str = ( "DURING TODAY" if (date_range.start == date_range.end) 
                 else f"BETWEEN '{date_range.start.isoformat()}' AND '{date_range.end.isoformat()}'" )
    printout("date_range_str: ", date_range_str)

    # Define the GAQL query strings to run for each customer ID.
    # Keywords Performance is the old category 
    keywords_performance_query = { 
        "name": "keywords_performance",
        "dbschema": keywords_performance_dbschema,
        "dbtable": "ITZ_MKT_KEY",
        "query": f'SELECT {keywords_performance_select_str} ' 
                 f'FROM keyword_view '
                 f'WHERE segments.date {date_range_str} AND campaign.status = {campaign_status} '
                 f'ORDER BY metrics.clicks DESC'    # Ordering it by metric.clicks in DESCending order because why not
    }
    
    ad_performance_query = {
        "name": "ad_performance", 
        "dbschema": ad_performance_dbschema,
        "dbtable" : "ITZ_MKT_ADS",
        "query": f'SELECT {ad_performance_select_str} '
                 f'FROM ad_group_ad '
                 f'WHERE segments.date {date_range_str} AND campaign.status = {campaign_status} '
                 f"ORDER BY metrics.clicks DESC"    # ... idem
    }

    inputs = generate_inputs(client, customer_ids, [keywords_performance_query, ad_performance_query])
    
    with multiprocessing.Pool(MAX_PROCESSES) as pool:
        # Call issue_search_request on each input, parallelizing the work across processes in the pool.
        results = pool.starmap(issue_search_request, inputs)
        
        # Partition our results into successful and failed results.
        successes = []
        failures = []
        for res in results:
            if res[0]:
                successes.append(res[1])    # Everything on this list... commit to database
            else:
                failures.append(res[1])     # Potential errors to be dealt with

        # Output results summary
        # How many, and which jobs succeded -- make it explicit
        printout(f"Total successful results: {len(successes)}\n")
        if successes:
            printout("Successes:")
            for success in successes:
                printout(f'\tcustomer_id : {success["customer_id"]} '
                         f'// query_name : {success["query"]["name"]} '
                         f'// # results : {len(success["results"])}')
        
        # How many, and which jobs failed -- make it explicit
        printout(f"Total failed results: {len(failures)}\n")
        if failures:
            printout("Failures:")
            for failure in failures:
                printout(f'\tcustomer_id : {failure["customer_id"]} // query_name : {failure["query"]["name"]}')
        
        # DB: Connect to the database:
        # DB: ... loading database configuration
        printout("Loading database configuration from", DB_CONFIG_FILE)
        with open(DB_CONFIG_FILE) as f:
            dbs = json.load(f)
        base = dbs[database]
        
        # DB: ... connecting to the database
        printout("Connecting to Database...")
        printout(f'\tHost: {base["host"]} / Port: {base["port"]} / ServiceName: {base["database"]}')
        printout(f"\tUser: {base['user2']}")
        dsn_tns = cx_Oracle.makedsn(base['host'], base['port'], service_name = base['database'])
        
        with cx_Oracle.connect(user = base['user2'], password = base['passwd'], dsn = dsn_tns) as conn:
            # DB: ...building database cursor
            cursor = conn.cursor()

            for success in successes:
                dbschema = success["query"]["dbschema"]     # "pointer" to successful job's corresponding dbschema
                dbtable_name = success["query"]["dbtable"]  # ... to database table name
                query_name = success["query"]["name"]
                customer_id = success["customer_id"]

                results = success["results"]                # ... a list of GoogleAdsRow objects

                gaql_cols_names = [col[0] for col in dbschema]  # dbschema guarantees for these two to have a one-to-one
                sql_cols_names = [col[1] for col in dbschema]   # correspondence or it should anyway... FIXME: and there are 
                                                                # non-corresponding fields due to 'fossil' columns in the database

                sql_insert_string = (   # Construct Oracle SQL on-the-fly according to the corresponding dbschema
                        'INSERT INTO ' + dbtable_name + ' ' +
                        '(' + ', '.join(sql_cols_names) + ', FECHA_CREACION) ' +
                        'VALUES ' +
                        '(' + ', '.join((":" + str(i) for i, _ in enumerate(sql_cols_names, start = 1))) + ', SYSDATE)' 
                    ) # in .join'ing the sql_cols_names names, could use range(), but enumerate() makes it more explicit

                n_results = len(results)    # I prefer explicit pre-calculation to implicit compiler optimization cuz' issues...
                for i, result in enumerate(results):
                    fields_vals_list = []
                    for field in gaql_cols_names:
                        f = get_field(result, field)    # retrieves GAQL `field` from `result`: see get_field() definition
                        # NOTE: for 'plurals', GoogleAds return a list. If it's not a list, stringify. If it is a list AND
                        #    it ain't empty, stringify it's first element. If it is empty, settle for a stringified 'None'
                        f = (str(f[0]) if len(f) > 0 else str(None)) if isinstance(f, list) else str(f)
                        fields_vals_list.append(f)
                    try:
                        cursor.execute(sql_insert_string, fields_vals_list)     # TODO: Use cursor.executemany() for performance
                        printout(f"Executing INSERT {i+1}/{n_results}")
                        printout('dbtable_name:', dbtable_name, '// query:', query_name, '// For client_id:', customer_id)
                    except Exception as e:
                        printerr(f'For <{i}/{n_results}>', '=' * 40)
                        printerr(f"\tFAILED INSERT {i}/{n_results}!!!")
                        printerr('\tdbtable_name:', dbtable_name, '// query:', query_name, '// For client_id:', customer_id)
                        printerr('-' * 40)
                        printerr("sql_insert_string>\n\t", sql_insert_string)
                        printerr("fields_vals_list>\n\t", fields_vals_list)
                conn.commit()

        # TODO: Improve error Management
        printerr("Failures:") if len(failures) else None
        for failure in failures:
            ex = failure["exception"]
            printerr(f'Request with ID "{ex.request_id}" failed with status '
                     f'"{ex.error.code().name}" for customer_id '
                     f'{failure["customer_id"]} and query "{failure["query"]}" and '
                      "includes the following errors:" )
            for error in ex.failure.errors:
                printerr(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        printerr(f"\t\tOn field: {field_path_element.field_name}")


def issue_search_request(client, customer_id, query):
    """Issues a search request using streaming.
    Retries if a GoogleAdsException is caught, until MAX_RETRIES is reached.
    Args: client: an initialized GoogleAdsClient instance.
          customer_id: a client customer ID str.
          query: a GAQL query str.
    """
    ga_service = client.get_service("GoogleAdsService")
    retry_count = 0
    # Retry until we've reached MAX_RETRIES or have successfully received a
    # response.
    while True:
        try:
            stream = ga_service.search_stream(customer_id = customer_id, query = query["query"])

            # Returning a list of GoogleAdsRows will result in a PicklingError, so instead 
            # we put the GoogleAdsRow data into a list of str results and return that.
            results_dicts = []
            for batch in stream:
                for row in batch.results:
                    results_dicts.append(json_format.MessageToDict(row))
                # NOTE: True indicates a successful query
                return (True, {"customer_id": customer_id,     # NOTE: Label it so it can be 
                               "query":       query,           #    dealt with when returned
                               "results":     results_dicts,})

        except GoogleAdsException as ex:
            # This example retries on all GoogleAdsExceptions. In practice, developers 
            # might want to limit retries to only those error codes they deem retriable.
            if retry_count < MAX_RETRIES:
                retry_count += 1
                time.sleep(retry_count * BACKOFF_FACTOR)
            else:
                # NOTE: False indicates a failed query, after MAX_RETRIES attempts
                return (False, {"customer_id": customer_id,    # NOTE: Label it so it can be 
                                "query":       query,          #    dealt with when returned
                                "exception":   ex,})

def generate_inputs(client, customer_ids, queries):
    """Generates all inputs to feed into search requests.
    A GoogleAdsService instance cannot be serialized with pickle for
    parallel processing, but a GoogleAdsClient can be, so we pass the 
    client to the pool task which will then get the GoogleAdsService 
    instance.
    Args: client: An initialized GoogleAdsClient instance.
          customer_ids: A list of str client customer IDs.
          queries: A list of str GAQL queries.
    """
    return product([client], customer_ids, queries)

def as_camelcase(string):
    """
    Convert a string from snake_case to camelCase
    Args: string: A string to be converted from snake_case to camelCase
                                                              by Ленина
    """
    substrings = string.split('_')
    if substrings[1:]:
        substrings[1:] = [s.capitalize() for s in substrings[1:]]
    
    return ''.join(substrings)

def get_field(mapping, field):
    """
    Treewalks a given dictionary's keys in order to access a nested
    field. If such field does not exist, it returns 'None'.
    NOTE: if field == None, then it defaults to returning None. While
          this is a kruft added to deal with an outdated database, on
          the other hand it seems like a 'sane' behaviour all by itself
    Args: mapping: A dictionary, maybe having dictionaries for values
          field: A point-separated ('.') separated string that
              specifies a key potentially buried within nested dicts
              It expects each point-delimited substring as snake-case,
              and converts them into camelCase prior to using them to
              treewalk mapping.
                                                              by Ленина
    """
    if not field:   # FIXME: Exists to deal with None's in the schema, that themselves exist
        return None # in order to deal with an outdated (and to be fixed), database schema

    attrs = field.split('.')
    attrs = [as_camelcase(attr) for attr in attrs]
    attrs.reverse() # reverse in place so as to be treated as a stack

    if attrs:                           # XXX: is this redundant? ... i think it is...
        pivot = mapping[attrs.pop()]
    else:
        return None
  
    while attrs:
        try:
            pivot = pivot[attrs.pop()]
        except KeyError:    # the Google Ads API returned a non-existent field ... assume that field is empty
            return None     # it signals an empty field
        except Exception as ex:
            printerr("SOMETHING WENT AWFULY WRONG!!!")
            printerr("+++ DEBUG INFO: ")
            printerr('\t', ex)
            raise     
    
    return pivot

def printout(*args, **kwargs):
    """
    Wrapper around print in order to redirect print() to so as to be
    recognizable in postprocessing/logging when both stdout & stderr
    are multiplexed into a single terminal
                                                              by Ленина
    """
    return print(*args, **kwargs, file = sys.stdout)

def printerr(*args, **kwargs):
    """
    Wrapper around print in order to redirect print() to stderr and 
    label its output so as to be recognizable in postprocessing/logging
    when both stdout & stderr are multiplexed into a single terminal
                                                              by Ленина
    """
    return print("stderr:", *args, **kwargs, file = sys.stderr)

# @entrypoint
if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage(version="v11", path='.\google-ads.yaml')

    parser = argparse.ArgumentParser(description = "Download a set of reports in parallel from a list of accounts.")
    
    # cmdline arguments
    # ... regarding authentication and which account to query
    parser.add_argument("-c", "--customer_ids",
                        nargs = "+", type = str, required = True,
                        help = "The Google Ads customer IDs.",)
    parser.add_argument("-l", "--login_customer_id",
                        type = str,
                        help = "The login customer ID (optional).",)
    # ... regarding which time period to query
    parser.add_argument("-s", "--start_date",
                        type = str, default = date.today().isoformat(),
                        help = "Start date for the queries as YYYY-MM-DD. Defaults to TODAY")
    parser.add_argument("-e", "--end_date",
                        type = str, default = date.today().isoformat(),
                        help = "End date for the queries as YYYY-MM-DD. Defaults to TODAY")
    parser.add_argument("-k", "--campaign_status",
                        type = str, default = "ENABLED",
                        help = "Specifies campaigns.status for the GAQL request. "
                               "Valid values: " + ', '.join(CAMPAIGN_VALID_STATUSES) + ". "
                               "Defaults to: ENABLED")
    # ... regarding database to which to commit
    try:    # Take a peek at the available database keys
        with open(DB_CONFIG_FILE) as f:
            available_dbs = [k.upper() for k in json.load(f).keys()] # just in case some key is not ALL UPPERCASE
    except FileNotFoundError:
        printerr(f"FileNotFoundError: Database configuration file {DB_CONFIG_FILE} not found")
        printerr(f"A valid configuration file with name {DB_CONFIG_FILE} must exist in the same directory that {sys.argv[0]}")
        exit(1)
    except:
        printerr("Unknown error!!! Exiting...")
        exit(1)
    parser.add_argument("-d", "--database",                 # Database selection option
                        type = str, default = DB_DEFAULT,   # Defaults to DESArrollo database
                        help = "Specifies to which database to commit. Database names with spaces must be quoted. " + 
                               "Values specified in " + DB_CONFIG_FILE + '. ' "Available database options: " + 
                               ', '.join(available_dbs))
    
    args = parser.parse_args()

    # Override the login_customer_id on the GoogleAdsClient, if specified.
    if args.login_customer_id is not None:
        googleads_client.login_customer_id = args.login_customer_id

    # Compute and validate date range from cmd_line parameters:
    # ... it's necessary to validate if dates is specified in cmdline. XXX: there's probably a better way to write this...
    try:
        date.fromisoformat(args.start_date)
    except ValueError:
        printerr("Wrong start_date parameter format! Date parameters are required to be in YYYY-MM-DD format")
        exit(1)
    try:
        date.fromisoformat(args.end_date)
    except ValueError:
        printerr("Wrong end_date parameter format! Date parameters are required to be in YYYY-MM-DD format")
        exit(1)
    
    # Data validation and sanity
    date_range = namedtuple('DateRange', ['start', 'end'])(date.fromisoformat(args.start_date), date.fromisoformat(args.end_date))
    campaign_status = args.campaign_status.strip().upper()

    # ... for main to be called...
    if date_range.start > date_range.end:                   # ... date_range has to be sane... 
        printerr("Cannot go back in time yet (?). start_date <= end_date needed!!!")    
        exit(1)
    elif campaign_status not in CAMPAIGN_VALID_STATUSES:    # ... campaign_status gotta have a valid value
        printerr("Value for campaign_status is invalid. It has to be one among: " + ', '.join(CAMPAIGN_VALID_STATUSES))
        exit(1)
    
    # Selected database validation
    database = args.database.strip().upper()
    if database not in available_dbs:
        printerr("Database not available.")
        printerr("Available Databases:", ', '.join(available_dbs))
        exit(1)
    else:
        main(googleads_client, args.customer_ids, date_range, campaign_status, database)