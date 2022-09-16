# -*- coding: utf-8 -*-
"""
Archivo de configuración
"""

import os

CURRENT_PATH = os.getcwd()


LOGGING_PATH = os.path.join('/ODI/marketing/etl_adwords/', 'logs')
TMP_PATH = os.path.join('/ODI/marketing/etl_adwords/', 'temp')

if not os.path.exists(TMP_PATH):
    os.makedirs(TMP_PATH)
if not os.path.exists(LOGGING_PATH):
    os.makedirs(LOGGING_PATH)


# -------- GOOGLE ANALYTICS ACCOUNT -------------
##SCOPES = 'https://www.googleapis.com/auth/analytics.readonly'
##DISCOVERY_URI = 'https://analyticsreporting.googleapis.com/$discovery/rest'
##KEY_FILE_LOCATION = os.path.join('/App/projects/etls/etl_ga', 'config', '.p12')
##SERVICE_ACCOUNT_EMAIL = ''
##VIEW_ID = '63123287' # fravega FULL:25171469  // App Fravega: '100617471 // fravega.com: 63123287' https://ga-dev-tools.appspot.com/account-explorer/?hl=es
##


# -------- ADWORDS REQUEST -------------

REPORTES = {
  'keyword' :
  {
   'report_definition':
       {
      'reportName': 'Custom KEYWORDS_PERFORMANCE_REPORT',
      'dateRangeType': 'CUSTOM_DATE',
      'reportType': 'KEYWORDS_PERFORMANCE_REPORT',
      'downloadFormat': 'CSV',
      'selector': {
          'fields': ['AccountDescriptiveName', 'Date', 'Device', 'CampaignName', 'Criteria',
                          'AdGroupName', 'Status', 'KeywordMatchType', 'CpcBid', 'Clicks',
                          'Impressions', 'AverageCpc', 'Ctr', 'Cost', 'AveragePosition',
                          'QualityScore', 'Labels', 'SearchImpressionShare', 'SearchRankLostImpressionShare',
                          'SearchExactMatchImpressionShare', 'Conversions', 'AllConversions',
                          'CrossDeviceConversions', 'ConversionValue', 'AllConversionValue'
                          ,'VideoQuartile100Rate','VideoQuartile75Rate','VideoQuartile50Rate'
                          #,'AveragePageviews'
                          ,'VideoViews'
                          ],
          'dateRange': {'min': 20180125, 'max': 20180129},
          'predicates': {
              'field': 'AdGroupStatus',
              'operator': 'IN',
              'values': ['ENABLED', 'PAUSED'] }
          },
      },

   'download_directory': '/ODI/marketing/etl_adwords/reports/keyword',
   'credentials_directory': '/ODI/marketing/etl_adwords/config/googleads.yaml',
   #'download_directory': '/App/projects/etls/etl_adwords/reports/keyword',
   #'credentials_directory': '/App/projects/etls/etl_adwords/config/googleads.yaml',
   'file_name': 'keyword',
   'db_target': 'db_ec_dwh_dev',
   'db_table': 'mkt_adwords_keywords'
   },

  'ad_performance':
    {
   'report_definition':
       {
      'reportName': 'Custom AD_PERFORMANCE_REPORT',
      'dateRangeType': 'CUSTOM_DATE',
      'reportType': 'AD_PERFORMANCE_REPORT',
      'downloadFormat': 'CSV',
      'selector': {
          'fields': ['AccountDescriptiveName', 'Date', 'Device', 'CampaignName',
                          'AdGroupName',
                          #quito el Id porque hace que se abra y no se usa
                          'Id',
                          'AdType',  'Headline', 'ImageCreativeName',
                          'Clicks', 'Impressions', 'Ctr', 'AverageCpc', 'AverageCpm',
                          'Cost', 'AveragePosition', 'CreativeFinalUrls',
                          'ExternalCustomerId', 'CreativeDestinationUrl',
                          'CreativeFinalMobileUrls', 'Status', 'Conversions',
                          'AllConversionValue', 'CrossDeviceConversions', 'AllConversions',
                          'ConversionValue','VideoQuartile100Rate','VideoQuartile75Rate','VideoQuartile50Rate'
                          #'AveragePageviews' #,'webpage_view'
                          ,'VideoViews'
                          #no puedo mezclar AveragePageviews con Device, VideoViews viene en gral en 0...
                          ],
          'dateRange': {'min': 20180125, 'max': 20180129},
          # Predicates are optional.
          'predicates': {
              'field': 'AdGroupStatus',
              'operator': 'IN',
              'values': ['ENABLED', 'PAUSED'] }
          },
        },
    'download_directory': '/ODI/marketing/etl_adwords/reports/ad_performance',
   #'download_directory': '/App/projects/etls/etl_adwords/reports/ad_performance',
    'credentials_directory': '/ODI/marketing/etl_adwords/config/googleads.yaml',
   #'credentials_directory': '/App/projects/etls/etl_adwords/config/googleads.yaml',
   'file_name': 'ad_performance',
   'db_target': 'db_ec_dwh_dev',
   'db_table': 'mkt_adwords_ad_performance'
   },
  }

# -------- ERROR EMAILS -------------
ERROR_EMAIL = {
    'send_from': 'eCommerce AutoReports',
    'send_to': ['biecommerce@fravega.com.ar'],
    'subject': '[Error] ',
    'body': 'Este es un email de aviso de Error.'
}



# -------- Logging -----------
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': '%(asctime)s [%(levelname)s] %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level':'DEBUG',
            'class':'logging.StreamHandler',
            'formatter': 'simple',
            'stream': 'ext://sys.stdout'
        },
        'rotating_file': {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "simple",
            "filename": os.path.join(LOGGING_PATH, 'log.log'),
            "maxBytes": 10485760, # 10 MB
            "backupCount": 3,
            "encoding": "utf-8"
        },
    },
    'loggers': {
        '': {
            'handlers': ['console', 'rotating_file'],
            'level': 'DEBUG',
            'propagate': True
        }
    }
}
