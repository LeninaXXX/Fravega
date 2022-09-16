#!/bin/python3
# ------------- viejo !/usr/bin/env python
# -*- coding: cp1252 -*-
import logging
import multiprocessing
#from Queue import Empty #lo saque antes para que funcionara, lo volvi a poner a ver que onda...
from queue import Queue, Empty
import time
import googleads
from config import settings
import sys
import os
import datetime
from datetime import timedelta
from utils import utils

#agregue esto por si faltaba importar
#import requests
#import request
#import urllib
try: #python3
    from urllib.request import urlopen
except: #python2
    from urllib2 import urlopen

"""
Baja informacion de Adwords, la deja en un csv en /report/%s
Al final llama proceso que parsea los csv

Parametros:
- report_name (keyword, ad_performance)
- start_date (yyyymmdd)
- end_date (yyyymmdd)

Si no tiene parametros de fechas, toma la fecha de ayer


"""

#logging.basicConfig(level=logging.DEBUG)
#logging.getLogger('suds.transport').setLevel(logging.DEBUG)

# Timeout between retries in seconds.
BACKOFF_FACTOR = 5
# Maximum number of processes to spawn.
MAX_PROCESSES = multiprocessing.cpu_count()
# Maximum number of retries for 500 errors.
MAX_RETRIES = 5
# Maximum number of items to be sent in a single API response.
PAGE_SIZE = 100


def main(client, report_download_directory, report_definition):
  # Determine list of customer IDs to retrieve report for.
  input_queue = GetCustomerIDs(client)
  reports_succeeded = multiprocessing.Queue()
  reports_failed = multiprocessing.Queue()

  queue_size = input_queue.qsize()
  num_processes = min(queue_size, MAX_PROCESSES)
##  print 'Retrieving %d reports with %d processes:' % (queue_size, num_processes)

  # Start all the processes.
  processes = [ReportWorker(client, report_download_directory,
                            report_definition, input_queue, reports_succeeded,
                            reports_failed)
               for _ in range(num_processes)]

  for process in processes:
    process.start()

  for process in processes:
    process.join()

  #print ("Finished downloading reports with the following results:")
  logging.info("Finished downloading reports with the following results:")

  while True:
    try:
      success = reports_succeeded.get(timeout=0.01)
    except Empty:
      break
    #print ('\tReport for CustomerId "%d" succeeded.' % success['customerId'])
    logging.info('\tReport for CustomerId "%d" succeeded.' % success['customerId'])

  while True:
    try:
      failure = reports_failed.get(timeout=0.01)
    except Empty:
      break
    #print ('\tReport for CustomerId "%d" failed with error code "%s" and '
    #       'message: %s.' % (failure['customerId'], failure['code'],
    #                         failure['message']))
    logging.error('\tReport for CustomerId "%d" failed with error code "%s" and '
            'message: %s.' % (failure['customerId'], failure['code'],
                              failure['message']))

class ReportWorker(multiprocessing.Process):
  """A worker Process used to download reports for a set of customer IDs."""

  _FILENAME_TEMPLATE = 'adwords_%s.csv'
  _FILEPATH_TEMPLATE = '%s/%s'

  def __init__(self, client, report_download_directory, report_definition,
                 input_queue, success_queue, failure_queue):
    """Initializes a ReportWorker.

    Args:
      client: An AdWordsClient instance.
      report_download_directory: A string indicating the directory where you
        would like to download the reports.
      report_definition: A dict containing the report definition that you would
        like to run against all customer IDs in the input_queue.
      input_queue: A Queue instance containing all of the customer IDs that
        the report_definition will be run against.
      success_queue: A Queue instance that the details of successful report
        downloads will be saved to.
      failure_queue: A Queue instance that the details of failed report
        downloads will be saved to.
    """
    super(ReportWorker, self).__init__()
#    self.report_downloader = client.GetReportDownloader(version='v201806')
    self.report_downloader = client.GetReportDownloader(version='v201809')
    self.report_download_directory = report_download_directory
    self.report_definition = report_definition
    self.input_queue = input_queue
    self.success_queue = success_queue
    self.failure_queue = failure_queue


  def _DownloadReport(self, customer_id):
    filepath = self._FILEPATH_TEMPLATE % (self.report_download_directory,
                                          self._FILENAME_TEMPLATE % customer_id)
                                          #self._FILENAME_TEMPLATE % (customer_id, datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')))

    retry_count = 0

    while True:
##      print ('[%d/%d] Loading report for customer ID "%s" into "%s"...'
##             % (self.ident, retry_count, customer_id, filepath))
      try:
        with open(filepath, 'wb') as handler:
          self.report_downloader.DownloadReport(
              self.report_definition, output=handler,
              client_customer_id=customer_id)
        return (True, {'customerId': customer_id})
      except googleads.errors.AdWordsReportError:#, e: renglon modif
        if e.code == 500 and retry_count < MAX_RETRIES:
          time.sleep(retry_count * BACKOFF_FACTOR)
        else:
##          print ('Report failed for customer ID "%s" with code "%d" after "%d" '
##                 'retries.' % (customer_id, e.code, retry_count+1))
          return (False, {'customerId': customer_id, 'code': e.code,
                          'message': e.message})
      except Exception:#, e: renglon modif
##        print 'Report failed for customer ID "%s".' % customer_id
##        print 'e: %s' % e.__class__
        return (False, {'customerId': customer_id, 'code': None,
                        'message': e.message})

  def run(self):
    while True:
      try:
        customer_id = self.input_queue.get(timeout=0.01)
      except Empty:
        break
      result = self._DownloadReport(customer_id)
      (self.success_queue if result[0] else self.failure_queue).put(result[1])


def GetCustomerIDs(client):
  """Retrieves all CustomerIds in the account hierarchy.

  Note that your configuration file must specify a client_customer_id belonging
  to an AdWords manager account.

  Args:
    client: an AdWordsClient instance.

  Raises:
    Exception: if no CustomerIds could be found.

  Returns:
    A Queue instance containing all CustomerIds in the account hierarchy.
  """
  # For this example, we will use ManagedCustomerService to get all IDs in
  # hierarchy that do not belong to MCC accounts.
  managed_customer_service = client.GetService('ManagedCustomerService')
#                                               version='v201806')
#  managed_customer_service = client.GetService('ManagedCustomerService',



  offset = 0

  # Get the account hierarchy for this account.
  selector = {'fields': ['CustomerId'],
              'predicates': [{
                  'field': 'CanManageClients',
                  'operator': 'EQUALS',
                  'values': [False]
              }],
              'paging': {
                  'startIndex': str(offset),
                  'numberResults': str(PAGE_SIZE)}}

  # Using Queue to balance load between processes.
  queue = multiprocessing.Queue()
  more_pages = True

  while more_pages:
    page = managed_customer_service.get(selector)

    if page and 'entries' in page and page['entries']:
      for entry in page['entries']:
        queue.put(entry['customerId'])
    else:
      raise Exception('Can\'t retrieve any customer ID.')
    offset += PAGE_SIZE
    selector['paging']['startIndex'] = str(offset)
    more_pages = offset < int(page['totalNumEntries'])

  return queue


# --------- Inicializaci�n --------------

# logger
logging.getLogger("petl").setLevel(logging.WARNING)
logging.config.dictConfig(settings.LOGGING_CONFIG)
logging.info(u"Inicia proceso %s" % (sys.argv[0]))

#print("Hace algo?")

# manejo de parametros
try:
    report_name = sys.argv[1]
    #print("Probando 1er parametro",sys.argv[1])
    logging.info("Probando 1er parametro %s" % sys.argv[1])

except IndexError:
    logging.critical(u"No hay parametro de reporte.")
    sys.exit()

try:
    #print("Probando 2do parametro",sys.argv[2])
    logging.info("Probando 2do parametro %s" % sys.argv[2])

    start_date = sys.argv[2]

    #print("Probando 3er parametro",sys.argv[3])
    logging.info("Probando 3er parametro %s" % sys.argv[3])
    end_date = sys.argv[3]

except IndexError:
    #hoy = datetime.date.today()
    ayer = datetime.date.today() - timedelta(1)
    start_date = ayer.strftime("%Y%m%d")
    end_date = ayer.strftime("%Y%m%d")
    #print ("Fechas OK")
    logging.info("Fechas OK")
except ValueError:
    logging.critical(u"No se reconocen parametros de fecha correctos")
    #print("Error en Fechas")
    logging.critical("Error en Fechas")
    #TODO: enviar mail con notificacion de error
    sys.exit(1)

##fecha_ini = 20180201
##fecha_fin = 20180201

#print("leyendo definiciones de reportes en settings")
logging.info("leyendo definiciones de reportes en settings")
REPORT_DEFINITION = settings.REPORTES.get(report_name).get('report_definition')
REPORT_DEFINITION.get('selector')['dateRange']={'max': end_date, 'min': start_date}

REPORT_DOWNLOAD_DIRECTORY = settings.REPORTES.get(report_name).get('download_directory')
CREDENTIALS = settings.REPORTES.get(report_name).get('credentials_directory')
FILE_NAME = settings.REPORTES.get(report_name).get('file_name')
#print("definiciones OK")
logging.info("definiciones OK")

try:

  if __name__ == '__main__':
    ADWORDS_CLIENT = googleads.adwords.AdWordsClient.LoadFromStorage(path= CREDENTIALS)
    main(ADWORDS_CLIENT, REPORT_DOWNLOAD_DIRECTORY, REPORT_DEFINITION)


  #LLAMA AL PARSEADOR, FUNCIONA PERO LA INTERACCION PYTHON  -> ORACLE ES LENTO, POR ESO COMENTO
  #print("llamando al parseador del reporte (parse_repors.py)")
  #os.system("/usr/bin/python /App/projects/etls/etl_adwords/parse_reports.py %s" % (report_name) )
  #os.system("/usr/bin/python /ODI/marketing/etl_adwords/parse_reports.py %s" % (report_name) )
  #print ("parseo ok,fin")


#ante un error envio un email de alerta
except BaseException:
    logging.critical(u"Error crítico - No se pudo completar la operación", exc_info=True)
    #utils.send_mail(send_from=settings.ERROR_EMAIL['send_from'],
                   #send_to=settings.ERROR_EMAIL['send_to'],
                   # subject=settings.ERROR_EMAIL['subject'] + sys.argv[0] + ' ' + sys.argv[1],
                   # text=settings.ERROR_EMAIL['body'] + '<br>' + 'Ubicado en ' + sys.argv[0])
    sys.exit(1)