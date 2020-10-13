import pyodbc
import config

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime


endDate = "yesterday";

server = config.server
database =config.database
username = config.username
password = config.password
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = conn.cursor()

VIEW_ID = config.VIEW_ID

def guardar(info):
    print('inicio', datetime.datetime.now(), 'Total: ', len(info))
    for analytics in info:
      
        date_time_obj = datetime.datetime.strptime(analytics['fecha'], '%Y%m%d')
        pdate = date_time_obj.strftime('%Y-%m-%d')
       
        QUERY  = (" IF NOT EXISTS (SELECT * FROM ga_indicador_Audience WHERE fecha ='" + pdate +"') BEGIN " +
     
        "INSERT INTO ga_indicador_Audience ([fecha] " +
        ",[users]" +
        ",[sessions]" +
        ",[fecha_creacion], fecha_actualizacion )" +
        "values ('"+pdate+
        "', "+analytics['ga:users']+
        ", " +analytics['ga:sessions']+
        ", getdate(), getdate()) END " +
        "ELSE " +
        "BEGIN " +
     
        "update  [dbo].ga_indicador_Audience " +
        "set users = "+analytics['ga:users']+
        ",sessions = " +analytics['ga:sessions']+
        ",fecha_actualizacion = getdate() END")
     
        cursor.execute(QUERY)
        conn.commit()
    print('fin', datetime.datetime.now())


def respuesta(response):
  """Parses and prints the Analytics Reporting API V4 response.

  Args:
    response: An Analytics Reporting API V4 response.
  """
  for report in response.get('reports', []):

    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    return_data = []

    for row in report.get('data', {}).get('rows', []):
      
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])
      pipeline_insert = {}
      for header, dimension in zip(dimensionHeaders, dimensions):
        pipeline_insert['fecha'] =dimension
      


      for i, values in enumerate(dateRangeValues):
     
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          pipeline_insert[metricHeader.get('name')] = value
        return_data.append(pipeline_insert)

    return return_data;



def get_ga_indicador_Audience(analytics, fecha, fin):
    """Queries the Analytics Reporting API V4.

    Args:
    analytics: An authorized Analytics Reporting API V4 service object.
    Returns:
    The Analytics Reporting API V4 response.
    """


    return analytics.reports().batchGet(
        body={
        'reportRequests': [
        {
            'viewId': VIEW_ID,
            'dateRanges': [{'startDate': fecha, 'endDate': fin}],
            'metrics': [
            {'expression': 'ga:sessions'},
            {'expression':'ga:users'},

            ],
            'dimensions': [{'name': "ga:date"}]
        }]
        }
    ).execute()

