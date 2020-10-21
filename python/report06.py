
import pyodbc
import config

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime


endDate = "yesterday"
fechaGuardar = ''
server = config.server
database = config.database
username = config.username
password = config.password
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                      server+';DATABASE='+database+';UID='+username+';PWD=' + password)
cursor = conn.cursor()

VIEW_ID = config.VIEW_ID


def guardar(info):
  
    print('inicio', datetime.datetime.now(), 'Total: ', len(info))

    for analytics in info:

        date_time_obj = datetime.datetime.strptime(
            analytics['ga:date'], '%Y%m%d')
        pdate = date_time_obj.strftime('%Y-%m-%d')
        fechaGuardar = pdate;

        QUERY = (" IF NOT EXISTS (SELECT * FROM ga_indicador_Pages WHERE fecha ='" + pdate + "' ) BEGIN " +
                 

                 "INSERT INTO ga_indicador_Pages ([fecha] " +
                 ",[avgPageLoadTime]" +
                 ",[PageLoadTime]" +
                 ",[avgPageDownloadTime]" +
                 ",[fecha_creacion], fecha_actualizacion )" +
                 "values ('"+pdate +
                 "', "+analytics['ga:avgPageLoadTime'] +
                 ","+analytics['ga:pageLoadTime'] +
                 ","+analytics['ga:avgPageDownloadTime'] +
                 ", getdate(), getdate()) END " +
                 "ELSE " +
                 "BEGIN " +
                 "update  [dbo].ga_indicador_Pages " +
                 "set avgPageLoadTime = "+analytics['ga:avgPageLoadTime'] +
                 ",PageLoadTime = " + analytics['ga:pageLoadTime'] +
                 ",avgPageDownloadTime = " + analytics['ga:avgPageDownloadTime'] +
                 ",fecha_actualizacion = getdate() "
                 " where fecha = '" + pdate + "' END ")

        
        cursor.execute(QUERY)
        conn.commit()
    print('fin', datetime.datetime.now())
    actualizarFecha(fechaGuardar);


def respuesta(response):
    """Parses and prints the Analytics Reporting API V4 response.

    Args:
      response: An Analytics Reporting API V4 response.
    """
    for report in response.get('reports', []):

        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get(
            'metricHeader', {}).get('metricHeaderEntries', [])

        return_data = []

        for row in report.get('data', {}).get('rows', []):

            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])
            pipeline_insert = {}
            for header, dimension in zip(dimensionHeaders, dimensions):
                pipeline_insert[header] = dimension

            for i, values in enumerate(dateRangeValues):

                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    pipeline_insert[metricHeader.get('name')] = value
                return_data.append(pipeline_insert)

        return return_data


def reporte(analytics):
    fecha = getFechaInicio()
    pdate = datetime.datetime.now().strftime('%Y-%m-%d')
    if(fecha == pdate):
        fin = fecha
    else:
        fin = 'yesterday'

    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'dateRanges': [{'startDate': fecha, 'endDate': fin}],
                    'metrics': [
                        {'expression': 'ga:avgPageLoadTime'},
                        {'expression': 'ga:pageLoadTime'},
                        {'expression': 'ga:avgPageDownloadTime'},
                    ],
                    'dimensions': [
                        {'name': "ga:date"}
                    ]
                }]
        }
    ).execute()

# metodo para obtener la fecha de inicio


def getFechaInicio():

    cursor.execute(
        "SELECT ga_parametros.fecha_inicio FROM ga_parametros where tabla = 'ga_indicador_Pages'")
    for row in cursor.fetchall():
        fechainicio = row[0]

    return fechainicio

    # metodo para actualizar la utlima fecha


def actualizarFecha(fecha):
    pdate = datetime.datetime.now().strftime('%Y-%m-%d')
    QUERY = ("UPDATE [dbo].[ga_parametros]  SET [fecha_inicio]='" +
             fecha + "'  where tabla = 'ga_indicador_Pages'")
    cursor.execute(QUERY)
    cursor.commit()
