import pyodbc
import config

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime
from datetime import timedelta


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
    FINAL = ''
    conteo = 0;
    total = len(info)
    print('inicio', datetime.datetime.now(), 'Total: ', len(info))
    for analytics in info:
        conteo = conteo + 1

        date_time_obj = datetime.datetime.strptime(
            analytics['fecha'], '%Y%m%d')
        pdate = date_time_obj.strftime('%Y-%m-%d')
        fechaGuardar = pdate;

        QUERY = (" IF NOT EXISTS (SELECT * FROM ga_indicador_Audience WHERE fecha ='" + pdate + "') BEGIN " +

                 "INSERT INTO ga_indicador_Audience ([fecha] " +
                 ",[users]" +
                 ",[sessions]" +
                 ",[newUsers]" +
                 ",[bounceRate]" +
                 ",[fecha_creacion], fecha_actualizacion )" +
                 "values ('"+pdate +
                 "',"+analytics['ga:users'] +
                 ","+analytics['ga:sessions'] +
                 ","+analytics['ga:newUsers'] +
                 ","+analytics['ga:bounceRate'] +


                 ", getdate(), getdate()) END " +
                 "ELSE " +
                 "BEGIN " +

                 "update  [dbo].ga_indicador_Audience " +
                 "set users = "+analytics['ga:users'] +
                 ",sessions = " + analytics['ga:sessions'] +
                 ",bounceRate = " + analytics['ga:bounceRate'] +
                 ",newUsers = " + analytics['ga:newUsers'] +
                 ",fecha_actualizacion = getdate() "
                 " where fecha = '" + pdate + "'   END")
        FINAL = FINAL  +  QUERY;
        if(total==1000):
            if(conteo%100==0):
                print('guardando 100')
                cursor.execute(FINAL)
                conn.commit()
                FINAL = ''
        else:
            cursor.execute(QUERY)
            conn.commit()
        
        
    #print(FINAL)
    print('fin', datetime.datetime.now())
    print('fechaGuardar', fechaGuardar)

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
                pipeline_insert['fecha'] = dimension

            for i, values in enumerate(dateRangeValues):

                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    pipeline_insert[metricHeader.get('name')] = value
                return_data.append(pipeline_insert)

        return return_data


def get_ga_indicador_Audience(analytics):
    """Queries the Analytics Reporting API V4.

    Args:
    analytics: An authorized Analytics Reporting API V4 service object.
    Returns:
    The Analytics Reporting API V4 response.
    """
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
                        {'expression': 'ga:sessions'},
                        {'expression': 'ga:users'},
                        {'expression': 'ga:newUsers'},
                        {'expression': 'ga:bounceRate'},
                    ],
                    'dimensions': [{'name': "ga:date"}]
                }]
        }
    ).execute()

# metodo para obtener la fecha de inicio


def getFechaInicio():

    cursor.execute(
        "SELECT ga_parametros.fecha_inicio FROM ga_parametros where tabla = 'ga_indicador_Audience'")
    for row in cursor.fetchall():
        fechainicio = row[0]

    return fechainicio

    # metodo para actualizar la utlima fecha


def actualizarFecha(fecha):
    new_date = datetime.datetime.today() + timedelta(-1)
    new_date = new_date.strftime('%Y-%m-%d')
    if fecha == new_date:
      pdate = datetime.datetime.now().strftime('%Y-%m-%d')
      fecha = pdate;

    
    QUERY = ("UPDATE [dbo].[ga_parametros]  SET [fecha_inicio]='" +
             fecha + "'  where tabla = 'ga_indicador_Audience'")
    cursor.execute(QUERY)
    cursor.commit()
