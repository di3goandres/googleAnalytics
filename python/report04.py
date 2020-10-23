
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
    print('inicio', datetime.datetime.now(), 'Total: ', len(info))
    for analytics in info:

        date_time_obj = datetime.datetime.strptime(
            analytics['ga:date'], '%Y%m%d')
        pdate = date_time_obj.strftime('%Y-%m-%d')
        fechaGuardar = pdate;

        keyword = analytics['ga:fullReferrer']
        keyword2 = analytics['ga:referralPath']

        QUERY = (" IF NOT EXISTS (SELECT * FROM ga_indicador_AllTrafficReferrals WHERE fecha ='" + pdate + "' " +
                 " and country = '" + analytics['ga:country'] + "' and city = '" + analytics['ga:city'] + "' " +

                 " and fullReferrer = '" + keyword + "' and referralPath = '" + keyword2 + "') BEGIN " +

                 "INSERT INTO ga_indicador_AllTrafficReferrals ([fecha] " +
                 ",[newUsers]" +
                 ",[fullReferrer]" +
                 ",[pageviews]" +

                 ",[referralPath]" +
                 ",[medium]" +
                 ",[country]" +
                 ",[city]" +


                 ",[users]" +
                 ",[sessions]" +
                 ",[fecha_creacion], fecha_actualizacion )" +
                 "values ('"+pdate +
                 "', "+analytics['ga:newUsers'] +
                 ",'"+analytics['ga:fullReferrer'] +
                 "',"+analytics['ga:pageviews'] +

                 ",'"+analytics['ga:referralPath'] +
                 "','"+analytics['ga:medium'] +
                 "','"+analytics['ga:country'] +
                 "','"+analytics['ga:city'] +


                 "', "+analytics['ga:users'] +
                 ", " + analytics['ga:sessions'] +
                 ", getdate(), getdate()) END " +
                 "ELSE " +
                 "BEGIN " +
                 "update  [dbo].ga_indicador_AllTrafficReferrals " +
                 "set users = "+analytics['ga:users'] +
                 ",sessions = " + analytics['ga:sessions'] +
                 ",newUsers = " + analytics['ga:newUsers'] +
                 ",pageviews = " + analytics['ga:pageviews'] +
                 ",country = '" + analytics['ga:country'] +
                 "',city = '" + analytics['ga:city'] +

                 

                 "',fecha_actualizacion = getdate() "
                 " where fecha = '" + pdate + "' "
                 " and country = '" + analytics['ga:country'] + "' and city = '" + analytics['ga:city'] + "' " +

                 " and fullReferrer = '" + keyword + "' and referralPath = '" + keyword2 + "' END")

        
        # print(QUERY)
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
                        {'expression': 'ga:sessions'},
                        {'expression': 'ga:newUsers'},
                        {'expression': 'ga:users'},
                        {'expression': 'ga:pageviews'},
                       


                    ],
                    'dimensions': [
                        {'name': "ga:date"},
                        {'name': "ga:medium"},
                        {'name': "ga:fullReferrer"},
                        {'name': "ga:referralPath"},
                        {'name': "ga:country"},
                        {'name': "ga:city"},

                   



                    ]
                }]
        }
    ).execute()

# metodo para obtener la fecha de inicio


def getFechaInicio():

    cursor.execute(
        "SELECT ga_parametros.fecha_inicio FROM ga_parametros where tabla = 'ga_indicador_AllTrafficReferrals'")
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
             fecha + "'  where tabla = 'ga_indicador_AllTrafficReferrals'")
    cursor.execute(QUERY)
    cursor.commit()
