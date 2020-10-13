"""Hello Analytics Reporting API V4."""
import pyodbc
import config
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import report01;
import report02;





server = config.server
endDate = "yesterday";
database =config.database
username = config.username
password = config.password
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = conn.cursor()

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'client.json'
VIEW_ID = config.VIEW_ID


def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics


  






def report_audience_01(analytics):
  response = report01.get_ga_indicador_Audience(analytics)
  datos  = report01.respuesta(response)
  report01.guardar(datos)
  report01.actualizarFecha();


 
def report02_Adquicision(analytics):

  response = report02.reporte(analytics)
  datos  = report02.respuesta(response)
  report02.guardar(datos)
  report02.actualizarFecha();



def main():
  analytics = initialize_analyticsreporting()
  report_audience_01(analytics)
  report02_Adquicision(analytics)


  
        
        

  
  #se obtiene el primer reporte y se guarda en base de datos
  # ()

if __name__ == '__main__':
  main()