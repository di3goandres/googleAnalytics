"""Hello Analytics Reporting API V4."""
import pyodbc
import config
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import report01;




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

# metodo para obtener la fecha de inicio
def getFechaInicio():
 
  cursor.execute("SELECT ga_parametros.fecha_inicio FROM ga_parametros")
  for row in cursor.fetchall():
    fechainicio = row[0];

  
  return fechainicio;
  


# metodo para actualizar la utlima fecha
def actualizarFecha():     
  pdate = datetime.datetime.now().strftime('%Y-%m-%d')
  QUERY  = ("UPDATE [dbo].[ga_parametros]  SET [fecha_inicio]='" + pdate +"'") 
  cursor.execute(QUERY)
  cursor.commit()





def report_audience_01(analytics, fecha, end):
  response = report01.get_ga_indicador_Audience(analytics, fecha, end)
  datos  = report01.respuesta(response)
  report01.guardar(datos)

 



def main():
  
  fecha= getFechaInicio()
  pdate = datetime.datetime.now().strftime('%Y-%m-%d')  
  analytics = initialize_analyticsreporting()

  if(fecha == pdate):
    print('iguales')
    endDate = fecha
    print('iguales', endDate)
    report_audience_01(analytics, fecha, fecha)
  else:
     report_audience_01(analytics, fecha, 'yesterday')
        
        

  
  #se obtiene el primer reporte y se guarda en base de datos
  actualizarFecha()

if __name__ == '__main__':
  main()