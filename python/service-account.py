from  oauth2client.service_account import ServiceAccountCredentials
# from google.oauth2.service_account import ServiceAccountCredentials


# The scope for the OAuth2 request.
SCOPE = 'https://www.googleapis.com/auth/analytics.readonly'

# The location of the key file with the key data.
KEY_FILEPATH = 'client.json'

# Defines a method to get an access token from the ServiceAccount object.
def get_access_token():
  
    data = ServiceAccountCredentials.from_json_keyfile_name(
       KEY_FILEPATH, SCOPE).get_access_token() 
    print(data)


get_access_token()
