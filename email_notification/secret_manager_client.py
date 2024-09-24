from google.cloud import secretmanager

def get_secret(secret_name, project_id):
    try:
        print('into get secret method - ', secret_name)
        client = secretmanager.SecretManagerServiceClient()
        name = "projects/" + project_id + "/secrets/" + secret_name + "/versions/1"
        #print('b4 response:::::')
        response = client.access_secret_version(name=name)
        #print('response - ', response)
        secret_string = response.payload.data.decode("UTF-8")
        #print('secret_string - ', secret_string)
        return secret_string
    except TypeError as error:
        print('Exception in reading the secret values - ', error)
        raise TypeError(error)


