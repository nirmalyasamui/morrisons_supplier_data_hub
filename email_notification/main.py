import json
import base64
import os
from email.mime.text import MIMEText
from googleapiclient import discovery
from googleapiclient import errors
from google.oauth2 import service_account
from secret_manager_client import get_secret
import constants as constant
from cloud_storage_client import get_bucket_data
import email_constants as email_constant



"""
Initialize the google drive client
:param config: config client to pass the configuration based on environment
During initialization it tries to connect to the google drive
If any error while connecting then raise GoogleDriveConnectionError
"""

def send_email(body, subject, to_email_address):
    print('into send_email')
    try:
        scopes = ['https://www.googleapis.com/auth/gmail.send']
        project_id = os.environ.get(constant.GCLOUD_PROJECT)
        secret_name = os.environ.get(constant.SECRET_NAME)

        secret_data = json.loads(get_secret(secret_name=secret_name, project_id=project_id))
        #print("-----get_secret_data-----", secret_data)

        credentials = service_account.Credentials.from_service_account_info(secret_data, scopes=scopes)
        sender_email = secret_data[constant.SENDER_MAIL]
        credentials = credentials.with_subject(sender_email)

        #print('sender_email - {}'.format(sender_email))
        service = discovery.build('gmail', 'v1', credentials=credentials, cache_discovery=False)

        email_body = body
        message = MIMEText(email_body, 'html')
        message['from'] = sender_email
        message['to'] = to_email_address
        message['subject'] = subject
        #print('message - {}'.format(message))
        message = {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}
        #print('message again - {}'.format(message))
        message = (service.users().messages().send(userId="me", body=message).execute())
        print('message sent')
        print('message - {}'.format(message))
        return message
    except ValueError as e:
        print('Exception in sending mail - ', e)
        raise ValueError(e)


def get_request_header(method):
    headers = {}
    if method == 'OPTIONS':
        headers['Access-Control-Allow-Origin'] = '*'
        headers['Access-Control-Allow-Methods'] = 'GET'
        headers['Access-Control-Allow-Headers'] = 'Content-Type'
        headers['Access-Control-Max-Age'] = '3600'  # Cache for max 1 hour = 3600 seconds...
    else:
        headers['Access-Control-Allow-Origin'] = '*'

    return headers


def service(request, context):
    try:
        print('context - {}'.format(context))
        req_body = request[email_constant.EMAIL_ATTRIBUTES]
        #print('request body - {}'.format(req_body))
        if req_body is None:
            raise ValueError('No data to send email')
        
        try:
            email_to = req_body[email_constant.EMAIL_ATTRIBUTE_NOTIFY_USER]
            template = req_body[email_constant.EMAIL_ATTRIBUTE_ACTION]
            to_mail_id =  req_body[email_constant.EMAIL_ATTRIBUTE_TO_EMAIL_ID]
        except KeyError:
            js_result = {"errorCode": 400,
                        "errorMessage": "Bad request",
                        "errorMoreInfo": "Not a valid email scenario"}
            return {"statusCode": constant.ERROR_CODE_METHOD_NOT_SUPPORTED, "response": js_result}
        
        #print('template - {}'.format(template))
        config_data = get_bucket_data('email_templates')
        #print('config_data - {}'.format(config_data))
        if email_constant.FIELD_EMAIL_TEMPLATES not in config_data:
            js_result = {"errorCode": 400,
                         "errorMessage": "Bad request",
                         "errorMoreInfo": "Not a valid email scenario"}
            return {"statusCode": constant.ERROR_CODE_METHOD_NOT_SUPPORTED, "response": js_result}
        email_templates = config_data[email_constant.FIELD_EMAIL_TEMPLATES]
        user_email_templates = None if email_constant.EMAIL_TEMPLATE_FIELD_USER not in email_templates else email_templates[email_constant.EMAIL_TEMPLATE_FIELD_USER]
        #print('user_email_templates - {}'.format(user_email_templates))
        admin_email_templates = None if email_constant.EMAIL_TEMPLATE_FIELD_ADMIN not in email_templates else email_templates[email_constant.EMAIL_TEMPLATE_FIELD_ADMIN]
        email_data = {}
        body = None
        subject = None
        if str(email_to).casefold() == email_constant.EMAIL_TEMPLATE_FIELD_USER.casefold():
            email_data = user_email_templates[str(template).casefold()]
        elif str(email_to).casefold() == email_constant.EMAIL_TEMPLATE_FIELD_ADMIN.casefold():
            email_data = admin_email_templates[str(template).casefold()]
        subject = email_data[email_constant.EMAIL_SUBJECT]
        body = email_data[email_constant.EMAIL_BODY]
        if body is not None and subject is not None:
            body = body.replace('<' + email_constant.EMAIL_ATTRIBUTE_RECIPIENT_FORENAME +'>', req_body[email_constant.EMAIL_ATTRIBUTE_RECIPIENT_FORENAME])
            body = body.replace('<' + email_constant.EMAIL_ATTRIBUTE_RECIPIENT_SURNAME + '>', req_body[email_constant.EMAIL_ATTRIBUTE_RECIPIENT_SURNAME])
            body = body.replace('<' + email_constant.EMAIL_ATTRIBUTE_RECIPIENT_REFID + '>', req_body[email_constant.EMAIL_ATTRIBUTE_RECIPIENT_REFID])
            body = body.replace('<' + email_constant.EMAIL_ATTRIBUTE_RECIPIENT_REMARKS + '>', req_body[email_constant.EMAIL_ATTRIBUTE_RECIPIENT_REMARKS])
            body = body.replace('<' + email_constant.EMAIL_ATTRIBUTE_LOGIN_LINK + '>', "https://" + os.environ.get(constant.GCLOUD_PROJECT) + ".firebaseapp.com/login")
            body = body.replace('<' + email_constant.EMAIL_ATTRIBUTE_COMPANY + '>', req_body[email_constant.EMAIL_ATTRIBUTE_COMPANY])
            body = body.replace('<' + email_constant.EMAIL_ATTRIBUTE_WORK_EMAIL_ID + '>', req_body[email_constant.EMAIL_ATTRIBUTE_WORK_EMAIL_ID])
            body = body.replace('<' + email_constant.EMAIL_ATTRIBUTE_JOB_ROLE + '>', req_body[email_constant.EMAIL_ATTRIBUTE_JOB_ROLE])
            body = body.replace('<' + email_constant.EMAIL_ATTRIBUTE_USER_TYPE + '>', req_body[email_constant.EMAIL_ATTRIBUTE_USER_TYPE])
            body = body.replace('<' + email_constant.EMAIL_ATTRIBUTE_GRANT_ACCESS_LINK + '>', req_body[email_constant.EMAIL_ATTRIBUTE_GRANT_ACCESS_LINK])
            body = body.replace('<' + email_constant.EMAIL_ATTRIBUTE_INACTIVE_DAYS + '>', req_body[email_constant.EMAIL_ATTRIBUTE_INACTIVE_DAYS])

            subject = subject.replace('<' + email_constant.EMAIL_ATTRIBUTE_RECIPIENT_FORENAME +'>', req_body[email_constant.EMAIL_ATTRIBUTE_RECIPIENT_FORENAME])
            subject = subject.replace('<' + email_constant.EMAIL_ATTRIBUTE_RECIPIENT_SURNAME + '>', req_body[email_constant.EMAIL_ATTRIBUTE_RECIPIENT_SURNAME])
            #print('body - {}'.format(body))
            
            message = send_email(body, subject, to_mail_id)
            return {"statusCode": constant.SUCCESS_CODE, "response": {"message": message}}
    
    except Exception as e:
        js_result = {"errorCode": constant.ERROR_CODE_INTERNAL_ERROR,
                     "errorMessage": constant.ERROR_INTERNAL_SERVER,
                     "errorMoreInfo": e}
        return {"statusCode": constant.ERROR_CODE_METHOD_NOT_SUPPORTED, "response": js_result}