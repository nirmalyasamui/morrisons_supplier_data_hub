import json
from flask import abort
import constant as constants
from documentsservicehandle import DocumentsServiceHandle as handle
import google.cloud.logging as loggers

client = loggers.Client()
client.setup_logging()

import logging


def get_request_header(method):
    #test 
    headers = {}
    if method == 'OPTIONS':
        headers['Access-Control-Allow-Origin'] = '*'
        headers['Access-Control-Allow-Methods'] = ["GET", "POST", "OPTIONS", "PUT", "DELETE", "PATCH"]
        headers['Access-Control-Allow-Headers'] = ['Content-Type', 'Authorization']
        headers['Access-Control-Max-Age'] = '3600'  # Cache for max 1 hour = 3600 seconds....
    else:
        headers['Access-Control-Allow-Origin'] = '*'

    return headers

def invoke_post_apis(request, headers, url):
    request_json = request.get_json()
    if url.endswith('/documents/addfile'):
        return handle.save_document_details(request_json), headers
    if url.endswith('/documents/addcategory'):
        return handle.add_document_category(request_json), headers

def invoke_delete_apis(request, headers, url):
    if url.endswith('/documents/removefile'):
        return handle.delete_metadata(request), headers

def invoke_get_apis(request, headers, url):
    if url.endswith('/documents/category'):
        return handle.get_category(request), headers
    if url.endswith('/documents/details'):
        return handle.get_document_details(request), headers

def service(request):
    # Set CORS headers for the preflight request
    logging.info('request in main- {}'.format(request.get_json()))
    try:
        headers = get_request_header(request.method)
        url = str(request.url).split('?')[0]

        if request.method == constants.HTTP_METHOD_OPTIONS:
            return '', constants.OPTIONS_SUCCESS, headers
            
        method = request.method
        if method == constants.HTTP_METHOD_POST:
            return invoke_post_apis(request, headers, url)
        elif method == constants.HTTP_METHOD_DELETE:
            return invoke_delete_apis(request, headers, url)
        elif method == constants.HTTP_METHOD_GET:
            return invoke_get_apis(request, headers, url)
        else:
            js_result = {"errorCode": constants.ERROR_CODE_METHOD_NOT_SUPPORTED,
                         "errorMessage": constants.ERROR_METHOD_NOT_SUPPORTED,
                         "errorMoreInfo": constants.ERROR_METHOD_NOT_SUPPORTED}
            return {"statusCode": constants.ERROR_CODE_METHOD_NOT_SUPPORTED, "response": js_result}, constants.ERROR_CODE_METHOD_NOT_SUPPORTED, headers
    except Exception as e:
        logging.error('in  exception block::::{}'.format(str(e)))
        js_result = {"errorCode": constants.ERROR_CODE_INTERNAL_ERROR,
                     "errorMessage": constants.ERROR_INTERNAL_SERVER,
                     "errorMoreInfo": e}
        return {"statusCode": constants.ERROR_CODE_METHOD_NOT_SUPPORTED, "response": js_result}, constants.ERROR_CODE_METHOD_NOT_SUPPORTED, headers
