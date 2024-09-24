import logging


from dc_service_controller import get_handle 
from req_res_controller import get_request_details,set_response_details
from error_response import frame_error_message
import constant as constant
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)
"""
    Desc : Distribution Center Outbound Reporting
    Input: Request query parameter String
    Output:Response JSON Format
"""

def get_request_header_token(method):
    headers = {}

    if method == 'OPTIONS':
        headers['Access-Control-Allow-Origin'] = '*'
        headers['Access-Control-Allow-Methods'] = 'GET'
        headers['Access-Control-Allow-Headers'] = ['Content-Type', 'Authorization']
        headers['Access-Control-Max-Age'] = '3600'  # Cache for max 1 hour = 3600 seconds...
    else:
        headers['Access-Control-Allow-Origin'] = '*'

    return headers    


def dc_outbound_service(request):
    logger.info("dc_outbound_service execution started ")
    try:
        headers = get_request_header_token(request.method)
        if request.method == "OPTIONS":
            return '', '204', headers

        if request.method != "GET":
            return frame_error_message(
                constant.ERROR_CODE_METHOD_NOT_SUPPORTED, constant.ERROR_METHOD_NOT_SUPPORTED
            ), constant.ERROR_CODE_METHOD_NOT_SUPPORTED, headers
        req_data = get_request_details(request)  # Get Request 
        logger.info("dc_outbound_service get request execution completed ")          
        bq_results = get_handle(req_data)        # Query Database    
        print("bqresults",bq_results)     
        if constant.MESSAGE in bq_results and bq_results[constant.MESSAGE]==constant.ERROR_NO_QUERY_FOUND:
            return frame_error_message(
                constant.ERROR_CODE_BAD_REQUEST, constant.ERROR_NO_QUERY_FOUND
            ), constant.ERROR_CODE_BAD_REQUEST, headers
        if constant.MESSAGE in bq_results and bq_results[constant.MESSAGE]==constant.ERROR_INVALID_PATH:
            return frame_error_message(
                constant.ERROR_CODE_BAD_REQUEST, constant.ERROR_INVALID_PATH
            ), constant.ERROR_CODE_BAD_REQUEST, headers
    
        logger.info("dc_outbound_service query databse execution completed ")    
        json_result = set_response_details(bq_results,req_data)  # Set Response  
        if constant.MESSAGE in json_result and json_result[constant.MESSAGE]==constant.ERROR_NO_CONTENT:
            return frame_error_message(
                constant.ERROR_CODE_NOT_FOUND, constant.ERROR_NO_CONTENT
            ), constant.ERROR_CODE_NOT_FOUND, headers
 
        logger.info("dc_outbound_service set response execution completed ")     
        return {"statusCode": constant.HTTP_SUCCESS_CODE_200, "response": json.loads(json_result)}, constant.HTTP_SUCCESS_CODE_200, headers
    except Exception as exp:
        return frame_error_message(constant.ERROR_CODE_GENERIC, exp), constant.ERROR_CODE_GENERIC, headers
