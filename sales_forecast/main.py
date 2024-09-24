from flask import abort  # HTTP helper from flask...
from commonutil import CommonUtil
import constant as constant
from salesforecastservicehandle import SalesForecastServiceHandle as handle


def service(request):
    # Set CORS headers for the preflight request
    headers = CommonUtil.get_request_header_token(request.method)
    if request.method == constant.HTTP_OPTIONS:
        return '', constant.HTTP_SUCCESS_CODE_204, headers
    request_dictionary = CommonUtil.get_request_details(request)

    method = request_dictionary[constant.METHOD]
    if method == constant.HTTP_GET:
        json_result = handle.get_handle(request_dictionary)
        if json_result[constant.MESSAGE] == constant.ERROR_FOREECAST_NOT_AVAILABLE:
            return CommonUtil.frame_error_message(constant.ERROR_FOREECAST_NOT_AVAILABLE, json_result,
                                                  constant.ERROR_CODE_NO_DATA_FOUND), constant.ERROR_CODE_NO_DATA_FOUND, headers
        
        elif json_result[constant.MESSAGE] == constant.ERROR_NO_DATA_FOUND:
            return CommonUtil.frame_error_message(constant.ERROR_NO_DATA_FOUND, json_result,
                                                  constant.ERROR_CODE_NO_DATA_FOUND), constant.ERROR_CODE_NO_DATA_FOUND, headers

        elif json_result[constant.MESSAGE] == constant.ERROR_NO_VENDOR_SPECIFIED or json_result[constant.MESSAGE] == constant.ERROR_NO_USERID_SPECIFIED:
            # Unprocessed Entity
            return CommonUtil.frame_error_message(constant.ERROR_UNPROCESSABLE_ENTITY, json_result,
                                                  constant.ERROR_CODE_UNPROCESSABLE_ENTITY), constant.ERROR_CODE_UNPROCESSABLE_ENTITY, headers
        
        elif json_result[constant.MESSAGE] == constant.ERROR_INVALID_PATH:
            # Bad request
            return CommonUtil.frame_error_message(constant.ERROR_INVALID_PATH, json_result,
                                                          constant.ERROR_CODE_BAD_REQUEST), constant.ERROR_CODE_BAD_REQUEST, headers
        elif json_result[constant.MESSAGE] == constant.ERROR_RESPONSE_TOO_LARGE:
            # Bad request
            return CommonUtil.frame_error_message(constant.ERROR_RESPONSE_TOO_LARGE, json_result,
                                                  constant.ERROR_CODE_ALREADY_FOUND), constant.ERROR_CODE_ALREADY_FOUND, headers          
        elif json_result[constant.MESSAGE] == constant.ERROR_INTERNAL_SERVER:
            return CommonUtil.frame_error_message(constant.ERROR_INTERNAL_SERVER, json_result,
                                                  constant.ERROR_CODE_INTERNAL_SERVER), constant.ERROR_CODE_INTERNAL_SERVER, headers
                                                  
        # Success
        return CommonUtil.frame_success_message(json_result), constant.HTTP_SUCCESS_CODE_200, headers
    else:
        return CommonUtil.frame_error_message(constant.ERROR_METHOD_NOT_SUPPORTED,
                                              constant.ERROR_METHOD_NOT_SUPPORTED,
                                              constant.ERROR_CODE_METHOD_NOT_SUPPORTED), constant.ERROR_CODE_METHOD_NOT_SUPPORTED, headers
