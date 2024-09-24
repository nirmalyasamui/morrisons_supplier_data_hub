"""
    Desc : Return error message with status code as input
    Input: String
    Output:Response JSON Format

"""


def frame_error_message(status_code, exp):
    message=str(exp)
    message=message[:250]
    status = {
        204: {
            "statusCode": 204,
            "response": {
                "errorCode": 204,
                "errorMessage": "No Content",
                "errorMoreInfo": message,
            },
        },
        400: {
            "statusCode": 400,
            "response": {
                "errorCode": 400,
                "errorMessage": "Bad Request",
                "errorMoreInfo": message,
            },
        },
        403: {
            "statusCode": 403,
            "response": {
                "errorCode": 403,
                "errorMessage": "Client Error",
                "errorMoreInfo": message,
            },
        },
        404: {
            "statusCode": 404,
            "response": {
                "errorCode": 404,
                "errorMessage": "Not Found",
                "errorMoreInfo": message,
            },
        },
        405: {
            "statusCode": 405,
            "response": {
                "errorCode": 405,
                "errorMessage": "Client Error",
                "errorMoreInfo": message,
            },
        },
        500: {
            "statusCode": 500,
            "response": {
                "errorCode": 500,
                "errorMessage": "Internal Server Error",
                "errorMoreInfo": message,
            },
        },
    }
    return status[status_code]
