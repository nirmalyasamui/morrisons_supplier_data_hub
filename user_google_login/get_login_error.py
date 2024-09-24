"""
    Desc : Return error message with status code as input
    Input: String
    Output:Response JSON Format

"""


def get_error_message(status_code, exp, to_string=True):
    if to_string:
        exp = str(exp)
    status = {
        400: {
            "statusCode": 400,
            "response": {
                "errorCode": 400,
                "errorMessage": "Bad Request",
                "errorMoreInfo": exp,
            },
        },
        422: {
            "statusCode": 422,
            "response": {
                "errorCode": 422,
                "errorMessage": "Unprocessable Entity",
                "errorMoreInfo": exp,
            },
        },
        404: {
            "statusCode": 404,
            "response": {
                "errorCode": 404,
                "errorMessage": "Client Error",
                "errorMoreInfo": exp,
            },
        },
        403: {
            "statusCode": 403,
            "response": {
                "errorCode": 403,
                "errorMessage": "Client Error",
                "errorMoreInfo": exp,
            },
        },
        500: {
            "statusCode": 500,
            "response": {
                "errorCode": 500,
                "errorMessage": "Internal Server Error",
                "errorMoreInfo": exp,
            },
        },
    }
    return status[status_code]


def map_sdh_error(status_code, message):
    status = {
        200: {"status": 404, "message": None},
        404: {"status": 422, "message": "User is not found in supplier portal"},
    }
    return (
        status[status_code]
        if status_code in status
        else {"status": status_code, "message": message}
    )