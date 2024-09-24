import logging
import os

from google.cloud import firestore

logger = logging.getLogger()
logger.setLevel(logging.INFO)
"""
    Desc : Update Field accepted_terms_and_conditions in Firestore collection user
    Input: Request JSON format
    Output:Response JSON Format
"""
def get_request_header_token(method):
    headers = {}
    print('method - {}'.format(method))
    if method == 'OPTIONS':
        headers['Access-Control-Allow-Origin'] = '*'
        headers['Access-Control-Allow-Methods'] = ["GET", "POST", "OPTIONS", "PUT", "DELETE", "PATCH"]
        headers['Access-Control-Allow-Headers'] = ['Content-Type', 'Authorization']
        headers['Access-Control-Max-Age'] = '3600'  # Cache for max 1 hour = 3600 seconds...
    else:
        headers['Access-Control-Allow-Origin'] = '*'

    return headers

def accept_terms_conditions(request):
    logger.info("accept_terms_conditions execution started ")
    try:
        headers = get_request_header_token(request.method)
        if request.method == "OPTIONS":
            print('i am here in options:::::::')
            return '', 204 , headers

        input_data = {}  # To store selected input from request
        status_code = 200
        doc_id = None
        req_data = request.get_json()
        collection_name = os.environ.get("USER_COLLECTION")

        if req_data:
            input_data["accepted_terms_and_conditions"] = "Y"
            input_data["updated_date"] = firestore.SERVER_TIMESTAMP
            if "googleAccountId" in req_data:
                user_id = req_data["googleAccountId"]
            else:
                raise ValueError("googleAccountId Not Found")
        else:
            raise ValueError("Request is invalid")
        filter_field = "google_id"
        filter_value = user_id
        operator = "=="
        db = firestore.Client()
        collection_ref = db.collection(collection_name)
        query_ref = collection_ref.where(filter_field, operator, filter_value)
        docs = query_ref.stream()

        for doc in docs:
            if doc.exists:
                doc_id = doc.id
        if doc_id is None:
            return {
                "statusCode": 404,
                "response": {"errorCode": 404, "errorMessage": "No data found", "errorMoreInfo": "No match for google account id found"},
            }, 404, headers
        doc_ref = db.collection(collection_name).document(doc_id)
        doc_ref.update(input_data)
        return {
            "statusCode": status_code,
            "response": {"message": "Accepted terms and conditions"},
        }, status_code, headers

    except ValueError as e:
        return {
            "statusCode": 404,
            "response": {
                "errorCode": 404,
                "errorMessage": "Client Error",
                "errorMoreInfo": str(e),
            },
        }, 404, headers
    except Exception as exp:
        return {
            "statusCode": 500,
            "response": {
                "errorCode": 500,
                "errorMessage": "Internal Server Error",
                "errorMoreInfo": str(exp),
            },
        }, 500, headers
