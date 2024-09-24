
import logging
import os
from datetime import datetime

import pytz
from google.cloud import firestore

tz = pytz.timezone("Europe/London")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
"""
    Desc : Updates Message in Firestore collection 
    Input: Request JSON format
    Output:Response JSON Format
"""


def sms_update(req_data, headers):
    logger.info("Update Message execution started")
    try:
        start_date = None
        start_time = None
        end_date = None
        end_time = None
        js_input = {}
        if not req_data:
            raise ValueError("Request is invalid")
        collection_name = os.environ.get("ADMIN_MESSAGE_COLLECTION")
        if "id" not in req_data:
            raise ValueError("id field Not Found")
        doc_id = req_data["id"]
            
        if "startDate" in req_data:
            start_date = datetime.strptime(req_data["startDate"], "%Y-%m-%d")
        if "startTime" in req_data:
            start_time = datetime.strptime(req_data["startTime"], "%H:%M").time()
        if "endDate" in req_data:
            end_date = datetime.strptime(req_data["endDate"], "%Y-%m-%d")
        if "endTime" in req_data:
            end_time = datetime.strptime(req_data["endTime"], "%H:%M").time()
        if "remarks" in req_data:
            js_input["remarks"] = req_data["remarks"]
        if "message" in req_data:
            js_input["message"] = req_data["message"]
        if not start_date or not start_time:
            raise ValueError("startDate and startTime both input required")
        
        js_input["start_datetime"] = datetime.combine(start_date, start_time)
        
        if not end_date or not end_time:
            raise ValueError("endDate and endTime both input required")
        js_input["end_datetime"] = datetime.combine(end_date, end_time)
        
        db = firestore.Client()
        doc_ref = db.collection(collection_name)
        req_data["created_timestamp"] = firestore.SERVER_TIMESTAMP
        doc_ref.document(doc_id).update(js_input)
        return {"statusCode": 200, "response": {"message": "Updated successfully"}}, 200, headers
    except ValueError as e:
        return {
            "statusCode": 400,
            "response": {
                "errorCode": 400,
                "errorMessage": "Client Error",
                "errorMoreInfo": str(e),
            },
        }, 400, headers
    except Exception as exp:
        return {
            "statusCode": 500,
            "response": {
                "errorCode": 500,
                "errorMessage": "Internal Server Error",
                "errorMoreInfo": str(exp),
            },
        }, 500, headers
