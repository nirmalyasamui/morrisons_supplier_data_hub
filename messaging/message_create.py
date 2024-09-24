
import logging
import os
from datetime import datetime

import pytz
from google.cloud import firestore

tz = pytz.timezone("Europe/London")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
"""
    Desc : Saves Message in Firestore collection 
    Input: Request JSON format
    Output:Response JSON Format
"""


def sms_create(req_data, headers):
    logger.info("Create Message execution started ")
    try:
        x = 1000  # Message Id Initial value
        y = []  # Intermediate Result
        js_input = {}
        if not req_data:
            raise ValueError("Request is invalid")
        collection_name = os.environ.get("ADMIN_MESSAGE_COLLECTION")
        if "startDate" not in req_data:
            raise ValueError("startDate field Not Found")
        req_data["startDate"] = datetime.strptime(
            req_data["startDate"], "%Y-%m-%d"
        )
            
        if "startTime" not in req_data:
            raise ValueError("startTime field Not Found")
        req_data["startTime"] = datetime.strptime(
            req_data["startTime"], "%H:%M"
        ).time()
            
        if "endDate" not in req_data:
            raise ValueError("endDate field Not Found")
        req_data["endDate"] = datetime.strptime(req_data["endDate"], "%Y-%m-%d")
            
        if "endTime" not in req_data:
            raise ValueError("endTime field Not Found")
        req_data["endTime"] = datetime.strptime(
            req_data["endTime"], "%H:%M"
        ).time()
            
        if "remarks" not in req_data:
            raise ValueError("remarks field Not Found")
        js_input["remarks"] = req_data["remarks"]
            
        if "message" not in req_data:
            raise ValueError("message field Not Found")
        js_input["message"] = req_data["message"]
    
        js_input["start_datetime"] = datetime.combine(
            req_data["startDate"], req_data["startTime"]
        )
        js_input["end_datetime"] = datetime.combine(
            req_data["endDate"], req_data["endTime"]
        )
        db = firestore.Client()
        doc_ref = db.collection(collection_name).document()
        js_input["created_timestamp"] = firestore.SERVER_TIMESTAMP
        req_ids = db.collection(collection_name).where("message_id", "!=", "").stream()
        for z in req_ids:
            result = z.to_dict()
            y.append(result["message_id"])
        if len(y) != 0:
            y = [int(i) for i in y]
            x = max(y) + 1
        js_input["message_id"] = x
        doc_ref.set(js_input)
        return {"statusCode": 201, "response": {"message": "Created successfully"}}, 201, headers
    except ValueError as e:
        return {
            "statusCode": 400,
            "response": {
                "errorCode": 400,
                "errorMessage": "Client Error",
                "errorMoreInfo": str(e),
            },
        },  400, headers
    except Exception as exp:
        return {
            "statusCode": 500,
            "response": {
                "errorCode": 500,
                "errorMessage": "Internal Server Error",
                "errorMoreInfo": str(exp),
            },
        },  500, headers
