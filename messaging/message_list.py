import logging
import os
from datetime import datetime

import pytz
from google.cloud import firestore


tz = pytz.timezone("Europe/London")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
"""
    Desc : List Messages from Firestore collection based on request by admin or non admin
    Input: Request JSON format
    Output:Response JSON Format
"""
DATE_FORMAT_DMYHM = "%d-%m-%Y %H:%M"

def sms_list(req_data, headers):
    logger.info("List Messages execution started ")
    try:
        final_data = None
        strcheck = "Site Administrator"
        collection_name = os.environ.get("ADMIN_MESSAGE_COLLECTION")
        db = firestore.Client()
        doc_ref = db.collection(collection_name)
        if "role" in req_data:
            role = req_data["role"]
        else:
            raise ValueError("role Not Found")
        today_datetime = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(tz)
        today_datetime = today_datetime.strftime(DATE_FORMAT_DMYHM)
        today_datetime = datetime.strptime(today_datetime, DATE_FORMAT_DMYHM)
        print(today_datetime)
        if role.casefold() == strcheck.casefold():
            docs = doc_ref.where("end_datetime", ">=", today_datetime).stream()
            final_data = set_response_data(docs, today_datetime)
        else:
            docs = doc_ref.where("start_datetime", "<=", today_datetime).stream()
            final_data = set_response_data(docs, today_datetime)
        if final_data is None or len(final_data) == 0 or final_data == "null":
            return {"statusCode": 204, "response": {"message": "No Data to Fetch"}}, 204, headers
        else:
            return {"statusCode": 200, "response": final_data}, 200, headers
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


def set_response_data(docs, today_datetime):
    results = []
    today_datetime = today_datetime.strftime(DATE_FORMAT_DMYHM)
    for x in docs:
        db_result = x.to_dict()
        # print("db_result",db_result)
        db_result["end_datetime"].replace(tzinfo=pytz.utc).astimezone(tz)
        end_datetime = db_result["end_datetime"].strftime("%d-%m-%Y %H:%M:%S")
        
        db_results = filter_date(db_result, end_datetime,today_datetime)
        print("db_result end datetime", db_result["end_datetime"])
        if db_results:
            results.append(
                {
                    "id": x.id,
                    "reqId": db_results["message_id"],
                    "message": db_results["message"],
                    "startDate": db_results["start_datetime"].strftime("%Y-%m-%d"),
                    "startTime": db_results["start_datetime"].strftime("%H:%M"),
                    "endDate": db_results["end_datetime"].strftime("%Y-%m-%d"),
                    "endTime": db_results["end_datetime"].strftime("%H:%M"),
                    "remarks": db_results["remarks"],
                    "createdTimestamp": db_results["created_timestamp"].strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                }
            )

    return results


def filter_date(db_result, end_datetime,today_datetime):
    new_result = dict()
    for k, v in db_result.items():
        z = lambda k, v: datetime.strptime(end_datetime, "%d-%m-%Y %H:%M:%S") >= datetime.strptime(today_datetime, DATE_FORMAT_DMYHM)
        if z(k, v):
            new_result[k] = v
    return new_result
