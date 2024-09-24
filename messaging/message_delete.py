
import logging
import os

from google.cloud import firestore

logger = logging.getLogger()
logger.setLevel(logging.INFO)
"""
    Desc : Deletes Message by id in Firestore collection
    Input: Request JSON format
    Output:Response JSON Format
"""


def sms_delete(req_data, headers):
    logger.info("Delete Message execution started")
    try:
        collection_name = os.environ.get("ADMIN_MESSAGE_COLLECTION")
        if "id" in req_data:
            doc_id = req_data["id"]
        else:
            raise ValueError("id parameter Not Found")
        db = firestore.Client()
        doc_ref = db.collection(collection_name)
        if doc_id:
            doc_ref.document(doc_id).delete()
        return {"statusCode": 200, "response": {"message": "Deleted successfully"}}, 200, headers
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
