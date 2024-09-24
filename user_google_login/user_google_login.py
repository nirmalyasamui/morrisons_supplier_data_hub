from db_firestore import get_records,get_doc_records,update_login_time,get_record
from get_login_error import get_error_message,map_sdh_error
from req_res_controller import get_secret,get_access_levels,camel_case_dict,get_status
from user_details_external import googleid_details_service
import json

import logging
import os


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)



"""
    Desc : Verify User ID Status in Firestore collection user_details to allow login
    Input: Request JSON format
    Output:Response JSON Format

"""
def get_request_header_token(method):
    headers = {}

    if method == 'OPTIONS':
        headers['Access-Control-Allow-Origin'] = '*'
        headers['Access-Control-Allow-Methods'] = ["GET", "POST", "OPTIONS", "PUT", "DELETE", "PATCH"]
        headers['Access-Control-Allow-Headers'] = ['Content-Type', 'Authorization']
        headers['Access-Control-Max-Age'] = '3600'  # Cache for max 1 hour = 3600 seconds...
    else:
        headers['Access-Control-Allow-Origin'] = '*'

    return headers

def user_login(request):

    try:
        headers = get_request_header_token(request.method)
        if request.method == "OPTIONS":
            return '', 204, headers

        
        json_gresult = {
            "UserGroupID": [],
            "classification_id": [],
            "user_name": "",
            "UserRole": "",
            "status": 200,
        }
        project_id = os.environ.get("GCLOUD_PROJECT")
        collection_name = os.environ.get("USER_COLLECTION")
        user_role_collection_name = os.environ.get("USER_ROLES_COLLECTION")
        supplier_collection_name = os.environ.get("SUPPLIER_COLLECTION")
        group_collection_name = os.environ.get("GROUP_COLLECTION")
        secret_name = "googleid_userinfo_password"
        gid_password = get_secret(secret_name, project_id)
        googleid_userinfo_url = os.environ.get("GOOGLEID_USERINFO_URL")
        secret_name = "googleid_userinfo_username"
        gid_username = get_secret(secret_name, project_id)
        req_data = request.get_json()
        if 500 in [
                    gid_password,
                    gid_username
                ]:
            return get_error_message(
                500,
                "An Error occurred in Secret Manager while processing the request",
            ), 500, headers
        if not req_data:
            raise ValueError("Input is invalid")
        user_id = google_acount_check(req_data)
        try:
            username = gid_username
            password = gid_password
            url = googleid_userinfo_url + str(user_id) + "?apikey=" + username
            filter_field = "google_id"
            filter_value = user_id
            operator = "=="
            docs = get_records(collection_name, filter_field, operator, filter_value)
            
            for doc in docs:
                if doc.exists:
                    db_results = doc.to_dict()
                    doc_id = doc.id

                    return user_status_check(db_results,user_role_collection_name,supplier_collection_name,group_collection_name,collection_name,doc_id,json_gresult,headers)

            # if the user found in below external service, the user is allowed to request for access
            json_gresult = googleid_details_service(url, username, password)
            result = map_sdh_error(
                json_gresult["status"], json_gresult["exp_message"]
            )
            if json_gresult["status"] != 200:
                return get_error_message(result["status"], result["message"]), result['status'], headers
            classification_ids = [
                item.split("_")[-1]
                for item in list(filter(None, json_gresult["classification_id"]))
            ]
            if classification_ids:
                sp_result = classification_ids_check(classification_ids,supplier_collection_name)
            data = camel_case_dict(sp_result)
            data["jobRole"] = json_gresult["UserPosition"]
            data["workMail"] = json_gresult["workMail"]
            return get_error_message(404, data, to_string=False), 404, headers
        except Exception as e:
            return get_error_message(500, e), 500, headers
    except Exception as exp:
        return get_error_message(400, exp), 400, headers

def classification_ids_check(classification_ids,supplier_collection_name):
    sp_result = {"type": "", "supplierName": "", "jobRole": "", "workMail": ""}
    for classification_id in classification_ids:
        docs = get_records(
            supplier_collection_name, "step_id", "==", classification_id
        )
        if docs:
            for doc in docs:
                if doc.exists:
                    sp_result = doc.to_dict()
                    if sp_result["status"] == "Active":
                        break
            break
    return sp_result

def user_status_check(db_results,user_role_collection_name,supplier_collection_name,group_collection_name,collection_name,doc_id,json_gresult,headers):
    if (
            db_results["user_access"] == "Approved"
            and db_results["is_active"] == "Y"
        ):

            db_results["supplier_details"] = {}
            db_results["relatedSuppliers"] = []
            sdh_user_role = db_results["roles_assigned"]
            access_levels = get_access_levels(
                user_role_collection_name, sdh_user_role
            )
            if db_results["supplier_id"] is not None:
                doc = get_record(
                    supplier_collection_name, db_results["supplier_id"]
                )
                s_result = doc.to_dict()
                
                db_results["supplier_details"] = camel_case_dict(
                    s_result
                )
                if s_result["related_group"] is not None:
                    doc = get_record(group_collection_name, s_result["related_group"])
                    g_result = doc.to_dict()
                    docs = get_doc_records(g_result["related_suppliers"], supplier_collection_name, return_dict=True)

                    db_results["relatedSuppliers"] = [camel_case_dict(doc) for doc in docs]
            docs = get_doc_records(
                db_results["roles_requested"], user_role_collection_name
            )
            db_results["roles_requested"] = [
                camel_case_dict(doc.to_dict()) for doc in docs
            ]
            docs = get_doc_records(
                db_results["roles_assigned"], user_role_collection_name
            )
            db_results["roles_assigned"] = [
                camel_case_dict(doc.to_dict()) for doc in docs
            ]

            update_login_time(collection_name, doc_id)
            doc = get_record(collection_name, doc_id)

            updated_db_results = doc.to_dict()
            db_results["last_login_time"]=updated_db_results["last_login_time"]
            return get_status(
                db_results,
                access_levels,
            ), json_gresult['status'], headers

    if (
        db_results["user_access"] == "Approved"
        and db_results["is_active"] == "N"
    ):
        return get_error_message(422, "Your user id is disabled"), 422, headers

    if db_results["user_access"] == "Pending":
        return get_error_message(
            422,
            "Your request for access is being processed,try again later!",
        ), 422, headers

    if db_results["user_access"] == "Rejected":
        return get_error_message(
            422, "Your request for access is rejected"
        ), 422, headers

def google_acount_check(req_data):
    if "googleAccountId" in req_data:
        user_id = req_data["googleAccountId"]
        return user_id
    else:
        raise ValueError("googleAccountId Not Found")
