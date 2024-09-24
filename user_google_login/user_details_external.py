import json

import requests
from defusedxml import ElementTree as ET
from requests.auth import HTTPBasicAuth



"""
    Desc : Hit external service to with Google Account ID and return results
    Input: String
    Output:Response JSON Format

"""


def googleid_details_service(url, username, password):
    try:
        gresult = {
            "UserGroupID": [],
            "classification_id": [],
            "user_name": "",
            "workMail": "",
            "status": 200,
            "exp_message": "",
        }
        custom_message_500 = "An error occurred on Supplier Portal External service when processing the request."

        custom_message_503 = (
            "Service Unavailable Response from Supplier Portal External service"
        )
        
        response = requests.get(url, auth=HTTPBasicAuth(username, password), timeout=30)
        status = response.status_code
        if status == 200:
            gresult = get_final_gresult(response,gresult)
            return gresult
        elif status == 404:
            gresult["status"] = status
            gresult["exp_message"] = "404 Not Found on Supplier Portal External Service"
            return gresult
        else:
            raise ConnectionError()
    except ConnectionError:
        gresult["status"] = 500
        if status == 503:
            gresult["exp_message"] = custom_message_503
        else:
            gresult["exp_message"] = custom_message_500
        return gresult

def get_final_gresult(response,gresult):
    split_text = "_"
    root = ET.fromstring(response.content)
    if "EMail" in root.attrib:
        gresult["workMail"] = root.attrib["EMail"]
    for children in root:
        if children.tag == "Name":
            gresult["user_name"] = children.text
        if children.tag == "UserGroupLink":
            x = children.attrib
            if "UserGroupID" in x:
                gresult["UserGroupID"].append(x["UserGroupID"])
                if split_text in x["UserGroupID"]:
                    gresult["classification_id"].append(x["UserGroupID"])
        if children.tag == "MetaData":
            gresult = condition_check(children,gresult)
            
    return gresult

def condition_check(children,gresult):
    for child in children:
        x = child.attrib
        key = x["AttributeID"]
        gresult[key] = child.text
    return gresult