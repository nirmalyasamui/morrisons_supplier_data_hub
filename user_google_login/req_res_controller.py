from google.cloud import secretmanager

from db_firestore import get_doc_records


"""
    Desc : Set Response Data
    Input: JSON format
    Output:JSON Format

"""

DEFAULT_DATE_FORMAT = '%Y-%m-%d'

def get_status(
    db_results, access_levels
):

    data = camel_case_dict(db_results)
    data["lastLoginTime"] = data["lastLoginTime"].strftime("%Y-%m-%d, %H:%M")
    data["accessRequestedDate"] = data["accessRequestedDate"].strftime(DEFAULT_DATE_FORMAT)
    data["requestActionedOn"] = data["requestActionedOn"].strftime(DEFAULT_DATE_FORMAT)
    data["sdhUserAccessLevels"] = access_levels

    return {"statusCode": 200, "response": data}


"""
    Desc : Get Role access
    Input: JSON format
    Output:JSON Format

"""


def get_access_levels(ur_collection_name, sdh_user_role):
    access_level = []
    docs = get_doc_records(sdh_user_role, ur_collection_name)
    for doc in docs:
        if doc.exists:
            result = doc.to_dict()
            access_level.append(result["access_level"])
    access_level = sum(access_level, [])
    access_level = list(set(access_level))
    return access_level


def camel_case_dict(x):
    new_dict = dict(
        (k.split("_")[0] + "".join(word.title() for word in k.split("_")[1:]), v)
        for k, v in x.items()
    )
    if "createdDate" in new_dict:
        new_dict["createdDate"] = new_dict["createdDate"].strftime(DEFAULT_DATE_FORMAT)
    if "updatedDate" in new_dict:
        new_dict["updatedDate"] = new_dict["updatedDate"].strftime(DEFAULT_DATE_FORMAT)

    return new_dict


def get_secret(secret_name, project_id):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = "projects/" + project_id + "/secrets/" + secret_name + "/versions/latest"
        response = client.access_secret_version(name=name)
        secret_string = response.payload.data.decode("UTF-8")
        return secret_string
    except Exception:
        return 500