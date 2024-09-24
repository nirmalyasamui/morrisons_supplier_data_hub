from google.cloud import firestore


def get_db():
    db = firestore.Client()
    return db


"""
    Desc : Connects firestore database and created collection referrence
    Input: String
    Output:Response JSON Format

"""


def connect_db(collection_name):
    db = get_db()
    collection_ref = db.collection(collection_name)
    return collection_ref


"""
    Desc : Return firestore database result for doc_id input
    Input: String
    Output:Response JSON Format

"""


def get_record(collection_name, doc_id):
    collection_ref = connect_db(collection_name)
    doc_ref = collection_ref.document(doc_id)
    doc = doc_ref.get()
    return doc


"""
    Desc : Return firestore database results for field filter input
    Input: String
    Output:Response JSON Format

"""


def get_records(collection_name, filter_field, operator, filter_value):
    collection_ref = connect_db(collection_name)
    query_ref = collection_ref.where(filter_field, operator, filter_value)
    docs = query_ref.stream()
    return docs


"""
    Desc : Add a data in firestore database
    Input: String
    Output:Response JSON Format

"""


def update_login_time(collection_name, doc_id):
    collection_ref = connect_db(collection_name)
    doc_ref = collection_ref.document(doc_id)
    login_time_data = {"last_login_time": firestore.SERVER_TIMESTAMP}
    doc_ref.set(login_time_data, merge=True)
    return "success"


def get_doc_records(doc_ids, collection_name, return_dict=False):
    doc_refs = []
    db = get_db()
    collection_ref = connect_db(collection_name)
    if return_dict:
        for doc_id in doc_ids:
            #doc_refs.append(collection_ref.document(doc_id))
            doc_ref = collection_ref.document(doc_id).get()
            if doc_ref:
                doc_dict = doc_ref.to_dict()
                doc_dict['supplierId'] = doc_id
                doc_refs.append(doc_dict)
        return doc_refs
    else:
        for doc_id in doc_ids:
            doc_refs.append(collection_ref.document(doc_id))

        docs = db.get_all(doc_refs)
        return docs


