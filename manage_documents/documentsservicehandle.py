from tokenize import group
import constant as constants
from google.cloud import firestore
from datetime import datetime, timezone
from urllib.parse import unquote
from operator import itemgetter
import time
import os
import google.cloud.logging as loggers

client = loggers.Client()
client.setup_logging()

import logging

DOCUMENTS_COLLECTION = os.environ.get(constants.DOCUMENTS_COLLECTION)
CATEGORY_COLLECTION = str(os.environ.get(constants.CATEGORY_COLLECTION))


linkedColumn = {'documentID': 'documentID'}

def get_next_doc_id():
    try:
        db = firestore.Client()
        docs = db.collection(DOCUMENTS_COLLECTION).stream()
        doc_id_list = []
        for doc in docs:
            doc_id_list.append(doc.id)
                
        if len(doc_id_list):
            m = max(doc_id_list)
            m1 = m[3:]
            m3 = int(m1)+1
            m4 = str(m3)
            d = 6 - len(m4)
            doc_id = 'DOC'
            while d > 0:
                doc_id+='0'
                d-=1
            doc_id+=m4
            return doc_id
        else:
            doc_id = "DOC000001"
            return doc_id
    except ValueError as e: 
        return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": constants.ERROR_CLIENT_ERROR_BAD_REQUEST,"errorMoreInfo":str(e)}}
    except Exception as exp:
        return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": constants.ERROR_INTERNAL_SERVER,"errorMoreInfo":str(exp)}}

def get_next_cat_id():
    try:
        db = firestore.Client()
        cats = db.collection(CATEGORY_COLLECTION).stream()
        cat_id_list = []
        for doc in cats:
            
            cat_id_list.append(doc.id)
                
        if len(cat_id_list):
            m = max(cat_id_list)
            m1 = m[3:]
            m3 = int(m1)+1
            m4 = str(m3)
            d = 6 - len(m4)
            cat_id = 'CAT'
            while d > 0:
                cat_id+='0'
                d-=1
            cat_id+=m4
            return cat_id
        else:
            cat_id = "CAT000001"
            return cat_id
    except ValueError as e: 
        return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": constants.ERROR_CLIENT_ERROR_BAD_REQUEST,"errorMoreInfo":str(e)}}
    except Exception as exp:
        return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": constants.ERROR_INTERNAL_SERVER,"errorMoreInfo":str(exp)}}

def doc_delete_request_details(request):
    try:        
        logging.info("entered in doc_delete_request_details")
        full_path = request.full_path.strip('?')
        temp_array = full_path.split('?')
        path = temp_array[0]
        doc_ids = request.args.get(constants.DOCUMENT_ID, default=None, type=str)
        logging.info(doc_ids)
        request_dictionary = {}
        if len(full_path) > len(path):
            request_dictionary[constants.PATH] = path
            request_dictionary[constants.DOCUMENT_ID] = doc_ids
            request_dictionary[constants.METHOD] = request.method.upper()
            return request_dictionary
        else:
            return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": constants.ERROR_CLIENT_ERROR_BAD_REQUEST}}
    except ValueError as e: 
        return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": constants.ERROR_CLIENT_ERROR_BAD_REQUEST,"errorMoreInfo":str(e)}}
    except Exception as exp:
        return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": constants.ERROR_INTERNAL_SERVER,"errorMoreInfo":str(exp)}}

def cat_get_request_details(request):
    try:        
        logging.info("entered in cat_get_request_details")
        full_path = request.full_path.strip('?')
        temp_array = full_path.split('?')
        path = temp_array[0]
        if len(temp_array) > 1:
            categories = request.args.get(constants.CATEGORY, default=None, type=str)
        else:
            categories = None
        logging.info(categories)
        request_dictionary = {}
        request_dictionary[constants.PATH] = path
        request_dictionary[constants.CATEGORY] = categories
        request_dictionary[constants.METHOD] = request.method.upper()
        return request_dictionary
        
    except ValueError as e: 
        return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": constants.ERROR_CLIENT_ERROR_BAD_REQUEST,"errorMoreInfo":str(e)}}
    except Exception as exp:
        return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": constants.ERROR_INTERNAL_SERVER,"errorMoreInfo":str(exp)}}

def doc_get_request_details(request):
    try:        
        logging.info("entered in doc_get_request_details")
        full_path = request.full_path.strip('?')
        temp_array = full_path.split('?')
        path = temp_array[0]
        company_id = request.args.get(constants.COMPANY_ID, type =str)
        group_id = request.args.get(constants.GROUP_ID, default = None, type =str)
        doc_category = request.args.get(constants.DOC_CATEGORY, default = None, type =str)
        doc_sub_category = request.args.get(constants.DOC_SUB_CATEGORY, default = None, type =str)
        doc_name = request.args.get(constants.DOC_NAME, default = None, type =str)
        sort_by = request.args.get(constants.SORT_BY, default = None, type =str)
        sort_direction = request.args.get(constants.SORT_DIRECTION, default = constants.SORT_DIRECTION_DEFAULT, type = str)
        limit = request.args.get(constants.LIMIT, default=None)
        offset = request.args.get(constants.OFFSET, default='0')

        request_dictionary = {}
        request_dictionary[constants.PATH] = path
        request_dictionary[constants.METHOD] = request.method.upper()
        request_dictionary[constants.COMPANY_ID] = company_id
        request_dictionary[constants.GROUP_ID] = group_id
        request_dictionary[constants.DOC_CATEGORY] = doc_category
        request_dictionary[constants.DOC_SUB_CATEGORY] = doc_sub_category
        request_dictionary[constants.DOC_NAME] = doc_name
        request_dictionary[constants.SORT_BY] = sort_by
        request_dictionary[constants.SORT_DIRECTION] = sort_direction
        request_dictionary[constants.LIMIT] = limit
        request_dictionary[constants.OFFSET] = offset

        return request_dictionary
    except ValueError as e: 
        return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": constants.ERROR_CLIENT_ERROR_BAD_REQUEST,"errorMoreInfo":str(e)}}
    except Exception as exp:
        return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": "Internal Server Error, doc_get_request_details","errorMoreInfo":str(exp)}}

def create_document(js_input,doc_id):
    try:               
        db = firestore.Client()     
        doc_ref = db.collection(DOCUMENTS_COLLECTION).document(doc_id)
        doc_ref.set(js_input)        
        msg = constants.DOCUMENT + str(js_input["documentName"]) + '\" was added successfully'
        logging.info(msg)
        return {"statusCode": 200,"response":{"message": msg}}
    except ValueError as e: 
        return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": constants.ERROR_CLIENT_ERROR_BAD_REQUEST,"errorMoreInfo":str(e)}}
    except Exception as exp:
        return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": "Internal Server Error, create_document","errorMoreInfo":str(exp)}}
    
def update_document(js_input, doc_id):
    try:               
        db = firestore.Client()
        doc_ref = db.collection(DOCUMENTS_COLLECTION) 
        if isinstance(doc_id,list) is True:
            for docid in doc_id:
                doc_ref.document(docid).set(js_input,merge=True)       
                msg = constants.DOCUMENT + str(js_input["documentName"]) + constants.SUCCESS_TXT
                logging.info(msg)
                return {"statusCode": 200,"response":{"message": msg}}
        else:    
            doc_ref.document(doc_id).set(js_input,merge=True)         
            msg = constants.DOCUMENT + str(js_input["documentName"]) + constants.SUCCESS_TXT
            logging.info(msg)
            return {"statusCode": 200,"response":{"message": msg}}
    except ValueError as e: 
        return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": constants.ERROR_CLIENT_ERROR_BAD_REQUEST,"errorMoreInfo":str(e)}}
    except Exception as exp:
        return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": "Internal Server Error,update_document","errorMoreInfo":str(exp)}}

def delete_document(doc_ids):
    logging.info("Delete Document execution started")
    try:
        logging.info(doc_ids)
        db = firestore.Client()
        doc_ref = db.collection(DOCUMENTS_COLLECTION)
        if 'Ç' in doc_ids:
            doc_id_list = doc_ids.split('Ç')
            logging.info(doc_id_list)
            doc_name_list = []
            for docid in doc_id_list:
                docc = doc_ref.document(docid)
                doc = docc.get()
                if doc.exists:
                    doc = doc.to_dict()
                    doc_name = doc["documentName"]
                    logging.info(doc_name)
                    doc_name_list.append(doc_name)
                    docc.delete()
                else:
                    return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "No such document exists"}}
            docs = '\", \"'.join(doc_name_list)
            msg = 'The documents \"' + str(docs) + '\" were deleted successfully'
            logging.info(msg)
            return {"statusCode": 200,"response":{"message": msg}}
        else:
            doc_id = doc_ids
            docc = doc_ref.document(doc_id)
            doc = docc.get()
            if doc.exists:
                doc = doc.to_dict()
                doc_name = doc["documentName"]
                docc.delete()
            else:
                return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "No such document exists"}}
            msg = constants.DOCUMENT + str(doc_name) + '\" was deleted successfully'
            logging.info(msg)
            return {"statusCode": 200,"response":{"message": msg}}
    except ValueError as e: 
        return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": constants.ERROR_CLIENT_ERROR_BAD_REQUEST,"errorMoreInfo":str(e)}}
    except Exception as exp:
        return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": constants.ERROR_INTERNAL_SERVER,"errorMoreInfo":str(exp)}}

def create_category(js_input, cat_id):
    try:
        logging.info(cat_id)
        logging.info(js_input)
        db = firestore.Client()  
        cat_ref = db.collection(CATEGORY_COLLECTION).document(cat_id)
        cat_ref.set(js_input)
        msg = 'The category \"' + str(js_input["category"]) + '\" was added successfully'
        logging.info(msg)
        return {"statusCode": 200,"response":{"message": msg}}
    except ValueError as e: 
        return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": constants.ERROR_CLIENT_ERROR_BAD_REQUEST,"errorMoreInfo":str(e)}}
    except Exception as exp:
        return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": "Internal Server Error create category","errorMoreInfo":str(exp)}}


def get_all_categories(doc_ref):
    cat_list = []
    docs = doc_ref.stream()
    if docs:
        for doc in docs:
            cat = doc.to_dict()
            cat_list.append(cat)
    return {"statusCode": 200,"response":{"data": cat_list}}

def get_category_by_id(doc_ref, cat_id):
    cat_list = []
    docc = doc_ref.document(cat_id)
    doc = docc.get()
    if doc.exists:
        cat = doc.to_dict()
        cat_list.append(cat)
    else:
        return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "No such category exists"}}
    logging.info(cat_list)
    return {"statusCode": 200,"response":{"data": cat_list}}

def get_categories_by_list(doc_ref, categories):
    cat_list = []
    cat_id_list = categories.split('Ç')
    logging.info(cat_id_list)
    for catid in cat_id_list:
        docc = doc_ref.document(catid)
        doc = docc.get()
        if doc.exists:
            cat = doc.to_dict()
            cat_list.append(cat)
        else:
            return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "No such category exists"}}
    logging.info(categories)
    return {"statusCode": 200,"response":{"data": cat_list}}


def fetch_category(categories):
    logging.info("Fetch category execution started")
    try:
        logging.info(categories)
        db = firestore.Client()
        doc_ref = db.collection(CATEGORY_COLLECTION)
        if not categories:
            return get_all_categories(doc_ref)
        elif 'Ç' in categories:
            return get_categories_by_list(doc_ref, categories)
        else:
            return get_category_by_id(doc_ref, categories)
    except ValueError as e: 
        return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": constants.ERROR_CLIENT_ERROR_BAD_REQUEST,"errorMoreInfo":str(e)}}
    except Exception as exp:
        return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": constants.ERROR_INTERNAL_SERVER,"errorMoreInfo":str(exp)}}

def update_category(js_input, cat_id):
    try:               
        db = firestore.Client()
        cat_docs = db.collection(CATEGORY_COLLECTION).where(u'category', u'==', cat_id).stream()
        for cat in cat_docs:
            update_input = js_input
            key = cat.id
            logging.info(key)
            cat_data = cat.to_dict()
            sub_cat = cat_data["subCategory"]
            update_input["subCategory"] = sub_cat + [update_input["subCategory"]]
            logging.info(update_input)
            db.collection(CATEGORY_COLLECTION).document(key).set(update_input,merge=True)  
            break      
        msg = 'The category \"' + str(js_input["category"]) + constants.SUCCESS_TXT
        logging.info(msg)
        return {"statusCode": 200,"response":{"message": msg}}
        
    except ValueError as e: 
        return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": constants.ERROR_CLIENT_ERROR_BAD_REQUEST,"errorMoreInfo":str(e)}}
    except Exception as exp:
        return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": constants.ERROR_INTERNAL_SERVER,"errorMoreInfo":str(exp)}}

def get_doc_by_subcategory(doc_sub_category, doc_list):
    doc_list_temp = []
    if 'Ç' not in doc_sub_category:
        for doc in doc_list:
            if doc["subCategory"] == doc_sub_category:
                doc_list_temp.append(doc)
        return doc_list_temp
    
    cat_id_list = doc_sub_category.split('Ç')
    for cat in cat_id_list:
        for doc in doc_list:
            if doc["subCategory"] == cat:
                doc_list_temp.append(doc)
    return doc_list_temp
    
    
def get_doc_by_category(doc_category, doc_list):
    doc_list_temp = []
    if 'Ç' not in doc_category:
        for doc in doc_list:
            if doc["category"] == doc_category:
                doc_list_temp.append(doc)
        return doc_list_temp
    cat_id_list = doc_category.split('Ç')
    for cat in cat_id_list:
        for doc in doc_list:
            if doc["category"] == cat:
                doc_list_temp.append(doc)
    return doc_list_temp
        
    
def get_doc_by_name(doc_list, doc_name):
    doc_list_temp = []
    for doc in doc_list:
        if doc_name.lower() in doc["documentName"].lower():
            doc_list_temp.append(doc)
    doc_list = doc_list_temp
    return doc_list

def get_internaluser_docs(doc_name, doc_category, doc_sub_category, doc_list):
    logging.info("Admin login, returning all documents")
    
    if doc_name:
        doc_list = get_doc_by_name(doc_list, doc_name)

    if doc_category:
        doc_list = get_doc_by_category(doc_category, doc_list)

    if doc_sub_category:
        doc_list = get_doc_by_subcategory(doc_sub_category, doc_list)
    return doc_list


def get_docs_by_enddate(doc_list):
    doc_list_temp = []
    for doc in doc_list:
        if "endDate" not in  doc.keys():
            doc_list_temp.append(doc)
        else:
            if not doc["endDate"]:
                doc_list_temp.append(doc)
            else:
                end_date_str = doc["endDate"]
                end_date = datetime.strptime(end_date_str,constants.DEFAULT_DATE_FORMAT)
                current_date_str = datetime.now().strftime(constants.DEFAULT_DATE_FORMAT)
                current_date = datetime.strptime(current_date_str,constants.DEFAULT_DATE_FORMAT)
                if end_date >= current_date:
                    doc_list_temp.append(doc)
    return doc_list_temp
 
def get_docs_by_start_date(doc_list):
    doc_list_temp = []
    for doc in doc_list:
        start_date_str = doc["startDate"]
        start_date = datetime.strptime(start_date_str,constants.DEFAULT_DATE_FORMAT)
        current_date_str = datetime.now().strftime(constants.DEFAULT_DATE_FORMAT)
        current_date = datetime.strptime(current_date_str,constants.DEFAULT_DATE_FORMAT)
        if start_date <= current_date:
            doc_list_temp.append(doc)
    return doc_list_temp

def get_assigned_companies_docs(doc_list, company_id):
    assigned_list = []
    for doc in doc_list:
        comp_id_list = doc["assignedSuppliers"] + doc["assignedConsolidatorsHauliers"]
        for a_sup in comp_id_list:
            if a_sup["value"] == company_id:
                assigned_list.append(doc)
    return assigned_list

def get_supplier_docs(doc_list, company_id):
    supplier_docs = []
    if company_id.startswith("SUPPLIER"):
        for doc in doc_list:
            as_list = doc["assignedSuppliers"]
            for a_sp in as_list:
                if a_sp["value"].lower() == "all":
                    supplier_docs.append(doc)
    return supplier_docs

def get_ch_docs(doc_list, company_id):
    ch_docs = []
    if company_id.startswith("CH"):
        for doc in doc_list:
            ash_list = doc["assignedConsolidatorsHauliers"]
            for a_sph in ash_list:
                if a_sph["value"].lower() == "all":
                    ch_docs.append(doc)
    return ch_docs

def get_assigned_supplier_groups(doc_list):
    doc_list2 = []
    for doc in doc_list:
        for g_id in doc["assignedSupplierGroups"]:
            if g_id["value"].lower() == "all":
                doc_list2.append(doc)
    return doc_list2

def get_group_docs_by_group_id(group_id, doc_list):
    doc_list1 = []
    doc_list2 = []
    if not group_id:                
        return []
    for doc in doc_list:
        for g_id in doc["assignedSupplierGroups"]:
            if g_id["value"] == group_id:
                doc_list1.append(doc)
    
    if group_id.startswith("GROUP"):
        doc_list2 = get_assigned_supplier_groups(doc_list)
    return doc_list1 + doc_list2


def get_noninternaluser_docs(doc_name, doc_category, doc_sub_category, company_id, group_id, doc_list):
    logging.info("Non-admin login, returning selectected documents")
    if doc_name:
        doc_list = get_doc_by_name(doc_list, doc_name)

    doc_list = get_docs_by_enddate(doc_list)
    
    doc_list = get_docs_by_start_date(doc_list)
    
    #list1
    assigned_companies_docs = get_assigned_companies_docs(doc_list, company_id)
    
    #list2
    supplier_docs = get_supplier_docs(doc_list, company_id)
    
    #list 3
    ch_docs = get_ch_docs(doc_list, company_id)
    
    #list4 and list 5
    group_docs = get_group_docs_by_group_id(group_id, doc_list)
    
    doc_list = assigned_companies_docs + supplier_docs + ch_docs + group_docs
    logging.info(doc_list)
    doc_id_list = [doc["documentID"] for doc in doc_list]
    doc_id_unique = list(set(doc_id_list))
    doc_list_unique = []
    for idd  in doc_id_unique:
        for doc in doc_list:
            if doc["documentID"] == idd:
                doc_list_unique.append(doc)
                break
    logging.info(doc_list_unique)
    doc_list = doc_list_unique

    if doc_category:
        doc_list = get_doc_by_category(doc_category, doc_list)
            

    if doc_sub_category:
        doc_list = get_doc_by_subcategory(doc_sub_category, doc_list)
    return doc_list
    

def fetch_doc_details(company_id, group_id, doc_category, doc_sub_category, doc_name, sort_by, sort_direction, limit, offset):
    logging.info("Fetch document details execution started")
    try:
        if not company_id:
            return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "Company ID is invalid"}}
        if not sort_by:
            return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "Sort By parameter is missing"}}
        logging.info(company_id)
        doc_list = []
        db = firestore.Client()
        doc_ref = db.collection(DOCUMENTS_COLLECTION)
        docs = doc_ref.stream()
        for doc in docs:
            doc_dict = doc.to_dict()
            doc_list.append(doc_dict)
        
        doc_list = {True:get_internaluser_docs(doc_name, doc_category, doc_sub_category, doc_list),False:get_noninternaluser_docs(doc_name, doc_category, doc_sub_category, company_id, group_id, doc_list)}[company_id[:2] == 'MI']
            
        logging.info(doc_list)
        sort_dir = {True:True,False:False}[sort_direction == 'DESC']
        if sort_by == "subCategory":
            doc_sort_list = []
            doc_nsort_list = []
            for doc in doc_list:
                #doc_nsort_list.append(doc)
                if doc["subCategory"]:
                    doc_sort_list.append(doc)
                else:
                    doc_nsort_list.append(doc)  
            doc_sorted1 = sorted(doc_sort_list, key=itemgetter(sort_by), reverse=sort_dir)
            doc_list_sorted = doc_sorted1 + doc_nsort_list
        else:
            doc_list_sorted = sorted(doc_list, key=itemgetter(sort_by), reverse=sort_dir)
        
            
        logging.info(doc_list_sorted)

        end_limit = None
        if limit:
            end_limit = int(offset) + int(limit)
        final_doc_list  = doc_list_sorted[int(offset):end_limit]
        logging.info(final_doc_list)

        metadata = {}
        metadata["limit"] = int(limit)
        metadata["offset"] = int(offset)
        metadata["totalCount"] = len(doc_list)
        metadata["sortBy"] = sort_by
        metadata["sortDirection"] = sort_direction

        if len(final_doc_list)>0:
            return {"statusCode": 200,"response":{"data": final_doc_list, "metadata": metadata}}
        return {"statusCode":404,"response":{"errorCode": 404, "errorMessage": "bad request, no document for selected filters"}}
    except ValueError as e: 
        return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": constants.ERROR_CLIENT_ERROR_BAD_REQUEST,"errorMoreInfo":str(e)}}
    except Exception as exp:
        return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": "Internal Server Error, fetch_doc_details","errorMoreInfo":str(exp)}}


class DocumentsServiceHandle:

    @staticmethod
    def check_assigned_comanies_data(req_data, doc_details):
        if "assignedSuppliers" not in  req_data.keys():
            return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "Client Error, assignedSuppliers is not present"}}
        if not req_data["assignedSuppliers"]:
            doc_details["assignedSuppliers"] = []

        doc_details["assignedSuppliers"] = req_data["assignedSuppliers"]

        if "assignedConsolidatorsHauliers" not in  req_data.keys():
            return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "Client Error, assignedConsolidatorsHauliers is not present"}}
        
        if not req_data["assignedConsolidatorsHauliers"]:
            doc_details["assignedConsolidatorsHauliers"] = []
                
        doc_details["assignedConsolidatorsHauliers"] = req_data["assignedConsolidatorsHauliers"]

        if "assignedSupplierGroups" not in  req_data.keys():
            return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "Client Error, assignedSupplierGroups is not present"}}
        
        if not req_data["assignedSupplierGroups"]:
            doc_details["assignedSupplierGroups"] = []
                
        
        doc_details["assignedSupplierGroups"] = req_data["assignedSupplierGroups"]
        return {"statusCode":200, "docs": doc_details}
    
    @staticmethod
    def save_document_details(req_data):
        try:
            logging.info('Entered into save_document_details')
            #add your code here get refid from collection and asisgn below
            doc_details = {}
            check_fields = []

            check_fields.append('category')
            check_fields.append('documentName')
            check_fields.append('startDate')
            check_fields.append('fileSize')
            check_fields.append('gsUrl')


            for field in check_fields:
                if field not in req_data.keys():
                    return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "Client Error, " + field + " is not present"}}
                if not req_data[field]:
                    return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "Client Error, " + field + " can not be null"}}
                doc_details[field] = req_data[field]

            result = DocumentsServiceHandle.check_assigned_comanies_data(req_data, doc_details)
            if result['statusCode'] != 200:
                return result
            doc_details = result['docs']
            
            if "description" not in  req_data.keys() or not req_data["description"]:
                doc_details["description"] = ""
                
            doc_details["description"] = req_data["description"]

            
            if "subCategory" not in  req_data.keys() or not req_data["subCategory"]:
                doc_details["subCategory"] = None
            doc_details["subCategory"] = req_data["subCategory"]

            if "endDate" not in  req_data.keys() or not req_data["endDate"]:
                doc_details["endDate"] = None
            doc_details["endDate"] = req_data["endDate"]

            if "documentID" not in  req_data.keys() or not req_data["documentID"]:
                doc_id = get_next_doc_id()
                doc_details["documentID"] = doc_id
                return create_document(doc_details, doc_id)
            doc_id = req_data["documentID"]
            return update_document(doc_details, doc_id)
        except Exception as exp:
            logging.error(constants.EXCEPTION.format(exp))
            return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": constants.ERROR_INTERNAL_SERVER,"errorMoreInfo":str(exp)}}

    @staticmethod
    def add_document_category(req_data):
        try:
            logging.info('Entered into add_document_category')
            #add your code here 
            cat_details = {}
            if "category" not in  req_data.keys():
                return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "Client Error, category is not present"}}
            if not req_data["category"]:
                return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "Client Error, category can not be null"}}
                
            category = req_data["category"]
            if isinstance(category,str) is False:
                return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "Category Name is not a string"}}
            logging.info("cat id: " + str(category))
            
            if "subCategory" not in  req_data.keys():
                cat_id = get_next_cat_id()
                cat_details["category"] = category
                cat_details["subCategory"] = []
                logging.info("subcategory and before  create")
                return create_category(cat_details, cat_id)
            
            if not req_data["subCategory"]:
                cat_id = get_next_cat_id ()
                cat_details["category"] = category
                cat_details["subCategory"] = []
                return create_category(cat_details, cat_id)
            
            cat_details["category"] = category
            sub_category = req_data["subCategory"]
            if isinstance(sub_category,str) is False:
                return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "Subcategory Name is not a string"}}
            cat_details["subCategory"] = sub_category
            return update_category(cat_details, category)
        except Exception as exp:
            logging.error(constants.EXCEPTION.format(exp))
            return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": constants.ERROR_INTERNAL_SERVER,"errorMoreInfo":str(exp)}}

    @staticmethod
    def delete_metadata(request):
        try:
            logging.info('Entered into delete_metadata')
            #add your code here get refid of document that is removed
            req_data = request.get_json()
            logging.info(req_data)
            request_dictionary = doc_delete_request_details(request)
            method = request_dictionary[constants.METHOD]
            if method == constants.HTTP_METHOD_POST:
                req_body = request.get_json()
                request_dictionary['req_body'] = req_body
            if method == constants.HTTP_METHOD_DELETE:
                final_result = delete_document(request_dictionary[constants.DOCUMENT_ID])
                return final_result
        except Exception as exp:
            logging.error(constants.EXCEPTION.format(exp))
            return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": constants.ERROR_INTERNAL_SERVER,"errorMoreInfo":str(exp)}}

    @staticmethod
    def delete_documents(req_data):
        try:
            logging.info('Entered into delete_metadata')
            #add your code here get refid of document that is removed
            if "documentID" not in  req_data.keys():
                return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "Client Error, document Id is not present"}}
            else:
                if not req_data["documentID"]:
                    return {"statusCode":400,"response":{"errorCode": 400, "errorMessage": "Client Error, document Id can not be null"}}
                else:
                    doc_ids = req_data["documentID"]
                    return delete_document(doc_ids)
        except Exception as exp:
            logging.error(constants.EXCEPTION.format(exp))
            return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": constants.ERROR_INTERNAL_SERVER,"errorMoreInfo":str(exp)}}

    @staticmethod
    def get_category(request):
        try:
            logging.info('Entered into get_category')
            request_dictionary = cat_get_request_details(request)
            method = request_dictionary[constants.METHOD]
            if method == constants.HTTP_METHOD_GET:
                final_result = fetch_category(request_dictionary[constants.CATEGORY])
                return final_result
        except Exception as exp:
            logging.error(constants.EXCEPTION.format(exp))
            return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": constants.ERROR_INTERNAL_SERVER,"errorMoreInfo":str(exp)}}


    @staticmethod
    def get_document_details(request):
        try:
            logging.info('Entered into get_document_details')
            #add your code here 
            request_dictionary = doc_get_request_details(request)
            method = request_dictionary[constants.METHOD]
            if method == constants.HTTP_METHOD_GET:
                final_result = fetch_doc_details(request_dictionary[constants.COMPANY_ID],
                                                request_dictionary[constants.GROUP_ID],
                                                request_dictionary[constants.DOC_CATEGORY],
                                                request_dictionary[constants.DOC_SUB_CATEGORY],
                                                request_dictionary[constants.DOC_NAME],
                                                request_dictionary[constants.SORT_BY],
                                                request_dictionary[constants.SORT_DIRECTION],
                                                request_dictionary[constants.LIMIT],
                                                request_dictionary[constants.OFFSET])
                return final_result
        except Exception as exp:
            logging.error(constants.EXCEPTION.format(exp))
            logging.info(constants.EXCEPTION.format(exp))
            return {"statusCode":500,"response":{"errorCode": 500, "errorMessage": "Internal Server Error, get_document_details","errorMoreInfo":str(exp)}}
