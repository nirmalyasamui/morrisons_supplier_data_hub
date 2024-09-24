'''
***********************************************************************************************************************
Purpose: Encapsulate all location related service methods
Developer: Somnath De (somnath.de@morrisonsplc.co.uk)
****************************************************************s*******************************************************
'''
from bigqueryutil import BigQueryUtil
from commonutil import CommonUtil
import constant as constant
from datetime import datetime, timezone
import time
import google.cloud.logging
client = google.cloud.logging.Client()
client.setup_logging()
import logging
import queryconstants as qc

FILTER_CATEGORIES='filter_categories'
FILTER_CLASSES = 'filter_classes'
FILTER_BRANDS = 'filter_brands'
FILTER_SKUS='filter_skus'
FILTER_TUCS='filter_tucs'
FILTER_BARCODE='filter_barcode'
FILTER_FOR_DOWNLOAD='download_filter'
FILTER_STORES='filter_stores'
FILTER_ITEM_STATUS = 'filter_item_status'
FILTER_FROM_BSNS_UNIT_CD = 'filter_from_bsns_unit_cd'
FILTER_DCS='filter_dcs'

COLUMN_COMMERCIAL_CATEGORY='commercial_category'
COLUMN_COMMERCIAL_CLASS='commercial_class'
COLUMN_BRAND_NAME='brand_name'
COLUMN_SKU_ITEM='sku'
COLUMN_TUC='tuc'
COLUMN_PROMO_IND='PROMO_IND'
COLUMN_DIRECT_PO='DIRECT_PO_IND'
COLUMN_ITEM_STATUS='itemStatus'
COLUMN_BSNS_CD='BSNS_UNIT_CD'
COLUMN_FROM_BSNS_UNIT_CD = 'from_bsns_unit_cd'
COLUMN_FROM_BSNS_CD='FROM_BSNS_UNIT_CD'

DOWNLOAD_DIC={
    'ITEMNUMBER': 'ITEM_NO.',
    'ITEMDESCRIPTION' : 'ITEM_DESCRIPTION',
    'ITEMSTATUS'      : 'ITEM_STATUS',
    'DCNAME' :'DC_NAME',
    'DCNUMBER' : 'DC_NO.',
    'STORENUMBER' :'STORE_NO.',
    'STORENAME':'STORE_NAME',
    'NUMBEROFITEMS': 'NO._OF_ITEMS',
    'DC':'DC',
    'STORES':  'STORES',
    'TUC' :'TUC'
}


class RangedProductServiceHandle:
    # This query must have some ORDER BY clause to work with limit and offset...
    @staticmethod
    def _get_dcs(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]
            startswith = request_dictionary[constant.STARTS_WITH]
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            filter_stores = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BSNS_CD)

            db_string= CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT)

            query = qc.DC_QUERY.replace(constant.STARTS_WITH, startswith)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(FILTER_STORES, filter_stores)
            query=query.replace(constant.DB_STRING, db_string)
            logging.info("Query")
            logging.info(query)
            final_result = BigQueryUtil.search_and_display(query, limit, page)
            return final_result
        except Exception as e:
            logging.error('Exception in _get_dcs - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _get_store(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]
            startswith = request_dictionary[constant.STARTS_WITH]
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]        
            db_string= CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT)
            

            
            query = qc.STORE_QUERY.replace(constant.STARTS_WITH, startswith)       
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query=query.replace(constant.DB_STRING, db_string)
            logging.info("Query")
            logging.info(query)
            final_result = BigQueryUtil.search_and_display(query, limit, page)
            return final_result
        except Exception as e:
            logging.error('Exception in _get_store - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _get_store_summary(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]
            startswith = request_dictionary[constant.STARTS_WITH]
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            store = request_dictionary[constant.STORE]
            item_status= request_dictionary[constant.ITEMSTATUS] 
            sort_by = request_dictionary[constant.SORT_BY]
            sort_direction = request_dictionary[constant.SORT_DIRECTION]
            sort_detail = CommonUtil.get_sort_details(sort_by, sort_direction) 
            filter_dcs = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_FROM_BSNS_CD)        
            db_string= CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT)
            

            
            query = qc.STROE_SUMMARY_QUERY.replace(constant.STARTS_WITH, startswith)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.STORE_ID, store)
            query = query.replace(FILTER_DCS, filter_dcs)
            query = query.replace(constant.ITEM_STATUS, item_status)
            if item_status == "approved" or item_status == "discontinued":
                query = query.replace(constant.STATUS_FIELD, qc.STATUS_FIELD) 
            else:
                query = query.replace(constant.STATUS_FIELD, '"ALL"')
            query = query.replace(constant.SORT_DETAIL, sort_detail)
            query=query.replace(constant.DB_STRING, db_string)
            logging.info("Query")
            logging.info(query)
            final_result = BigQueryUtil.search_and_display_sort(query, limit, page,sort_by,sort_direction)
            return final_result  
        except Exception as e:
            logging.error('Exception in _get_store_summary - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _get_store_item_summary(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]
            startswith = request_dictionary[constant.STARTS_WITH]
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            item_status= request_dictionary[constant.ITEMSTATUS]        
            store = request_dictionary[constant.STORE] 
            sort_by = request_dictionary[constant.SORT_BY]
            sort_direction = request_dictionary[constant.SORT_DIRECTION]
            sort_detail = CommonUtil.get_sort_details(sort_by, sort_direction)
            filter_dcs = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_FROM_BSNS_CD) 
            db_string= CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT)
            

            
            query = qc.STORE_ITEM_SUMMARY_QUERY.replace(constant.STARTS_WITH, startswith)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.STORE_ID, store)
            query = query.replace(constant.ITEM_STATUS, item_status)
            query = query.replace(FILTER_DCS, filter_dcs)      
            query=query.replace(constant.DB_STRING, db_string)
            query = query.replace(constant.SORT_DETAIL, sort_detail)
            logging.info("Query")
            logging.info(query)
            final_result = BigQueryUtil.search_and_display_sort(query, limit, page,sort_by,sort_direction)
            return final_result
        except Exception as e:
            logging.error('Exception in _get_store_item_summary - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _get_store_itemsummary_downloadall(request_dictionary):
        
        try:
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            username = request_dictionary[constant.USERNAME]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]  
            store = request_dictionary[constant.STORE] 
            db_string= CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT)

            
            current_filter=[]

            if (constant.DOWNLOAD_FILTER in request_dictionary[constant.FILTERS] ):
                for x in request_dictionary[constant.FILTERS][constant.DOWNLOAD_FILTER]:
                    current_filter.append(x.upper())
            else:
                return {constant.MESSAGE: constant.ERROR_INVALID_PATH}    
   
            default_storefields = ['DCNUMBER','DCNAME','ITEMNUMBER','ITEMDESCRIPTION']  
            column_field = RangedProductServiceHandle.get_selectedfield(current_filter,default_storefields)                 

            query = qc.STOREITEM_DOWNLOAD_QUERY_CSVALL.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.STORE_ID, store)
            query = query.replace(FILTER_FOR_DOWNLOAD, column_field['download_filter'])
            query=query.replace(constant.DB_STRING, db_string)
            logging.info("Query")
            logging.info(query)
    
        
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            logging.info("file_and_table_name")
            logging.info(file_and_table_name)
            query = query.replace(constant.TABLE, file_and_table_name)
            query_columns=qc.STOREITEM_DOWNLOAD_QUERY_CSVALL_SELECTED_FIELDS.replace(constant.FIELD_ARRAY, column_field['csv_field'])
            query_columns=query_columns.replace(constant.FIELD_ARAAY_SELECTED, column_field['precsv_field'])
            query_columns=query_columns.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
                
            destination_blob=file_and_table_name

            final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendor_number, 'csv')
            time.sleep(1)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            final_result = BigQueryUtil.create_download_table(query,destination_blob, file_and_table_name, vendor_number, 'csv')
            return final_result      
                                   
        except Exception as e:
            logging.error('Exception in _get_store_itemsummary_downloadall - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _get_itemstoresummary_downloadall(request_dictionary):
        
        try:
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            username = request_dictionary[constant.USERNAME]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]  
            store = request_dictionary[constant.STORE] 
            db_string= CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT)

            
            current_filter=[]

            if (constant.DOWNLOAD_FILTER in request_dictionary[constant.FILTERS] ):
               
                for x in request_dictionary[constant.FILTERS][constant.DOWNLOAD_FILTER]:
                    current_filter.append(x.upper())
            else:
                 return {constant.MESSAGE: constant.ERROR_INVALID_PATH}
            default_itemfields = ['ITEMNUMBER','ITEMDESCRIPTION','DC','STORES']
            column_field = RangedProductServiceHandle.get_selectedfield(current_filter,default_itemfields)    


            query = qc.QUERY_BYITEM_CSVALL.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.STORE_ID, store)
            query = query.replace(FILTER_FOR_DOWNLOAD, column_field['download_filter'])
            query=query.replace(constant.DB_STRING, db_string)
            logging.info("Query")
            logging.info(query)
    
        
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            logging.info("file_and_table_name")
            logging.info(file_and_table_name)
            query = query.replace(constant.TABLE, file_and_table_name)
            query_columns=qc.BYITEM_QUERY_CSVALL_SELECTED_FIELDS.replace(constant.FIELD_ARRAY, column_field['csv_field'])
            query_columns=query_columns.replace(constant.FIELD_ARAAY_SELECTED,column_field['precsv_field'] )
            query_columns=query_columns.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
            destination_blob=file_and_table_name

            final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendor_number, 'csv')
            time.sleep(1)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            final_result = BigQueryUtil.create_download_table(query,destination_blob, file_and_table_name, vendor_number, 'csv')
            return final_result      
                                   
        except Exception as e:
            logging.error('Exception in _get_itemstoresummary_downloadall - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}
                 
                
    @staticmethod
    def _get_store_itemsummary_download(request_dictionary):
        try:
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            username = request_dictionary[constant.USERNAME]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]  
            store = request_dictionary[constant.STORE]
            item_status= request_dictionary[constant.ITEMSTATUS]
            dc = request_dictionary[constant.DISTRIBUTING_DC]
            db_string= CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT)

            
            query = qc.STOREITEM_DOWNLOAD_QUERY.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.STORE_ID, store)
            query = query.replace(constant.DISTRIBUTING_DC, dc)
            query = query.replace(constant.ITEM_STATUS, item_status)
            query=query.replace(constant.DB_STRING, db_string)
            logging.info("Query")
            logging.info(query)

            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            logging.info("file_and_table_name")
            logging.info(file_and_table_name)
            query = query.replace(constant.TABLE, file_and_table_name)
            query_columns=qc.STOREITEM_DOWNLOAD_QUERY_CSV_SELECTED_FIELDS .replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
            logging.info("query_columns")
            logging.info(query_columns)
            destination_blob=file_and_table_name

            final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendor_number, 'csv')
            time.sleep(1)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            final_result = BigQueryUtil.create_download_table(query,destination_blob, file_and_table_name, vendor_number, 'csv')
            return final_result
        except Exception as e:
            logging.error('Exception in _get_store_itemsummary_download - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _get_storesummary_download(request_dictionary):
        try:
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            username = request_dictionary[constant.USERNAME]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]  
            store = request_dictionary[constant.STORE]
            item_status= request_dictionary[constant.ITEMSTATUS] 
            filter_dcs = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_FROM_BSNS_CD)        
            db_string= CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT)

            
            query = qc.STORE_SUMMARY_DOWNLOAD_QUERY.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.STORE_ID, store)
            query = query.replace(FILTER_DCS, filter_dcs)
            query = query.replace(constant.ITEM_STATUS, item_status)
            if item_status == "approved" or item_status == "discontinued":
                query = query.replace(constant.STATUS_FIELD, qc.STATUS_FIELD) 
            else:
                query = query.replace(constant.STATUS_FIELD, '"ALL"')
            query=query.replace(constant.DB_STRING, db_string)
            logging.info("Query")
            logging.info(query)

            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            logging.info("file_and_table_name")
            logging.info(file_and_table_name)
            query = query.replace(constant.TABLE, file_and_table_name)
            query_columns=qc.STORE_SUMMARY_DOWNLOAD_QUERY_CSV_SELECTED_FIELDS.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
            logging.info("query_columns")
            logging.info(query_columns)
            destination_blob=file_and_table_name

            final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendor_number, 'csv')
            time.sleep(1)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            final_result = BigQueryUtil.create_download_table(query,destination_blob, file_and_table_name, vendor_number, 'csv')
            return final_result        
        except Exception as e:
            logging.error('Exception in _get_storesummary_download - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}    

    @staticmethod
    def _get_by_item_summary(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]
            logging.info(request_dictionary)
            # Fetch filters from the input request...
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM)
            filter_from_business_unit_cd = CommonUtil.get_filter_value_as_string(request_dictionary, constant.DISTRIBUTING_DC)
            filter_item_status = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_ITEM_STATUS)
            filter_item_tucs = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_TUC)
            
            if filter_item_tucs==constant.TUC_FILTER:
                filter_item_tucs = constant.BARCODE_DEFAULT_RP
            sort_by = request_dictionary[constant.SORT_BY]
            sort_direction = request_dictionary[constant.SORT_DIRECTION]
            sort_detail = CommonUtil.get_sort_details(sort_by, sort_direction)
            
            # Build SQL query...
            query = qc.QUERY_GET_BY_ITEM_SUMMARY.replace(FILTER_CATEGORIES, filter_categories)
            query = query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.IFNULL_SKU, constant.SKU_REPLACE)
            if filter_item_status == constant.APPROVED or filter_item_status == constant.DISCONTINUED:
                query = query.replace(constant.ITEM_STATUS, filter_item_status) 
            else:
                query = query.replace(constant.ITEM_STATUS, '""')

            query = query.replace(FILTER_TUCS, filter_item_tucs)
            if filter_from_business_unit_cd != constant.UPPER_IFNULL+constant.DISTRIBUTING_DC+constant.NULL_VALUE:
                query = query.replace(constant.ALL_DC, constant.ALLDC_REPLACE)
                query = query.replace(constant.GROUP_ALLDC, constant.GROUP_ALLDC_REPLACE)
            else:
                query = query.replace(constant.GROUP_DCS, "")
            query = query.replace(constant.DISTRIBUTING_DC, filter_from_business_unit_cd)
            query = query.replace(constant.IF_NULL+constant.DISTRIBUTING_DC+',', constant.IFNULL_BSNSCD)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.SORT_DETAIL, sort_detail)
            logging.info(query)
            final_result = BigQueryUtil.search_and_display_sort(query, limit, page, sort_by, sort_direction)
            records = final_result['data']
            count_of_stores = 0
            if len(records) > 0:
                final_result['summary'] = {}
                for rec in records:
                    count_of_stores = count_of_stores + int(rec['numberOfStores'])
                final_result['summary']['numberOfStoresAverage'] = round(count_of_stores/(len(records)))
            return final_result
        except Exception as e:
            logging.error('Exception in _get_by_item_summary - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER} 

    @staticmethod
    def _get_by_item_summary_detail(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]
            
            # Fetch filters from the input request...
            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_from_business_unit_cd = CommonUtil.get_filter_value_as_string(request_dictionary, constant.DISTRIBUTING_DC)
            filter_item_status = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_ITEM_STATUS)
            filter_item_tucs = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_TUC)
            if filter_item_tucs==constant.TUC_FILTER:
                filter_item_tucs = constant.BARCODE_DEFAULT_RP
            
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            sku = request_dictionary[constant.SKU]
            sort_by = request_dictionary[constant.SORT_BY]
            sort_direction = request_dictionary[constant.SORT_DIRECTION]
            sort_detail = CommonUtil.get_sort_details(sort_by, sort_direction)
            

            # Build SQL query...
            query = qc.QUERY_GET_BY_ITEM_SUMMARY.replace(FILTER_CATEGORIES, filter_categories)
            query = query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, '"'+sku+'"')
            query = query.replace(constant.IFNULL_SKU, constant.SKU_REPLACE)
            if filter_item_status == constant.APPROVED or filter_item_status == constant.DISCONTINUED:
                query = query.replace(constant.ITEM_STATUS, filter_item_status) 
            else:
                query = query.replace(constant.ITEM_STATUS, '""')

            query = query.replace(FILTER_TUCS, filter_item_tucs)
            query = query.replace(constant.ALL_DC, constant.ALLDC_REPLACE)
            query = query.replace(constant.GROUP_ALLDC, constant.GROUP_ALLDC_REPLACE)
            query = query.replace(constant.DISTRIBUTING_DC, filter_from_business_unit_cd)
            query = query.replace(constant.IF_NULL+constant.DISTRIBUTING_DC+',', constant.IFNULL_BSNSCD)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.SORT_DETAIL, sort_detail)
            logging.info(query)
            final_result = BigQueryUtil.search_and_display_sort(query, limit, page, sort_by, sort_direction)
            records = final_result['data']
            count_of_stores = 0
            if len(records) > 0:
                final_result['summary'] = {}
                for rec in records:
                    count_of_stores = count_of_stores + int(rec['numberOfStores'])
                final_result['summary']['numberOfStoresTotal'] = count_of_stores
            return final_result
        except Exception as e:
            logging.error('Exception in _get_by_item_summary_detail - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER} 

    @staticmethod
    def _get_by_item_detail(request_dictionary): #GET BY ITEM DRILL DOWN DETAILS
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]
            
            # Fetch filters from the input request...
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            sku = request_dictionary[constant.SKU]
            filter_from_business_unit_cd = CommonUtil.get_filter_value_as_string(request_dictionary, constant.DISTRIBUTING_DC)
            filter_item_status = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_ITEM_STATUS)
            filter_item_tucs = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_TUC)
            if filter_item_tucs==constant.TUC_FILTER:
                filter_item_tucs = constant.BARCODE_DEFAULT_RP
            sort_by = request_dictionary[constant.SORT_BY]
            sort_direction = request_dictionary[constant.SORT_DIRECTION]
            sort_detail = CommonUtil.get_sort_details(sort_by, sort_direction)
            
            # Build SQL query...
            query = qc.QUERY_GET_BY_ITEM_DETAIL.replace(constant.ITEM_TUC, filter_item_tucs)
            if filter_from_business_unit_cd != constant.UPPER_IFNULL+constant.DISTRIBUTING_DC+constant.NULL_VALUE:
                query = query.replace(constant.ALL_DC, 'A.FROM_BSNS_UNIT_CD')
                query = query.replace(constant.GROUP_ALLDC, 'A.FROM_BSNS_UNIT_CD')
            else:
                query = query.replace(constant.GROUP_DCS, '')
            query = query.replace(constant.DISTRIBUTING_DC, filter_from_business_unit_cd)
            query = query.replace(constant.IF_NULL+constant.DISTRIBUTING_DC+',', constant.IFNULL_BSNSCD)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.ITEM_NUMBER, sku)
            query = query.replace(constant.SORT_DETAIL, sort_detail)
            if filter_item_status == constant.APPROVED or filter_item_status == constant.DISCONTINUED:
                query = query.replace(constant.ITEM_STATUS, filter_item_status) 
            else:
                query = query.replace(constant.ITEM_STATUS, '""')
            logging.info(query)
            final_result = BigQueryUtil.search_and_display_sort(query, limit, page, sort_by, sort_direction)
            if len(final_result['data']) > 0:
                final_result['summary'] = {}
                final_result['summary']['numberOfStoresTotal'] = final_result['metadata']['totalCount']
            return final_result
        except Exception as e:
            logging.error('Exception in _get_by_item_detail - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _get_item_summary_download(request_dictionary):
        try:
            # Fetch filters from the input request...
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            username = request_dictionary[constant.USERNAME]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]
            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM)
            filter_from_business_unit_cd = CommonUtil.get_filter_value_as_string(request_dictionary, constant.DISTRIBUTING_DC)
            filter_item_status = CommonUtil.get_filter_value_as_string(request_dictionary, constant.ITEM_STATUS)
            filter_item_tucs = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_TUC)
            if filter_item_tucs==constant.TUC_FILTER:
                filter_item_tucs = constant.BARCODE_DEFAULT_RP
            
            
        
            
            # Build SQL query...
            query = qc.QUERY_BY_ITEM_SUMMARY_DL.replace(FILTER_CATEGORIES, filter_categories)
            query = query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.IFNULL_SKU, constant.SKU_REPLACE)
            if filter_item_status == '"A"' or filter_item_status == '"DC"':
                
                query = query.replace(constant.ITEM_STATUS, filter_item_status) 
            else:
                query = query.replace(constant.ITEM_STATUS, '""')

            query = query.replace(FILTER_TUCS, filter_item_tucs)
            if filter_from_business_unit_cd != constant.UPPER_IFNULL+constant.DISTRIBUTING_DC+constant.NULL_VALUE:
                query = query.replace(constant.ALL_DC, 'A.FROM_BSNS_UNIT_CD')
                query = query.replace(constant.GROUP_ALLDC, 'A.FROM_BSNS_UNIT_CD')
            else:
                query = query.replace(constant.GROUP_DCS, "")
            query = query.replace(constant.DISTRIBUTING_DC, filter_from_business_unit_cd)
            query = query.replace(constant.IF_NULL+constant.DISTRIBUTING_DC+',', constant.IFNULL_BSNSCD)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            logging.info(query)

            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            logging.info("file_and_table_name")
            logging.info(file_and_table_name)
            query = query.replace(constant.TABLE, file_and_table_name)
            query_columns=qc.QUERY_BY_ITEM_SUMMARY_SELECTED_FIELDS.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
            logging.info("query_columns")
            logging.info(query_columns)
            destination_blob=file_and_table_name

            final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendor_number, 'csv')
            time.sleep(1)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            final_result = BigQueryUtil.create_download_table(query,destination_blob, file_and_table_name, vendor_number, 'csv')
            return final_result  
        except Exception as e:
            logging.error('Exception in _get_item_summary_download - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}      
    
    @staticmethod
    def _get_itemdc_summary_download(request_dictionary):
        try:
            
            # Fetch filters from the input request...
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            username = request_dictionary[constant.USERNAME]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]
            sku = request_dictionary[constant.SKU]
            filter_from_business_unit_cd = CommonUtil.get_filter_value_as_string(request_dictionary, constant.DISTRIBUTING_DC)
            filter_item_status = CommonUtil.get_filter_value_as_string(request_dictionary, constant.ITEM_STATUS)
            filter_item_tucs = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_TUC)
            if filter_item_tucs==constant.TUC_FILTER:
                filter_item_tucs = constant.BARCODE_DEFAULT_RP
            
            # Build SQL query...
            query = qc.QUERY_BYITEM_DC_DL.replace(constant.ITEM_TUC, filter_item_tucs)
            if filter_from_business_unit_cd != constant.UPPER_IFNULL+constant.DISTRIBUTING_DC+constant.NULL_VALUE:
                query = query.replace(constant.ALL_DC, 'A.FROM_BSNS_UNIT_CD')
                query = query.replace(constant.GROUP_ALLDC, 'A.FROM_BSNS_UNIT_CD')
            else:
                query = query.replace(constant.GROUP_DCS, '')
            query = query.replace(constant.DISTRIBUTING_DC, filter_from_business_unit_cd)
            query = query.replace(constant.IF_NULL+constant.DISTRIBUTING_DC+',', constant.IFNULL_BSNSCD)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.ITEM_NUMBER, sku)

            
            if filter_item_status == '"A"' or filter_item_status == '"DC"':
                query = query.replace(constant.ITEM_STATUS, filter_item_status) 
            else:
                query = query.replace(constant.ITEM_STATUS, '""')
            logging.info(query)    

            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            logging.info("file_and_table_name")
            logging.info(file_and_table_name)
            query = query.replace(constant.TABLE, file_and_table_name)
            query_columns=qc.QUERY_BYITEM_DC_SELECTED_FIELDS.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
            logging.info("query_columns")
            logging.info(query_columns)
            destination_blob=file_and_table_name

            final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendor_number, 'csv')
            time.sleep(1)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            final_result = BigQueryUtil.create_download_table(query,destination_blob, file_and_table_name, vendor_number, 'csv')
            return final_result
        except Exception as e:
            logging.error('Exception in _get_itemdc_summary_download - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _get_itemdc_detail_download(request_dictionary):
        try:
            logging.info("req_dic")
            logging.info(request_dictionary)
            # Fetch filters from the input request...
            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_from_business_unit_cd = CommonUtil.get_filter_value_as_string(request_dictionary, constant.DISTRIBUTING_DC)
            filter_item_status = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_ITEM_STATUS)
            filter_item_tucs = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_TUC)
            if filter_item_tucs==constant.TUC_FILTER:
                filter_item_tucs = constant.BARCODE_DEFAULT_RP
            
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            username = request_dictionary[constant.USERNAME]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]
            sku = request_dictionary[constant.SKU]
            sort_by = request_dictionary[constant.SORT_BY]
            sort_direction = request_dictionary[constant.SORT_DIRECTION]
            sort_detail = CommonUtil.get_sort_details(sort_by, sort_direction)
            

            # Build SQL query...
            query = qc.QUERY_BY_ITEM_SUMMARY_DL.replace(FILTER_CATEGORIES, filter_categories)
            query = query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, '"'+sku+'"')
            query = query.replace(constant.IFNULL_SKU, constant.SKU_REPLACE)
            if filter_item_status == constant.APPROVED or filter_item_status == constant.DISCONTINUED:
                query = query.replace(constant.ITEM_STATUS, filter_item_status) 
            else:
                query = query.replace(constant.ITEM_STATUS, '""')

            query = query.replace(FILTER_TUCS, filter_item_tucs)
            query = query.replace(constant.ALL_DC, constant.ALLDC_REPLACE)
            query = query.replace(constant.GROUP_ALLDC, constant.GROUP_ALLDC_REPLACE)
            query = query.replace(constant.DISTRIBUTING_DC, filter_from_business_unit_cd)
            query = query.replace(constant.IF_NULL+constant.DISTRIBUTING_DC+',', constant.IFNULL_BSNSCD)
            query = query.replace('ORDER BY PIN_SKU_ITEM_NBR', 'ORDER BY dc')
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.SORT_DETAIL, sort_detail)
            logging.info(query)

            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            logging.info("file_and_table_name")
            logging.info(file_and_table_name)
            query = query.replace(constant.TABLE, file_and_table_name)
            query_columns=qc.QUERY_BY_ITEM_SUMMARY_SELECTED_FIELDS.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
            logging.info("query_columns")
            logging.info(query_columns)
            destination_blob=file_and_table_name

            final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendor_number, 'csv')
            time.sleep(1)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('rangedProducts_' , username)
            final_result = BigQueryUtil.create_download_table(query,destination_blob, file_and_table_name, vendor_number, 'csv')
            return final_result
        except Exception as e:
            logging.error('Exception in _get_itemdc_detail_download - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}


    @staticmethod
    def get_selectedfield(current_filter,default_field):
           
                          
            filter_as_string = ''
            if  len( current_filter) > 0  :
                for filter_name in current_filter:
                    
                    filter_as_string = filter_as_string+ filter_name.upper() + ',' 
                for filters in default_field:
                    if filters not in current_filter:
                        filter_as_string = filter_as_string+ filters.upper() + ','
            download_filter = filter_as_string.strip(',')  # Remove the last ,
            filter_elements =download_filter.split(',')
            filter_as_field =''
            filter_as_selected_field =''      
                
            for item in  filter_elements:
                    filter_as_field= filter_as_field+'"'+DOWNLOAD_DIC[item.upper()]+ '" AS ' + item.upper()+','                     
                    filter_as_selected_field= filter_as_selected_field+' AS ' + item.upper()+',""' 
            filter_as_field = filter_as_field.strip(',')
            filter_as_selected_field=filter_as_selected_field.strip(',""')  
                           

            result= {'csv_field':filter_as_field,'precsv_field':filter_as_selected_field,'download_filter':download_filter}
            logging.info("result")
            logging.info(result)

            return result         
         

    @staticmethod
    def get_handle(request_dictionary):
        
        path = request_dictionary[constant.PATH]
        path_dic= { '/getstoresummary': RangedProductServiceHandle._get_store_summary,
        '/getstore/itemsummary': RangedProductServiceHandle._get_store_item_summary,
        '/storeitemsummary/download/all' :  RangedProductServiceHandle._get_store_itemsummary_downloadall,
        '/itemstoresummary/download/all': RangedProductServiceHandle._get_itemstoresummary_downloadall,
        '/storeitemsummary/download' :  RangedProductServiceHandle._get_store_itemsummary_download,
        '/storesummary/download': RangedProductServiceHandle._get_storesummary_download,
        '/getByItemSummary' :  RangedProductServiceHandle._get_by_item_summary,
        '/getByItemSummaryDetail': RangedProductServiceHandle._get_by_item_summary_detail,
        '/getByItemDetail' :  RangedProductServiceHandle._get_by_item_detail,
        '/itemsummary/download': RangedProductServiceHandle._get_item_summary_download,
        '/itemdcsummary/download' :  RangedProductServiceHandle._get_itemdc_summary_download,
        '/itemdcdetail/download': RangedProductServiceHandle._get_itemdc_detail_download
        

        }


        if request_dictionary[constant.VENDOR_NUMBER] == constant.DEFAULT_VENDOR_NUMBER:
                return {constant.MESSAGE: constant.ERROR_NO_VENDOR_SPECIFIED}
        
        if path in path_dic :
            if path in ['/getstore/itemsummary', '/getstoresummary'] or path.startswith('/store') :
                if request_dictionary[constant.STORE] == constant.DEFAULT_STORE:
                   return {constant.MESSAGE: constant.ERROR_NO_STORE_SPECIFIED} 
            if path in ['/getByItemSummaryDetail', '/getByItemDetail','/itemdcdetail/download']  :
                if request_dictionary[constant.SKU] == constant.DEFAULT_ITEM_NUMBER:
                   return {constant.MESSAGE: constant.ERROR_NO_ITEMNO_SPECIFIED}             
            final_result = path_dic[path](request_dictionary)
            return final_result
                                          
       
        else:
            return {constant.MESSAGE: constant.ERROR_INVALID_PATH}