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
import os
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

COLUMN_COMMERCIAL_CATEGORY='commercial_category'
COLUMN_COMMERCIAL_CLASS='commercial_class'
COLUMN_BRAND_NAME='brand_name'
COLUMN_SKU_ITEM='sku_item_number'
COLUMN_TUC='tuc'
COLUMN_PROMO_IND='PROMO_IND'
COLUMN_DIRECT_PO='DIRECT_PO_IND'
COLUMN_ITEM_STATUS='item_status'
COLUMN_BARCODE='barcode'

DOWNLOAD_DIC={
    'ITEMNUMBER': 'ITEM_NUMBER',
    'TUC'       :  'TUC',
    'ITEMDESCRIPTION' : 'ITEM_DESCRIPTION',
    'ITEMSTATUS'      : 'ITEM_STATUS',
    'DIVISION'        : 'DIVISION',
    'ITEMGROUP'      :  'ITEM_GROUP',
    'CATEGORY'        : 'CATEGORY',
    'DEPARTMENT'      : 'DEPARTMENT',
    'SUBCATEGORY'     : 'SUBCATEGORY',
    'BRAND'           : 'BRAND',
    'PACKSIZE'       :  'PACK_SIZE',
    'MINIMUMACCEPTABLEPRODUCTLIFE' :  'MIN_ACCEPTABLE_PRODUCT_LIFE_DAYS',
    'ORIGIN'  :  'ORIGIN',
    'POIND' : 'DELIVERY',
    'PACKSPERPALLET': 'PACKS_PER_PALLET',
    'BARCODE':'BARCODE_SELLING',
    'HEIGHT':'HEIGHT_CM',
    'WEIGHT':'WEIGHT_KG',
    'PACKSPERLAYER':'PACKS_PER_LAYER',
    'ONPROMOTION' :'ON_PROMOTION',
    'MINIMUMORDER' : 'MINIMUM_ORDER'

}


          
class ProductreferenceServiceHandle:
    # This query must have some ORDER BY clause to work with limit and offset...


    @staticmethod
    def _get_tucs(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]
            startswith = request_dictionary[constant.STARTS_WITH]
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            item_status= request_dictionary[constant.ITEM_STATUS]
            prom_status= request_dictionary[constant.PROMOIND]
            delivery_ind=request_dictionary[constant.DELIVERY]
            reportname= request_dictionary[constant.REPORT_NAME] if constant.REPORT_NAME in request_dictionary else None
            

            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            logging.info("filters")
            logging.info(filter_categories)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            logging.info(filter_classes)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            logging.info(filter_brands)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM)
            logging.info(filter_skus)
            if filter_skus == constant.SKU_FILTER:
                filter_skus=constant.SKU_DEFAULT   
            db_string= CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT)
            

            if(reportname is not None and reportname.casefold()=='rangedproducts'.casefold()):

                query= qc.RP_TUC_QUERY
            else:
                query=qc.TUC_QUERY
                    

            query = query.replace(constant.STARTS_WITH, startswith)
            query = query.replace(FILTER_CATEGORIES, filter_categories)
            query = query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.PROMOIND, prom_status)
            query = query.replace(constant.DELIVERY, delivery_ind)
            query = query.replace(constant.ITEM_STATUS,item_status)
            query = query.replace(FILTER_SKUS, filter_skus)
            query=query.replace(constant.DB_STRING, db_string)
            logging.info("Query")
            logging.info(query)
            final_result = BigQueryUtil.search_and_display(query, limit, page)
            return final_result
        except Exception as e:
            logging.error('Exception in _get_tucs - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _get_details(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]
            startswith = request_dictionary[constant.STARTS_WITH]
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            item_status= request_dictionary[constant.ITEM_STATUS]
            prom_status= request_dictionary[constant.PROMOIND]
            delivery=request_dictionary[constant.DELIVERY]
            sort_by = request_dictionary[constant.SORT_BY]
            sort_direction = request_dictionary[constant.SORT_DIRECTION]
            sort_detail = CommonUtil.get_sort_details(sort_by, sort_direction)
            

            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            logging.info("filters")
            logging.info(filter_categories)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            logging.info(filter_classes)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            logging.info(filter_brands)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM)
            logging.info(filter_skus)
            filter_tucs=CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_TUC)
            filter_barcode=CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_TUC)
            if filter_tucs == constant.TUC_FILTER:
                filter_tucs='UPPER(IFNULL(tuc_barcode.tuc,"NULLVALUE"))'
                filter_barcode=constant.BARCODE_DEFAULT
            else:
                filter_barcode=  filter_tucs 
            if filter_skus == constant.SKU_FILTER:
                filter_skus=constant.SKU_DEFAULT    
            logging.info(filter_tucs)
            db_string= CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT)

            query = qc.PRODUCT_DETAIL_QUERY.replace(constant.STARTS_WITH, startswith)
            query = query.replace(FILTER_CATEGORIES, filter_categories)
            query = query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.PROMOIND, prom_status)
            query = query.replace(constant.DELIVERY, delivery)
            query = query.replace(constant.ITEM_STATUS,item_status)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(FILTER_TUCS, filter_tucs)
            query = query.replace(FILTER_BARCODE, filter_barcode)
            query = query.replace(constant.SORT_DETAIL, sort_detail)
            query=query.replace(constant.DB_STRING, db_string)
            logging.info("Query")
            logging.info(query)
            final_result = BigQueryUtil.search_and_display_sort(query, limit, page,sort_by,sort_direction)
            return final_result   
        except Exception as e:
            logging.error('Exception in _get_details - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _get_details_download(request_dictionary):
        try:
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            itemstatus= request_dictionary[constant.ITEM_STATUS]
            prom_status= request_dictionary[constant.PROMOIND]
            delivery= request_dictionary[constant.DELIVERY]
            username = request_dictionary[constant.USERNAME]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]
        

            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            logging.info("filters")
            logging.info(filter_categories)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            logging.info(filter_classes)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            logging.info(filter_brands)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM)
            logging.info(filter_skus)
            filter_tucs=CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_TUC)
            if filter_tucs == constant.TUC_FILTER:
                filter_tucs='UPPER(IFNULL(temp.tuc,"NULLVALUE"))'
                filter_barcode=constant.BARCODE_DEFAULT
            else:
                filter_barcode=  filter_tucs 
            if filter_skus == constant.SKU_FILTER:
                filter_skus=constant.SKU_DEFAULT 
            logging.info("filter_tucs")
            logging.info(filter_tucs)
            db_string= CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT)
            
            selected_fields=[]
            if(constant.DL_FILTER in request_dictionary[constant.FILTERS] ):
                selected_fields =  ProductreferenceServiceHandle.get_selectedfields(request_dictionary)
                
                if selected_fields['default_download']==1:
                    query = qc.PRODUCT_DETAIL_DOWNLOAD_QUERY_CSVALL_DF.replace(FILTER_CATEGORIES, filter_categories)
                    query = query.replace(FILTER_CLASSES, filter_classes)
                    query = query.replace(FILTER_BRANDS, filter_brands)
                    query = query.replace(constant.VENDOR_NUMBER, vendor_number)
                    query = query.replace(constant.PROMOIND, prom_status)
                    query = query.replace(constant.DELIVERY, delivery)
                    query = query.replace(constant.ITEM_STATUS,itemstatus)
                    query = query.replace(FILTER_SKUS, filter_skus)
                    query = query.replace(FILTER_TUCS, filter_tucs)
                    query = query.replace(FILTER_FOR_DOWNLOAD, selected_fields['download_filter'])
                    
                    query=query.replace(constant.DB_STRING, db_string)
                    logging.info("Query")
                    logging.info(query)

                    file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('productReference_' , username)
                    logging.info("file_and_table_name")
                    logging.info(file_and_table_name)
                    query = query.replace(constant.TABLE, file_and_table_name)
                    query_columns=qc.PRODUCT_DETAIL_DOWNLOAD_QUERY_CSVALL_SELECTED_FIELDS_DF .replace(constant.FIELD_ARRAY,selected_fields['field1'] )
                    query_columns=query_columns.replace(constant.FIELD_ARAAY_SELECTED,selected_fields['field3'] )
                    query_columns=query_columns.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
                    logging.info("query_columns")
                    logging.info(query_columns)
                    destination_blob=file_and_table_name

                    final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendor_number, 'csv')
                    time.sleep(1)
                    file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('productReference_' , username)
                    final_result = BigQueryUtil.create_download_table(query,destination_blob, file_and_table_name, vendor_number, 'csv')
                    return final_result    
                else:
                    query = qc.PRODUCT_DETAIL_DOWNLOAD_QUERY_CSVALL.replace(FILTER_CATEGORIES, filter_categories)
                    query = query.replace(FILTER_CLASSES, filter_classes)
                    query = query.replace(FILTER_BRANDS, filter_brands)
                    query = query.replace(constant.VENDOR_NUMBER, vendor_number)
                    query = query.replace(constant.PROMOIND, prom_status)
                    query = query.replace(constant.DELIVERY, delivery)
                    query = query.replace(constant.ITEM_STATUS,itemstatus)
                    query = query.replace(FILTER_SKUS, filter_skus)
                    query = query.replace(FILTER_TUCS, filter_tucs)
                    query = query.replace(FILTER_FOR_DOWNLOAD, selected_fields['download_filter'])
                    query=query.replace(constant.DB_STRING, db_string)
                    logging.info("Query")
                    logging.info(query)
        
            
                    file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('productReference_' , username)
                    logging.info("file_and_table_name")
                    logging.info(file_and_table_name)
                    query = query.replace(constant.TABLE, file_and_table_name)
                    query_columns=qc.PRODUCT_DETAIL_DOWNLOAD_QUERY_CSVALL_SELECTED_FIELDS.replace(constant.FIELD_ARRAY, selected_fields['field1'])
                    query_columns=query_columns.replace(constant.FIELD_ARAAY_SELECTED,selected_fields['field2'] )
                    query_columns=query_columns.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
                    destination_blob=file_and_table_name

                    final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendor_number, 'csv')
                    time.sleep(1)
                    file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('productReference_' , username)
                    final_result = BigQueryUtil.create_download_table(query,destination_blob, file_and_table_name, vendor_number, 'csv')
                    return final_result 
            
            else:
                query = qc.PRODUCT_DETAIL_DOWNLOAD_QUERY_CSV.replace(FILTER_CATEGORIES, filter_categories)
                query = query.replace(FILTER_CLASSES, filter_classes)
                query = query.replace(FILTER_BRANDS, filter_brands)
                query = query.replace(constant.VENDOR_NUMBER, vendor_number)
                query = query.replace(constant.PROMOIND, prom_status)
                query = query.replace(constant.DELIVERY, delivery)
                query = query.replace(constant.ITEM_STATUS,itemstatus)
                query = query.replace(FILTER_SKUS, filter_skus)
                query = query.replace(FILTER_TUCS, filter_tucs)
                query = query.replace(FILTER_BARCODE, filter_barcode)               
                query=query.replace(constant.DB_STRING, db_string)
                logging.info("Query")
                logging.info(query)

                file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('productReference_' , username)
                logging.info("file_and_table_name")
                logging.info(file_and_table_name)
                query = query.replace(constant.TABLE, file_and_table_name)
                query_columns=qc.PRODUCT_DETAIL_DOWNLOAD_QUERY_CSV__SELECTED_FIELDS
                query_columns=query_columns.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
                logging.info("query_columns")
                logging.info(query_columns)
                destination_blob=file_and_table_name

                final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendor_number, 'csv')
                time.sleep(1)
                file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('productReference_' , username)
                final_result = BigQueryUtil.create_download_table(query,destination_blob, file_and_table_name, vendor_number, 'csv')
                return final_result 
        except Exception as e:
            logging.error('Exception in _get_details_download - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _serachby_item_download(request_dictionary):
        try:
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            item_number = request_dictionary[constant.ITEM_NUMBER]
            username = request_dictionary[constant.USERNAME]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]

            filter_tucs=CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_TUC)
            filter_barcode=CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_TUC)
            if ((filter_tucs == constant.TUC_FILTER)  or ( filter_tucs == '"NULL"')):
                filter_tucs='UPPER(IFNULL(temp.tuc,"NULLVALUE"))'
                filter_barcode=constant.BARCODE_DEFAULT     
            else:
                filter_barcode=  filter_tucs 
            db_string= CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT)

            
            query = qc.SEARCHBY_PRODUCT_QUERY.replace(constant.ITEM_NUMBER, item_number)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(FILTER_TUCS, filter_tucs)
            query = query.replace(FILTER_BARCODE, filter_barcode) 
            query=query.replace(constant.DB_STRING, db_string)

            logging.info("Query")
            logging.info(query)

            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user_itemno('productReference_' , username,item_number)
            logging.info("file_and_table_name")
            logging.info(file_and_table_name)
            query = query.replace(constant.TABLE, file_and_table_name)
            query_columns=qc.SEARCHBY_PRODUCT_QUERY_SELECTED_FIELDS
            query_columns = query_columns.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
            logging.info("query_columns")
            logging.info(query_columns)
            destination_blob=file_and_table_name

            final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendor_number, 'csv')
            time.sleep(1)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user_itemno('productReference_' , username,item_number)
            final_result = BigQueryUtil.create_download_table(query,destination_blob, file_and_table_name, vendor_number, 'csv')
            return final_result    
        except Exception as e:
            logging.error('Exception in _serachby_item_download - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}          

    @staticmethod
    def get_selectedfields(request_dictionary):

                default_download=1
                current_filter=[]

                for x in request_dictionary[constant.FILTERS][constant.DL_FILTER]:
                    current_filter.append(x.upper())
                if any(x in constant.DEFAULT_LIST for x in current_filter): 
                    default_download=0 
                filter_as_string = ''
                if len( current_filter) > 0:
                    for filter_name in current_filter:
                        filter_as_string = filter_as_string+ filter_name.upper() + ',' 

                download_filter = filter_as_string.strip(',')  # Remove the last ,
                logging.info("download_filter")
                logging.info(download_filter)
                filter_as_field = ''
                filter_as_selected_field=''
                filter_as_selected_fielddf=''     
                for item in  current_filter:
                    filter_as_field= filter_as_field+'"'+DOWNLOAD_DIC[item.upper()]+ '" AS ' + item.upper()+','  
                    filter_as_selected_fielddf= filter_as_selected_fielddf+'"" AS ' + item.upper()+','
                    filter_as_selected_field= filter_as_selected_field+' AS ' + item.upper()+',""'               
                logging.info("filter_as_selected_field0") 
                logging.info(filter_as_selected_field)   
                filter_as_field = filter_as_field.strip(',')
                filter_as_selected_field=filter_as_selected_field.strip(',""')
                filter_as_selected_fielddf=filter_as_selected_fielddf.strip(',')
                result= {'field1':filter_as_field,'field2':filter_as_selected_field,'field3':filter_as_selected_fielddf,'download_filter':download_filter,'default_download':default_download}
                logging.info("filter_as_field")
                logging.info(filter_as_field)
                logging.info("filter_as_selected_fielddf")
                logging.info(filter_as_selected_fielddf)
                logging.info("filter_as_selected_field") 
                logging.info(filter_as_selected_field)
                return result
              
    
    @staticmethod
    def get_handle(request_dictionary):
        path = request_dictionary[constant.PATH]

        if path == '/gettuc':
            if request_dictionary[constant.VENDOR_NUMBER] == constant.DEFAULT_VENDOR_NUMBER:
                return {constant.MESSAGE: constant.ERROR_NO_VENDOR_SPECIFIED}

            final_result = ProductreferenceServiceHandle._get_tucs(request_dictionary)
            return final_result
        elif  path == '/getdetails':
            if request_dictionary[constant.VENDOR_NUMBER] == constant.DEFAULT_VENDOR_NUMBER:
                return {constant.MESSAGE: constant.ERROR_NO_VENDOR_SPECIFIED}

            final_result = ProductreferenceServiceHandle._get_details(request_dictionary)
            return final_result
        elif  path == '/getdetails/download':
            if request_dictionary[constant.VENDOR_NUMBER] == constant.DEFAULT_VENDOR_NUMBER:
                return {constant.MESSAGE: constant.ERROR_NO_VENDOR_SPECIFIED}

            final_result = ProductreferenceServiceHandle._get_details_download(request_dictionary)
            return final_result
        elif  path == '/searchbyproduct':
            logging.info("search by item download")
            if request_dictionary[constant.VENDOR_NUMBER] == constant.DEFAULT_VENDOR_NUMBER:
                return {constant.MESSAGE: constant.ERROR_NO_VENDOR_SPECIFIED}
            if request_dictionary[constant.ITEM_NUMBER] == constant.DEFAULT_ITEM_NUMBER:
                return {constant.MESSAGE: constant.ERROR_NO_ITEMNO_SPECIFIED}    

            final_result = ProductreferenceServiceHandle._serachby_item_download(request_dictionary)
            return final_result
        else:
            return {constant.MESSAGE: constant.ERROR_INVALID_PATH}