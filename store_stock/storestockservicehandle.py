'''
***********************************************************************************************************************
Purpose: Encapsulate all store stock related service methods
Developer: Somnath De (somnath.de@morrisonsplc.co.uk)
***********************************************************************************************************************
'''
from bigqueryutil import BigQueryUtil
from commonutil import CommonUtil
import constant as constant
import time
import google.cloud.logging
import logging 

client = google.cloud.logging.Client()
client.setup_logging()

# Define local constants...
FILTER_REGIONS = 'filter_regions'
FILTER_STOREFORMATS = 'filter_storeformats'
FILTER_STORES = 'filter_Stores'
FILTER_CATEGORIES = 'filter_categories'
FILTER_CLASSES = 'filter_classes'
FILTER_BRANDS = 'filter_brands'
FILTER_LOCATIONIDS = 'filter_locationids'
FILTER_SKUS = 'filter_skus'

COLUMN_REGION_NAME = 'region_name'
COLUMN_STOREFORMAT_NAME = 'storeformat_name'
COLUMN_STORE_NAME = 'location_long_name'
COLUMN_COMMERCIAL_CATEGORY = 'commercial_category'
COLUMN_COMMERCIAL_CLASS = 'commercial_class'
COLUMN_BRAND_NAME = 'brand_name'
COLUMN_SKU_ITEM_NUMBER = 'sku_item_number'
COLUMN_LOCATION_ID = 'location_id'
DIM_PRODUCT_NAME = "DIM."

STORE_STOCK_DOWNLOAD_QUERY='SELECT ' \
                        'FORMAT_DATE("%d/%m/%Y",currentDate) transactionDate, ' \
                        'UPPER(DIM.commercial_category) category, ' \
                        'UPPER(DIM.commercial_class) sub_category, ' \
                        'UPPER(DIM.brand_name) brand, ' \
                        'DIM.sku_item_number sku, ' \
                        'DIM.item_description sku_description, ' \
                        'UPPER(region_name) region, ' \
                        'UPPER(storeformat_name) storeformat, ' \
                        'location_id locationId, ' \
                        'UPPER(location_long_name) location, ' \
                        'cast(IFNULL(if(currentVolumeUnits<0,0,currentVolumeUnits),0) as int64) AS volumeUnits, ' \
                        'cast(IFNULL(if(storeOrderForecastUnits<0,0,storeOrderForecastUnits),0) as int64) as storeOrderForecastUnits, ' \
                        'cast(IFNULL(if(ROUND(currentVolumeCases)<0,0,ROUND(currentVolumeCases)),0) as int64) AS volumeCases, ' \
                        'IFNULL(if(currentValue<0,0,currentValue),0) AS value ' \
                    'FROM   ' \
     '`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) +'.sdhdatamart.store_stock_staging_summary` as ss,`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) +'.data_marts_dimensions.DIM_PRODUCT_CURRENT`as DIM  WHERE '\
    'currentDate >= "' + constant.START_DATE + '"  AND currentDate <= "' + constant.END_DATE + '" '\
    'AND vndr_nbr="' + constant.VENDOR_NUMBER + '" AND ss.sku_item_number = DIM.sku_item_number '\
    +('AND UPPER(region_name) IN (' + FILTER_REGIONS + ') '  if FILTER_REGIONS != "region_name"  else  '' )+ '' \
    +('AND UPPER(storeformat_name) IN (' + FILTER_STOREFORMATS + ') '  if FILTER_STOREFORMATS!="storeformat_name" else  '')+ '' \
    +('AND UPPER(location_long_name) IN (' + FILTER_STORES + ') '  if FILTER_STORES!="location_long_name" else  '')+ '' \
    +('AND UPPER(location_id) IN (' + FILTER_LOCATIONIDS + ') '  if FILTER_LOCATIONIDS !="location_id" else  '' )+ '' \
    +('AND UPPER(DIM.commercial_category) IN (' + FILTER_CATEGORIES + ') '  if FILTER_CATEGORIES !="commercial_category" else  '')+ '' \
    +('AND UPPER(DIM.commercial_class) IN (' + FILTER_CLASSES + ')'  if FILTER_CLASSES!="commercial_class" else  '')+ '' \
    +('AND UPPER(DIM.brand_name) IN (' + FILTER_BRANDS + ') '  if FILTER_BRANDS!="brand_name" else  '')+ ''  \
    +('AND UPPER(DIM.sku_item_number) IN (' + FILTER_SKUS + ') ' if FILTER_SKUS!="sku_item_number" else  '')+ '' \
    +'ORDER BY  currentDate desc'

STORE_STOCK_DOWNLOAD_QUERY_SELECTED_FIELDS="""
SELECT "Please Note - The SKUs for which there is no relevant data do not feature in this report." as TRANSACTIONDATE,"" as CATEGORY,"" as SUB_CATEGORY,"" as BRAND,
"" as SKU,"" as SKU_DESCRIPTION,"" as REGION,
"" as  STOREFORMAT, "" as LOCATIONID,  "" as LOCATION, 
"" AS VOLUMEUNITS,"" as STORE_ORDER_FORECAST_UNITS, 
"" AS VOLUMECASES, "" AS VALUE 
UNION ALL 
SELECT "Supplier: SELECTED_SUPPLIER_NAME" as TRANSACTIONDATE,"" as CATEGORY,"" as SUB_CATEGORY,"" as BRAND,
"" as SKU,"" as SKU_DESCRIPTION,"" as REGION,
"" as  STOREFORMAT, "" as LOCATIONID,  "" as LOCATION, 
"" AS VOLUMEUNITS,"" as STORE_ORDER_FORECAST_UNITS, 
"" AS VOLUMECASES, "" AS VALUE 
UNION ALL 
SELECT "DATE" as TRANSACTIONDATE,"CATEGORY" as CATEGORY,"SUB_CATEGORY" as SUB_CATEGORY,"BRAND" as BRAND,
"SKU" as SKU,"SKU_DESCRIPTION" as SKU_DESCRIPTION,"REGION" as REGION,
"STORE_FORMAT" as  STOREFORMAT, "STORE_ID" as LOCATIONID,  "STORE_NAME" as LOCATION, 
"STORE_STOCK_UNITS" AS VOLUMEUNITS,"STORE_ORDER_FORECAST_UNITS" as STORE_ORDER_FORECAST_UNITS, 
"STORE_STOCK_CASES" AS VOLUMECASES, "STORE_STOCK_POUND" AS VALUE
"""
STORE_STOCK_QUERY=' SELECT ' \
        'currentDate,' \
        'IFNULL(if(currentVolumeUnits<0,0,currentVolumeUnits),0) as currentVolumeUnits ,'\
        'IFNULL(if(currentValue<0,0,currentValue),0) as currentValue,'\
        'IFNULL(if(currentVolumeCases<0,0,currentVolumeCases),0) as currentVolumeCases,'\
        'IFNULL(if(storeOrderForecastUnits<0,0,storeOrderForecastUnits),0) as storeOrderForecastUnits'\
        ' from('\
    ' SELECT ' \
    'FORMAT_DATE("%Y-%m-%d",currentDate) currentDate,  SUM(currentVolumeUnits) currentVolumeUnits,  SUM(currentValue) currentValue,' \
    'SUM(currentVolumeCases) currentVolumeCases,  SUM(storeOrderForecastUnits) storeOrderForecastUnits FROM ' \
    ' `' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) +'.sdhdatamart.store_stock_staging_summary`  WHERE '\
    'currentDate >= "' + constant.START_DATE + '"  AND currentDate <= "' + constant.END_DATE + '" '\
    'AND vndr_nbr="' + constant.VENDOR_NUMBER + '" '\
    +('AND UPPER(region_name) IN (' + FILTER_REGIONS + ') '  if FILTER_REGIONS != "region_name"  else  '' )+ '' \
    +('AND UPPER(storeformat_name) IN (' + FILTER_STOREFORMATS + ') '  if FILTER_STOREFORMATS!="storeformat_name" else  '')+ '' \
    +('AND UPPER(location_long_name) IN (' + FILTER_STORES + ') '  if FILTER_STORES!="location_long_name" else  '')+ '' \
    +('AND UPPER(location_id) IN (' + FILTER_LOCATIONIDS + ') '  if FILTER_LOCATIONIDS !="location_id" else  '' )+ '' \
    +('AND UPPER(commercial_category) IN (' + FILTER_CATEGORIES + ') '  if FILTER_CATEGORIES !="commercial_category" else  '')+ '' \
    +('AND UPPER(commercial_class) IN (' + FILTER_CLASSES + ')'  if FILTER_CLASSES!="commercial_class" else  '')+ '' \
    +('AND UPPER(brand_name) IN (' + FILTER_BRANDS + ') '  if FILTER_BRANDS!="brand_name" else  '')+ ''  \
    +('AND UPPER(sku_item_number) IN (' + FILTER_SKUS + ') ' if FILTER_SKUS!="sku_item_number" else  '')+ '' \
    +'GROUP BY  currentDate) ORDER BY  currentDate ASC'



class StoreStockServiceHandle:
    @staticmethod
    def get_store_stock_data(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]

            previous_start_date = request_dictionary[constant.PREVIOUS_START_DATE]
            previous_end_date = request_dictionary[constant.PREVIOUS_END_DATE]
            start_date = request_dictionary[constant.START_DATE]
            end_date = request_dictionary[constant.END_DATE]
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]

            filter_regions = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_REGION_NAME)
            filter_store_formats = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_STOREFORMAT_NAME)
            filter_stores = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_STORE_NAME)
            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM_NUMBER)
            filter_location_ids = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_LOCATION_ID)

            query = STORE_STOCK_QUERY.replace(FILTER_REGIONS, filter_regions)
            query = query.replace(FILTER_STOREFORMATS, filter_store_formats)
            query = query.replace(FILTER_STORES, filter_stores)
            query = query.replace(FILTER_CATEGORIES, filter_categories)
            query = query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(FILTER_LOCATIONIDS, filter_location_ids)
            query = query.replace(constant.PREVIOUS_START_DATE, previous_start_date)
            query = query.replace(constant.PREVIOUS_END_DATE, previous_end_date)
            query = query.replace(constant.START_DATE, start_date)
            query = query.replace(constant.END_DATE, end_date)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(constant.KEY_PREVIOUS_START_DATE, previous_start_date.replace("-", ""))
            query = query.replace(constant.KEY_PREVIOUS_END_DATE, previous_end_date.replace("-", ""))
            query = query.replace(constant.KEY_START_DATE, start_date.replace("-", ""))
            query = query.replace(constant.KEY_END_DATE, end_date.replace("-", ""))
            
            final_result = BigQueryUtil.search_and_display(query, limit, page)
            final_result = StoreStockServiceHandle._format_output(final_result)
            return final_result
        except Exception as e:
            logging.error('Exception in get_store_stock_data - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def download_store_stock_data(request_dictionary):
        try:
            
            previous_start_date = request_dictionary[constant.PREVIOUS_START_DATE]
            previous_end_date = request_dictionary[constant.PREVIOUS_END_DATE]
            start_date = request_dictionary[constant.START_DATE]
            end_date = request_dictionary[constant.END_DATE]
            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]
            username = request_dictionary[constant.USERNAME]

            filter_regions = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_REGION_NAME)
            filter_store_formats = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_STOREFORMAT_NAME)
            filter_stores = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_STORE_NAME)
            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM_NUMBER)
            filter_location_ids = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_LOCATION_ID)

            if filter_categories.__contains__(COLUMN_COMMERCIAL_CATEGORY):
                i = filter_categories.index(COLUMN_COMMERCIAL_CATEGORY)
                filter_categories = filter_categories[:i] + DIM_PRODUCT_NAME + filter_categories[i:]
            if filter_classes.__contains__(COLUMN_COMMERCIAL_CLASS):
                i = filter_classes.index(COLUMN_COMMERCIAL_CLASS)
                filter_classes = filter_classes[:i] + DIM_PRODUCT_NAME + filter_classes[i:]
            if filter_brands.__contains__(COLUMN_BRAND_NAME):
                i = filter_brands.index(COLUMN_BRAND_NAME)
                filter_brands = filter_brands[:i] + DIM_PRODUCT_NAME + filter_brands[i:]
            if filter_skus.__contains__(COLUMN_SKU_ITEM_NUMBER):
                i = filter_skus.index(COLUMN_SKU_ITEM_NUMBER)
                filter_skus = filter_skus[:i] + DIM_PRODUCT_NAME + filter_skus[i:]

            query = STORE_STOCK_DOWNLOAD_QUERY.replace(FILTER_REGIONS, filter_regions)
            query = query.replace(FILTER_STOREFORMATS, filter_store_formats)
            query = query.replace(FILTER_STORES, filter_stores)
            query = query.replace(FILTER_CATEGORIES, filter_categories)
            query = query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.PREVIOUS_START_DATE, previous_start_date)
            query = query.replace(constant.PREVIOUS_END_DATE, previous_end_date)
            query = query.replace(constant.START_DATE, start_date)
            query = query.replace(constant.END_DATE, end_date)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            query = query.replace(FILTER_LOCATIONIDS, filter_location_ids)

            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('storeStock_', username)
            query = query.replace(constant.TABLE, file_and_table_name)
            query_columns = STORE_STOCK_DOWNLOAD_QUERY_SELECTED_FIELDS.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)

            destination_blob=file_and_table_name
            final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendor_number, 'csv')
            time.sleep(1)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('storeStock_', username)
            final_result = BigQueryUtil.create_download_table(query, destination_blob, file_and_table_name, vendor_number, 'csv')
            return final_result
        except Exception as e:
            logging.error('Exception in download_store_stock_data - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _format_output(in_final_result) -> object:
        try:
            final_result = in_final_result

            # Get the data from the output dictionary...
            records = final_result['data']

            # Add value and volume to calculate summary...

            total_value = 0
            total_units_volume = 0           
            total_cases_volume = 0          
            total_order_forecast_units =  0
            count=0

            for record in records:
                if record['currentValue'] is not None:
                    #if total_value is None:
                        #total_value = 0
                    total_value = total_value + record['currentValue']
                    record['currentValue'] = round(record['currentValue'], 2)

                if record['currentVolumeUnits'] is not None:
                    #if total_units_volume is None:
                        #total_units_volume = 0
                    total_units_volume = total_units_volume + record['currentVolumeUnits']
                    record['currentVolumeUnits'] = round(record['currentVolumeUnits'], 2)

                if record['currentVolumeCases'] is not None:
                    #if total_cases_volume is None:
                        #total_cases_volume = 0
                    total_cases_volume = total_cases_volume + record['currentVolumeCases']
                    record['currentVolumeCases'] = round(record['currentVolumeCases'], 2)
                
                if record['storeOrderForecastUnits'] is not None:
                    #if total_order_forecast_units is None:
                        #total_order_forecast_units = 0
                    total_order_forecast_units = total_order_forecast_units + record['storeOrderForecastUnits']

            if records:
                count = records[0]['full_count']
    
            total_store_stock_value = None
            if total_value is not None:
                total_store_stock_value = round(total_value/count if count!=0 else 0, 2)

            total_store_stock_units_volume = None
            total_store_stock_cases_volume = None
            total_store_stock_units_volume = StoreStockServiceHandle.get_roundofdata(total_units_volume,count)           
            total_store_stock_cases_volume = StoreStockServiceHandle.get_roundofdata(total_cases_volume,count)

            summary = {'totalStoreStockValue': total_store_stock_value,
                    'totalStoreStockUnitsVolume': total_store_stock_units_volume,
                    'totalStoreStockCasesVolume': total_store_stock_cases_volume,
                    'totalstoreOrderForecastUnits': total_order_forecast_units}

            final_records = {'summary': summary, 'detail': records}
            final_result['data'] = final_records

            return final_result
        except Exception as e:
            logging.error('Exception in _format_output - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def get_roundofdata(value,count):
        if value is not None:
             result = round((value/count)+0.0000000001) if count!=0 else 0
             return result

    @staticmethod
    def get_handle(request_dictionary):
        path = request_dictionary[constant.PATH]
        if request_dictionary[constant.VENDOR_NUMBER] == constant.DEFAULT_VENDOR_NUMBER:
            return {constant.MESSAGE : constant.ERROR_NO_VENDOR_SPECIFIED}

        if path == '/search':
            final_result = StoreStockServiceHandle.get_store_stock_data(request_dictionary)

        elif path == '/download':
            if request_dictionary[constant.USERNAME] == constant.DEFAULT_USER_NAME:
                return {constant.MESSAGE : constant.ERROR_NO_USERID_SPECIFIED}

            final_result = StoreStockServiceHandle.download_store_stock_data(request_dictionary)
        else:
            return {constant.MESSAGE : constant.ERROR_INVALID_PATH}

        return final_result

