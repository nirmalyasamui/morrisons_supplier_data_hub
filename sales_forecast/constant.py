# Please have one file for each environment and copy properly during deployment...
# Environment variables actual values will be set by the build script during deployment...
CSV_FILE_STORAGE_BUCKET='CSV_FILE_STORAGE_BUCKET'
DATA_INTEGRATION_PROJECT = 'DATA_INTEGRATION_PROJECT'
SUPPLIER_DATA_HUB_PROJECT = 'SUPPLIER_DATA_HUB_PROJECT'
DATA_TRANSFER_SERVICE = 'DATA_TRANSFER_SERVICE'
COMS_PROJECT = 'COMS_PROJECT'
JOB_ID_PREFIX = 'JOB_ID_PREFIX'
DB_STRING='DB_STRING'
PO_STARTDATE='po_startdate'
STATUS_FIELD='statusfield'
QUERIES = 'queries'
QUERY_TTL = 'QUERY_TTL'
QUERIES_LIMIT = 'QUERIES_LIMIT'
LAST_USED_TIME = 'lastUsedTime'
CREATED_TIME = 'createdTime'
QUERIES_COLLECTION = 'QUERIES_COLLECTION'
SOLD_PERIOD = 'SOLD_PERIOD'
SOLD_WEEK ="SOLD_WEEK"
SOLD_DATE ="SOLD_DATE"

# Define all string constants here...
START_DATE = 'startDate'
END_DATE = 'endDate'
PREVIOUS_PO_STARTDATE='previousPOStartDate'
PREVIOUS_START_DATE = 'previousStartDate'
PREVIOUS_END_DATE = 'previousEndDate'
KEY_START_DATE = 'startKeyDate'
KEY_END_DATE = 'endKeyDate'
KEY_PREVIOUS_START_DATE = 'previousStartKeyDate'
KEY_PREVIOUS_END_DATE = 'previousEndKeyDate'
FILTERS = 'filters'
LIMIT = 'limit'
LOCATION = 'EU'
METHOD = 'method'
MESSAGE = 'message'
PAGE = 'page'
OFFSET = 'offset'
PATH = 'path'
REPORTING_PERIOD = 'reportingPeriod'
COMPARISON_PERIOD = 'comparisonPeriod'
TARGET = 'target'
SAVE = 'save'
STARTS_WITH = 'startswith'
VENDOR_NUMBER = 'vendornumber'
TABLE = 'download_table_name'
USERID = 'userid'
USERNAME = 'username'
REPORT_NAME = 'reportname'
PROJECT = 'data-supplierdatahub-dev'
DATASET = 'download'
PO_NUMBER = 'ponumber'
PROMOTION_NUMBER = 'promotionnumber'
PROMOTION_START_DATE = 'promotionstartdate'
PROMOTION_END_DATE = 'promotionenddate'
LOCATION_TYPE = 'locationType'
QUERY_TITLE = 'querytitle'
SALE_TYPE = 'saleType'
CONDITION = 'CONDITION'
QUERY_ORDER = 'ASC'
YEAR_TO_DATE = 'Year to Date'
MONTH_TO_DATE = 'Month to Date'
WEEK_TO_DATE = 'Week to Date'
LAST_FULL_WEEK = 'Last Full Week'
LAST_DAY = 'Latest Day'
WEEKS_52 = '52 Weeks'
WEEK_52 = 'Week 52'
DAY_52 = 'Day 52'
DAY_PREV_YEAR = 'Day Previous year'
PREVIOUS_DAY = 'Previous Day'
PREVIOUS_WEEK = 'Previous Week'
WEEKS_12 = '12 Weeks'
WEEKS_4 = '4 Weeks'
CUSTOM = 'Custom'
DAY = 'DAY'
WEEK = 'WEEK'
LAST_SEVEN_DAYS = 'Last Seven Days'
WEEKS_13 = '13 Weeks'
SUPPLIER_NAME="suppliername"
SELECTED_SUPPLIER_NAME="SELECTED_SUPPLIER_NAME"
ITEM_NUMBER='itemnumber'
ITEM_STATUS='itemstatus'
DELIVERY='delivery'
PROMOIND='promotionStatus'
FIELD_ARRAY='FIELD_ARRAY'
FIELD_ARAAY_SELECTED='FIELD_ARAAY_SELECTED'
DOWNLOAD_DIC='DOWNLOAD_DIC'
COLUMN1 = 'column1'
COLUMN2 = 'column2'
CURRENT_FORECAST_DATE='CURRENT_TIMESTAMP()'
SORT_BY='sortBy'
SORT_BY_27='sortBy'
SORT_DIRECTION='sortDirection'
SORT_DIRECTION_27='sortDirection'
DEFAULT_SORT_BY='currentForecast'
DEFAULT_SORT_BY_27 = 'sku'
DEFAULT_SORT_DIRECTION='DESC'
DEFAULT_SORT_DIRECTION_27='ASC'
SORT_DETAIL='sortDetail'
DATES= 'dateList'
DL_FILTER= 'download_filter'
SF_PATH= ['/getLongRangeSkuSalesForecast','/getLongRangeCombinedSalesForecast']
PR_PATH= ['/getdetails']
DEFAULT_LIST=['ITEMDESCRIPTION','ITEMNUMBER','TUC']

# Applicationwise default values
DEFAULT_END_DATE = 'None'
DEFAULT_REPORTING_PERIOD = 'None'
DEFAULT_START_DATE = 'None'
DEFAULT_VENDOR_NUMBER = '-99999999'
DEFAULT_USERID = '-99999999'
DEFAULT_USER_NAME = '-99999999'
DEFAULT_REPORT_NAME = None
DATE_FORMAT_DMYHM = "%d-%m-%Y %H:%M"
DEFAULT_DATE_FORMAT = '%Y-%m-%d'
REV_DATE_FORMAT = "%d-%m-%Y"
DATE_FORMAT_YMD = '%Y%m%d'
DATE_FORMAT_YMDHMS = '%Y-%m-%d %H:%M:%S'
DATE_FORMAT_YMDHMSF = '%Y%m%d%H%M%S%f'
DEFAULT_PO_NUMBER = '-99999999'
DEFAULT_PROMO_NUMBER = '-99999999'
DEFAULT_LOCATION_TYPE="STORE"+"'"+","+"'"+"WH"
DEFAULT_SALE_TYPE = None
DEFAULT_QUERY_TITLE = None
DEFAULT_SUPPLIER_NAME="NA"
DEFAULT_ITEM_STATUS=  None
DEFAULT_SORT_BY_PR='itemNumber'
DEFAULT_SORT_DIRECTION='DESC'
DEFAULT_ITEM_NUMBER= '-99999999'
TRX_DT='" ' \
                u'AND DATE(purchaseOrder.TRX_DT) <= "'
DWV_X_PO_ITEM_STATE_GCP= '.source_data_partitioned.DWV_X_PO_ITEM_STATE_GCP` purchaseOrder, ' \
                u'`'
STATUS_MAP='.references.purchase_order_status_map` purchase_order_status_map, ' \
                u'`'
DIM_PRODUCT_CURRENT='.data_marts_dimensions.DIM_PRODUCT_CURRENT` product, ' \
                u'`'
DIM_LOCATION_CURRENT= '.data_marts_dimensions.DIM_LOCATION_CURRENT` location, ' \
                u'`'
DWR_VNDR_ITEM= '.references.DWR_VNDR_ITEM` DWR_VNDR_ITEM ' \
                u'WHERE ' \
                u'DATE(purchaseOrder.TRX_DT) >= "'
                
COLUMN_COMMERCIAL_CATEGORY = 'commercial_category'
FILTER_CATEGORIES = 'filter_categories'

# Define HTTP services methods...
HTTP_GET = 'GET'
HTTP_POST = 'POST'
HTTP_PUT = 'PUT'
HTTP_DELETE = 'DELETE'
HTTP_OPTIONS = 'OPTIONS'

SUCCESS_MESSAGE = 'success'

# Define all error messages here...
ERROR_NO_VENDOR_SPECIFIED = 'No vendor number specified in the service call'
ERROR_NO_ITEMNO_SPECIFIED = 'No item number specified in the service call'
ERROR_NO_REPORT_SPECIFIED = 'No report name specified in the service call'
ERROR_NO_USERID_SPECIFIED = 'No user name specified in the service call'
ERROR_INVALID_PATH = 'Invalid path'
ERROR_NO_DATA_FOUND = 'No data found for the given parameters'
ERROR_UNPROCESSABLE_ENTITY = "Unprocessable Entity"
ERROR_METHOD_NOT_SUPPORTED = "API Method not supported"
ERROR_NO_PO_SPECIFIED = 'No purchase order specified in the service call'
ERROR_INTERNAL_SERVER = 'Internal server error'
ERROR_MAX_LIMIT_REACHED = 'Operation not allowed. Maximum limit reached'
ERROR_NO_QUERY_TITLE_SPECIFIED = 'No query title specified in the service call'
ERROR_DUPLICATE_ENTITY = 'The combination specified already exists'
ERROR_NO_REPORTING_PERIOD_SPECIFIED = 'No Reporting period specified in the service call'
ERROR_NO_SALE_TYPE_SPECIFIED = 'No Sale type specified in the service call'
ERROR_FOREECAST_NOT_AVAILABLE = 'Forecast is not available at the moment or the product is not a sellable line'
ERROR_RESPONSE_TOO_LARGE='Error response too large'
ERROR_NO_STORE_SPECIFIED= 'No store specified in the service call'
ERROR_NO_DC_SPECIFIED= 'No dc specified in the service call'
CLIENT_ERROR = "Client Error"

# Error codes foerrors
ERROR_CODE_UNPROCESSABLE_ENTITY = 422
ERROR_CODE_BAD_REQUEST = 422
ERROR_CODE_METHOD_NOT_SUPPORTED = 405
ERROR_CODE_NO_DATA_FOUND = 404
ERROR_CODE_ALREADY_FOUND = 409
ERROR_CODE_INTERNAL_SERVER = 500
# Other non string constants
HTTP_SUCCESS_CODE_200 = 200
HTTP_SUCCESS_CODE_201 = 201
HTTP_SUCCESS_CODE_202 = 202
HTTP_SUCCESS_CODE_203 = 203
HTTP_SUCCESS_CODE_204 = 204
HTTP_SUCCESS_CODE_205 = 205

#added for depotstock report
DEFAULT_STOCK_TYPE = None
DEFAULT_STOCK_MEASURE = None
STOCK_TYPE = 'stock'
ALLOWED_STOCK_TYPE = ['total', 'depot', 'store']
STOCK_MEASURE = 'stockmeasure'
ALLOWED_STOCK_MEASURE = ['value', 'volume']
ERROR_NO_STOCK_TYPE_SPECIFIED = 'No stock type specified in the service call'
ERROR_NO_STOCK_MEASURE_SPECIFIED = 'No stock measure specified in the service call'
ERROR_NOT_A_VALID_STOCK_TYPE = 'Not a valid stock type provided in the service call.'
ERROR_NOT_A_VALID_STOCK_MEASURE = 'Not a valid stock measure provided in the service call.'
ERROR_NO_SUPPLIER_NAME_SPECIFIED = 'No supplier name specified in the service call'
DEPOT_VALUE = 'depotValue'
DEPOT_VOLUME = 'depotVolume'
STORE_VALUE = 'storeValue'
STORE_VOLUME = 'storeVolume'
TOTAL_VOLUME = 'totalVolume'
TOTAL_VALUE = 'totalValue'
R_DATE = 'r_date'
DATE = 'date'
SKU = 'sku'
DEPOT = 'depot'
DEPOT_STOCK = 'depotStock'
STORE_STOCK = 'storeStock'
TOTAL_STOCK = 'totalStock'
STOCK_COVER = 'stockCover'
TOTAL_STOCK_COVER = 'totalStockCover'
DAYS_28 = '28 Days'
DS_PATH= ['/depotwisestock','/skuwisestock','/depotstock/download']
#end of changes to depotstock report


#added for wastage & markdown report
FILTER_CLASSES = 'filter_classes'
FILTER_BRANDS = 'filter_brands'
FILTER_SKUS = 'filter_skus'
FILTER_STORE = 'filter_store'

COLUMN_BRAND_NAME = 'brand_name'
COLUMN_SKU_ITEM_NUMBER = 'sku_item_number'
COLUMN_STORE = 'store'
ITEM_DESCRIPTION ='item_description'
COLUMN_COMMERCIAL_CLASS = 'commercial_class'
SKU_ITEM_NUMBER = 'sku_item_number'
LOCATION_NAME = 'location_long_name'

WASTE_TYPE = 'wasteType'
WASTE_MEASURE = 'wasteMeasure'
WASTE_BY = 'wasteBy'
WASTE_AS =  'wasteAs'

PREV_28_DAYS = 'prev_28_days'
PREV_YEAR_28_DAYS = 'prev_year_28_days'
PREV_YEAR = 'prev_year'
PREV_WEEKS_52 = 'prev_weeks_52'
PREV_WEEKS_12 = 'prev_weeks_12'
PREV_YEAR_WEEKS_12 = 'prev_year_weeks_12'
PREV_YEAR_WEEKS_4 = 'prev_year_weeks_4'
PREV_WEEKS_4 = 'prev_weeks_4'
WM_PATH= ['/waste/summary','/waste/table','/waste/download']


DEFAULT_COMPARISON_PERIOD='lastYear'
DEFAULT_WASTE_TYPE = 'total'
DEFAULT_WASTE_MEASURE = 'value'
DEFAULT_WASTE_AS = 'percentOfSales'
DEFAULT_WASTE_BY='sku'
DEFAULT_REPORTING_PERIOD_WM = 'yearToDate'
DEFAULT_SORT_BY_WM = DEFAULT_REPORTING_PERIOD_WM
CHART='chart'
TABLE1='table'
SUMMARY='summary'

YTD='yearToDate'
LFW='lastFullWeek'
WTD='weekToDate'
LD='latestDay'
W52='fiftyTwoWeeks'
W12='twelveWeeks'
W4='fourWeeks'

#end of changes to wastage & markdown report
#added for availability

DEFAULT_REPORTING_PERIOD_AP = 'lastFullWeek'
DEFAULT_TARGET = '98'
DEFAULT_COMPARISON_PERIOD_AP='target'
PREVIOUS = 'previous'
LAST_YEAR = 'lastYear'
AVAILABILITY_BY = 'availabilityBy'
DEFAULT_AVAILABILITY_BY = 'sku'
DEFAULT_SORT_BY_AP ='lastFullWeek'

#added for rangedproducts

STORE='store'
STORE_ID='storeId'
DEFAULT_LIST_RP=['ITEMDESCRIPTION','ITEMNUMBER','DCNAME','DCNUMBER']
DEFAULT_LISTITEM_RP=['ITEMDESCRIPTION','ITEMNUMBER','DC','STORE']
RP_PATH=['/getstoresummary','/getstore/itemsummary','/storeitemsummary/download/all','/itemstoresummary/download/all','/storeitemsummary/download','/storesummary/download','/getByItemSummary','/getByItemSummaryDetail','/getByItemDetail','/itemsummary/download','/itemdcsummary/download','/itemdcdetail/download']
DISTRIBUTING_DC='distributingDC'
ITEM_TUC = 'item_tuc'
ITEMSTATUS= 'itemStatus'
DOWNLOAD_FILTER= 'downloadFilter'
DEFAULT_STORE='-99999999'
DEFAULT_DC='-99999999'
DEFAULT_SORT_DIRECTION_RP= 'dcNumber'


#added for Supplier Service
DOWNLOAD_SALESCHANGE ='/download/sales/changereport'
SS_PATH=['/depotinbound/shortedcases','/storeinbound/shortedcases','/storeinbound/shortedcases/download','/depotinbound/shortedcases/download','/lowestavailableproducts/download',DOWNLOAD_SALESCHANGE]
DEFAULT_DEPOT_STOCK= 'cases'
DEPOTSTOCK_FIELD='depotstock_field'
SALE_MEASURE = 'salemeasure'
SALE_CHANNEL = "saleChannel"
ERROR_NO_SALE_MEASURE_SPECIFIED = 'No sale measure specified in the service request'
DEFAULT_SORT_BY_SS='shortPercent'
VALUE_CHANNEL = """ "SUPERMARKET","GROCERY HOME DELIVERY" """
VOLUME_CHANNEL = """ "SUPERMARKET","GROCERY HOME DELIVERY","WHOLESALE" """
SALE_PATH = ['/wastereport','/productsreport','/salesreport','/salesreportsummary','/sales/changereport',DOWNLOAD_SALESCHANGE]
SALES_CHANGE_PATH = ['/sales/changereport',DOWNLOAD_SALESCHANGE]

SELECT_FIELD= 'select_field'
PYTD='prev_yearToDate'
PLFW='prev_lastFullWeek'
PWTD='prev_weekToDate'
PLD='prev_latestDay'
PW52='prev_fiftyTwoWeeks'
PW12='prev_twelveWeeks'
PW4='prev_fourWeeks'


#Table Names

TABLE_SOURCE_VNDR_ITEM = ".source_data_partitioned.DWR_VNDR_ITEM"
TABLE_PRODUCT_CURRENT = ".data_marts_dimensions.DIM_PRODUCT_CURRENT"
TABLE_LOCATION_CURRENT = ".data_marts_dimensions.DIM_LOCATION_CURRENT"
TABLE_STORE_STOCK = ".supply_chain_summary.store_stock_summary"
TABLE_SDH_VNDR_ITEM = ".sdhdatamart.VNDR_SKU_ITEMSTATEGCP_EXTRACT"
TABLE_AVAILABILITY_SUMMARY = ".sdhdatamart.availability_staging_summary"
TABLE_AVAILABILITY_CHART = ".sdhdatamart.availability_staging_chart"
TABLE_WM_SUMMARY = ".sdhdatamart.wastage_markdown_staging_summary"
TABLE_WM_CHART = ".sdhdatamart.wastage_markdown_staging_chart"
TABLE_ITEM_STATE_GCP = ".source_data_partitioned.DWV_X_PO_ITEM_STATE_GCP"
TABLE_REF_VNDR_ITEM = ".references.DWR_VNDR_ITEM"
TABLE_SUPPLIER_TUC = ".supply_chain_tables.Item_Supplier_TUC"
TABLE_PO_STATUS_MAP = ".references.purchase_order_status_map"
TABLE_SALES_FORECAST = ".sdhdatamart.sales_forecast_staging"
TABLE_BARCODE_TUC = ".data_marts_dimensions.vw_min_pin_barcode_tuc"
TABLE_TIME_DAY = ".source_data_partitioned.DWV_TIME_DAY"
TABLE_SDH_SALES = ".data_marts.salesDatamartSDHSummary"
TABLE_SUPPLIER_ITEM_DEPOT = ".sdhdatamart.Supplier-Item-Depot"
TABLE_DEPOT_CROP_POSITION = ".sdhdatamart.Daily-Depot-Corp-Position"
TABLE_ITEM_COLLCTN = ".source_data_partitioned.DWR_SKU_ITEM_COLLCTN"
TABLE_POS_IDNT = ".source_data_partitioned.DWR_POS_IDNT"

#Added for Sales


CLASS_FIELD=') AND UPPER(product.commercial_class) IN ('
BRAND_FIELD= ') AND UPPER(product.brand_name) IN ('
ACT_DLVRY_FIELD='" AND DATE(ACT_DLVRY_DT) >= "'
ACT_DLVRYLESS_FIELD = '"  AND DATE(ACT_DLVRY_DT) <= "'
CATEGORY_FIELD=') AND UPPER(product.commercial_category) IN ('
LOCATION_FIELD=') AND UPPER(location.location_id) IN ('
STORENAME_FIELD=') AND UPPER(location.storeformat_name) IN ('
SKU_FIELD =') AND UPPER(product.sku_item_number) IN ('
SUBCHANNEL_FIELD =') AND UPPER(sales.sub_channel) IN ('
EFFDT_FIELD='"  AND DATE(EFF_TO_DT)>= "'
EFF_FRMDT_FIELD ='" AND (DATE(EFF_FROM_DT)<= "'

SALESCHANNEL_FIELD =') AND sales.channel = '
GHD='Grocery Home Delivery'
DATE_FIELD='derivedDate.d_date'
TRX_DATE_FIELD='sales.transaction_sold_date'
FORMAT_TRXDATE='FORMAT_DATE("%d/%m/%Y",sales.transaction_sold_date)'
FORMAT_DATE='FORMAT_DATE("%d/%m/%Y",d_date)'

AVAILABILITY_STAGING_SUMMARY_TABLE= ".sdhdatamart.availability_staging_summary"

ROUND_OFF = """>0 THEN ROUND(("""
ROUND_PERC = """)*100,1)- ROUND(("""
ELSE_FIELD = """)*100 ,1) ELSE 0 END AS """
GREATER_ZERO = """>0 and """ 
ROUNDOFF_WITHNOT =""")<>0 THEN ROUND(((ROUND(SUM(""" 
ROUND_OFF1 = """),1))/ROUND(SUM("""
SUM_OVER =""") - SUM("""
FORMAT_STRING = """)*100 ,1) ELSE 0 END AS STRING),'%') AS """
CASE_FIELD = """,
CASE WHEN """
TWO_ROUNDOFF =""">0 THEN ROUND(ROUND(("""
FORMAT_INT =""")*100,1)- 0,1)
WHEN """
LESSER_ONE_ROUNDOFF = """<1 THEN ROUND(ROUND(("""
LESSER_ONE ="""<1 and """
FORMAT_INT_WITHELSE=""")*100,1),1)
  ELSE 0 END AS """
GREATER_ZERO_ROUNDOFF =""">0 THEN ROUND(0- ROUND(("""   
AS_FIELD =""") AS """
ONE_AS_FIELD = """), 1) AS """
SUM_FIELD =""",
SUM("""
CONCAT_FIELD = """,
CONCAT(CAST( CASE WHEN """
ROUND_SUM = """,ROUND(SUM("""
CASE_SUM_FIELD = """,CASE WHEN SUM("""
ROUND_ELSE_FIELD = """),1))*100,1) ELSE 0 END AS """
ROUND_SUM_FIELD = """),1) - ROUND(SUM("""


#Added  for product reference
SKU_FILTER = 'UPPER(IFNULL(sku_item_number,"NULLVALUE"))'
SKU_DEFAULT ='UPPER(IFNULL(d.sku_item_number,"NULLVALUE"))'
TUC_FILTER ='UPPER(IFNULL(tuc,"NULLVALUE"))'
BARCODE_DEFAULT_RP = '(UPPER(IFNULL(B.tuc,barcode)))'
BARCODE_DEFAULT = 'UPPER(IFNULL(barcode,"NULLVALUE"))'
IF_NULL = 'IFNULL('
IFNULL_BSNSCD ='IFNULL(FROM_BSNS_UNIT_CD,'
SKU_REPLACE = 'IFNULL(PIN_SKU_ITEM_NBR,'
AND_FIELD = '" AND "'
IFNULL_SKU ='IFNULL(sku,'
APPROVED ='"APPROVED"'
DISCONTINUED = '"DISCONTINUED"'
UPPER_IFNULL = 'UPPER(IFNULL('
NULL_VALUE = ',"NULLVALUE"))'
ALLDC_REPLACE = 'A.FROM_BSNS_UNIT_CD || " - " || D.location_long_name'
ALL_DC = '"All DCs"'
GROUP_ALLDC = '"Group All DCs"'
GROUP_ALLDC_REPLACE = 'A.FROM_BSNS_UNIT_CD, D.location_long_name'
GROUP_DCS = '"Group All DCs",'

QUERY_BYTITLE = '/getquerybytitle'

