from commonutil import CommonUtil
import constant as constant


FILTER_CATEGORIES='filter_categories'
FILTER_CLASSES = 'filter_classes'
FILTER_BRANDS = 'filter_brands'
FILTER_SKUS='filter_skus'
FILTER_TUCS='filter_tucs'
FILTER_BARCODE='filter_barcode'
FILTER_FOR_DOWNLOAD='download_filter'
FILTER_STORES='filter_stores'
FILTER_DCS= 'filter_dcs'
FILTER_ITEM_STATUS = 'filter_item_status'
FILTER_FROM_BSNS_UNIT_CD = 'filter_from_bsns_unit_cd'

STATUS_FIELD='CASE p.item_status WHEN "A" THEN(CASE WHEN date_discontinued IS NOT NULL AND (delete_flag IS NULL OR delete_flag ="N") THEN "DISCONTINUED" WHEN date_discontinued IS NULL AND (delete_flag IS NULL OR delete_flag ="N") THEN "APPROVED" END) ELSE "-" END'

DC_QUERY = u'SELECT location_id as dcNumber, location_long_name as dcName FROM '\
           u'`' + constant.DB_STRING + '' + constant.TABLE_LOCATION_CURRENT + '` '\
           u'WHERE location_type_code = "WH" AND CAST(location_id AS int64) < 1000 ORDER BY dcNumber'


STORE_QUERY=  u'WITH thisWeek AS (SELECT DATE(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY), WEEK(MONDAY)))  as monday), ' \
              u'storeStock  AS (SELECT DISTINCT DATE(TRX_DATE) as stockDate FROM '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_STORE_STOCK + '` ), ' \
              u'final_date AS (SELECT storeStock.stockDate FROM thisWeek,storeStock WHERE storeStock.stockDate >= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN thisWeek.monday WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 7 END) ' \
              u'AND storeStock.stockDate <= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN CURRENT_DATE() WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 1 END) order by 1 limit 1) ' \
              u'SELECT distinct s.BSNS_UNIT_CD storeNumber,l1.location_long_name storeName ' \
              u'FROM ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_STORE_STOCK + '` s  ' \
              u' INNER JOIN '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_LOCATION_CURRENT + '` l1' \
              u' on s.BSNS_UNIT_CD = l1.location_id and l1.storeformat_name = "Supermarket" ' \
              u'where TRX_DATE >= (select * from final_date) ' \
              u' and s.VNDR_NBR= "' + constant.VENDOR_NUMBER + '" ' \
              u'AND s.STORE_RANGE_IND = "Y" AND s.proc_plan_ind = "Y"  '\
              u'ORDER BY storeNumber ASC'  

STROE_SUMMARY_QUERY=u'WITH thisWeek AS (SELECT DATE(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY), WEEK(MONDAY)))  as monday), ' \
              u'storeStock AS  (SELECT DISTINCT DATE(TRX_DATE) as stockDate FROM '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_STORE_STOCK + '` ), ' \
              u'final_date AS (SELECT storeStock.stockDate FROM thisWeek,storeStock WHERE storeStock.stockDate >= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN thisWeek.monday WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 7 END) ' \
              u'AND storeStock.stockDate <= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN CURRENT_DATE() WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 1 END) order by 1 limit 1) ' \
              u'select dcNumber,dcName,itemStatus ,sum(numberOfItems) numberOfItems from ' \
              u'(SELECT s.FROM_BSNS_UNIT_CD dcNumber,l1.location_long_name dcName,'+ constant.STATUS_FIELD +' as itemStatus, count(distinct PIN_SKU_ITEM_NBR) numberOfItems ' \
              u'FROM ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_STORE_STOCK + '` s ' \
              u' INNER JOIN ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_PRODUCT_CURRENT+ '` p ' \
              u'on s.PIN_SKU_ITEM_NBR = p.sku_item_number ' \
              u'INNER JOIN '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_LOCATION_CURRENT + '` l1 ' \
              u'on s.FROM_BSNS_UNIT_CD = l1.location_id ' \
              u'INNER JOIN '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_LOCATION_CURRENT + '` l2 ' \
              u' on s.BSNS_UNIT_CD = l2.location_id ' \
              u'where TRX_DATE >= (select * from final_date)  ' \
              u' and s.VNDR_NBR= "' + constant.VENDOR_NUMBER + '" ' \
              u'AND UPPER(IFNULL(BSNS_UNIT_CD,"NULLVALUE")) IN ("' + constant.STORE_ID + '") ' \
              u'AND s.STORE_RANGE_IND = "Y" AND proc_plan_ind = "Y" AND WHS_ACT_DELIVERY_QTY IS NOT NULL AND WHS_ACT_DELIVERY_QTY > 0 '\
              u'AND UPPER(IFNULL(FROM_BSNS_UNIT_CD,"NULLVALUE")) IN (' + FILTER_DCS + ') ' \
              u' AND case "' + constant.ITEM_STATUS + '" '  \
              u'when "discontinued" then  p.item_status="A" and (p.date_discontinued is not null and (p.delete_flag is  null OR p.delete_flag ="N")) ' \
              u'when "approved" then p.item_status="A" and (p.date_discontinued is null  and (p.delete_flag is  null OR p.delete_flag ="N")) ' \
              u'when "" then p.item_status ="A" ' \
              u'end ' \
              u'GROUP BY s.FROM_BSNS_UNIT_CD,l1.location_long_name,item_status,date_discontinued,delete_flag) group by 1,2,3 '\
              u'ORDER BY ' + constant.SORT_DETAIL +''

STORE_ITEM_SUMMARY_QUERY= u'WITH thisWeek AS  (SELECT DATE(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY), WEEK(MONDAY)))  as monday), ' \
              u'storeStock AS (SELECT DISTINCT DATE(TRX_DATE) as stockDate FROM '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_STORE_STOCK + '` ), ' \
              u'final_date AS (SELECT storeStock.stockDate FROM thisWeek,storeStock WHERE storeStock.stockDate >= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN thisWeek.monday WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 7 END) ' \
              u'AND storeStock.stockDate <= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN CURRENT_DATE() WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 1 END) order by 1 limit 1) ' \
              u'SELECT distinct s.FROM_BSNS_UNIT_CD dcNumber,l1.location_long_name dcName,s.PIN_SKU_ITEM_NBR  || " - "|| p.Item_Description AS items, ' \
              u'CASE p.item_status ' \
              u' WHEN "A"  '\
              u' THEN(CASE WHEN p.date_discontinued IS NOT NULL AND (p.delete_flag IS NULL OR p.delete_flag ="N") THEN "DISCONTINUED" ' \
              u'WHEN p.date_discontinued IS NULL AND (p.delete_flag IS NULL OR p.delete_flag ="N") THEN "APPROVED" ' \
              u'END) ' \
              u'ELSE "-" ' \
              u'END AS itemStatus, ' \
              u'FROM ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_STORE_STOCK + '` s  ' \
              u'INNER JOIN   ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_PRODUCT_CURRENT+ '` p ' \
              u'on  s.PIN_SKU_ITEM_NBR = p.sku_item_number ' \
              u'INNER JOIN '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_LOCATION_CURRENT + '` l1' \
              u' on s.FROM_BSNS_UNIT_CD = l1.location_id  ' \
              u'INNER JOIN '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_LOCATION_CURRENT + '` l2 ' \
              u' on s.BSNS_UNIT_CD = l2.location_id ' \
              u'where TRX_DATE >= (select * from  final_date) ' \
              u' and s.VNDR_NBR= "' + constant.VENDOR_NUMBER + '" ' \
              u'AND UPPER(IFNULL(BSNS_UNIT_CD,"NULLVALUE"))  IN ("' + constant.STORE_ID + '") ' \
              u'AND s.STORE_RANGE_IND = "Y" AND proc_plan_ind = "Y" AND WHS_ACT_DELIVERY_QTY IS NOT NULL AND WHS_ACT_DELIVERY_QTY > 0 '\
              u'AND UPPER(IFNULL(FROM_BSNS_UNIT_CD,"NULLVALUE")) IN (' + FILTER_DCS + ') ' \
              u'AND case "' + constant.ITEM_STATUS + '" '  \
              u'when "discontinued" then  p.item_status="A" and (p.date_discontinued is not null and (p.delete_flag is  null OR p.delete_flag ="N")) ' \
              u'when "approved" then p.item_status="A" and (p.date_discontinued is null  and (p.delete_flag is  null OR p.delete_flag ="N")) ' \
              u'when "" then p.item_status ="A" ' \
              u'end ' \
              u'ORDER BY '+ constant.SORT_DETAIL +''

STOREITEM_DOWNLOAD_QUERY_ALL = u'WITH thisWeek AS (SELECT DATE(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY), WEEK(MONDAY)))  as monday), ' \
              u'storeStock AS (SELECT DISTINCT DATE(TRX_DATE) as stockDate  FROM '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_STORE_STOCK + '` ), ' \
              u'final_date AS (SELECT storeStock.stockDate FROM thisWeek,storeStock WHERE storeStock.stockDate >= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN thisWeek.monday WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 7 END) ' \
              u'AND storeStock.stockDate <= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN CURRENT_DATE() WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 1 END) order by 1 limit 1) ' \
              u'SELECT distinct s.BSNS_UNIT_CD STORENUMBER,UPPER((IFNULL(l2.location_long_name,"-"))) AS  STORENAME,s.FROM_BSNS_UNIT_CD DCNUMBER,UPPER((IFNULL(l1.location_long_name,"-"))) AS  DCNAME ,s.PIN_SKU_ITEM_NBR ITEMNUMBER, UPPER((IFNULL(p.item_description,"-"))) AS ITEMDESCRIPTION, ' \
              u'CASE p.item_status ' \
              u' WHEN "A"  '\
              u' THEN(CASE WHEN p.date_discontinued IS NOT NULL AND (p.delete_flag IS NULL OR p.delete_flag ="N") THEN "DISCONTINUED" ' \
              u'WHEN p.date_discontinued IS NULL AND (p.delete_flag IS NULL OR p.delete_flag ="N") THEN "APPROVED" ' \
              u'END) ' \
              u'ELSE "-" ' \
              u'END AS ITEMSTATUS, ' \
              u'FROM ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_STORE_STOCK + '` s  ' \
              u'INNER JOIN ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_PRODUCT_CURRENT+ '` p ' \
              u'on s.PIN_SKU_ITEM_NBR = p.sku_item_number ' \
              u'INNER JOIN  '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_LOCATION_CURRENT + '` l1' \
              u' on s.FROM_BSNS_UNIT_CD = l1.location_id ' \
              u'INNER JOIN  '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_LOCATION_CURRENT + '` l2 ' \
              u' on s.BSNS_UNIT_CD = l2.location_id ' \
              u'where TRX_DATE >= (select * from final_date) ' \
              u' and s.VNDR_NBR= "' + constant.VENDOR_NUMBER + '" ' \
              u'AND  UPPER(IFNULL(BSNS_UNIT_CD,"NULLVALUE")) IN ("' + constant.STORE_ID + '") ' \
              u'AND s.STORE_RANGE_IND = "Y" AND proc_plan_ind = "Y" AND WHS_ACT_DELIVERY_QTY IS NOT NULL AND WHS_ACT_DELIVERY_QTY > 0 '\
              u'GROUP BY 1,2,3,4,5,6,7 '\
              u'ORDER BY ITEMNUMBER ASC'


STOREITEM_DOWNLOAD_QUERY = u'WITH thisWeek AS (SELECT DATE(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY), WEEK(MONDAY)))  as monday), ' \
              u'storeStock AS (SELECT  DISTINCT DATE(TRX_DATE) as stockDate FROM '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_STORE_STOCK + '` ), ' \
              u'final_date AS (SELECT storeStock.stockDate FROM thisWeek,storeStock WHERE storeStock.stockDate >= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN thisWeek.monday WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 7 END) ' \
              u'AND storeStock.stockDate <= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN CURRENT_DATE() WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 1 END) order by 1 limit 1) ' \
              u'SELECT distinct s.BSNS_UNIT_CD STORENO,UPPER((IFNULL(l2.location_long_name,"-"))) STORENAME,s.FROM_BSNS_UNIT_CD DCNUMBER,UPPER((IFNULL(l1.location_long_name,"-"))) DCNAME, ' \
              u'CASE p.item_status ' \
              u' WHEN "A"  '\
              u' THEN(CASE WHEN p.date_discontinued IS NOT NULL AND (p.delete_flag IS NULL OR p.delete_flag ="N") THEN "DISCONTINUED" ' \
              u'WHEN p.date_discontinued IS NULL AND (p.delete_flag IS NULL OR p.delete_flag ="N") THEN "APPROVED" ' \
              u'END) ' \
              u'ELSE "-" ' \
              u'END AS ITEMSTATUS, ' \
              u's.PIN_SKU_ITEM_NBR ITEMNUMBER, p.Item_Description AS ITEMDESCRIPTION ' \
              u'FROM ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_STORE_STOCK + '` s ' \
              u'INNER JOIN ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_PRODUCT_CURRENT+ '` p ' \
              u'on s.PIN_SKU_ITEM_NBR = p.sku_item_number ' \
              u' INNER JOIN '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_LOCATION_CURRENT + '` l1' \
              u' on s.FROM_BSNS_UNIT_CD = l1.location_id ' \
              u'INNER JOIN '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_LOCATION_CURRENT + '` l2 ' \
              u' on s.BSNS_UNIT_CD = l2.location_id ' \
              u'where TRX_DATE >= (select * from final_date) ' \
              u'and s.VNDR_NBR= "' + constant.VENDOR_NUMBER + '" ' \
              u'AND UPPER(IFNULL(BSNS_UNIT_CD, "NULLVALUE")) IN ("' + constant.STORE_ID + '") ' \
              u'AND UPPER(IFNULL(FROM_BSNS_UNIT_CD,"NULLVALUE")) IN ("' + constant.DISTRIBUTING_DC + '") ' \
              u'AND s.STORE_RANGE_IND = "Y" AND proc_plan_ind = "Y" AND WHS_ACT_DELIVERY_QTY IS NOT NULL AND WHS_ACT_DELIVERY_QTY > 0 '\
              u'AND case "' + constant.ITEM_STATUS + '" '  \
              u'when "discontinued" then  p.item_status="A" and (p.date_discontinued is not null and (p.delete_flag is  null OR p.delete_flag ="N")) ' \
              u'when "approved" then p.item_status="A" and (p.date_discontinued is null  and (p.delete_flag is  null OR p.delete_flag ="N")) ' \
              u'ELSE  p.item_status ="A" ' \
              u'end ' \
              u'GROUP BY 1,2,3,4,5,6,7 '\
              u'ORDER BY ITEMNUMBER ASC '

STORE_SUMMARY_DOWNLOAD_QUERY = u'WITH thisWeek AS (SELECT DATE(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY), WEEK(MONDAY)))  as monday), ' \
              u'storeStock AS (SELECT DISTINCT DATE(TRX_DATE) as stockDate FROM '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_STORE_STOCK + '` ), ' \
              u'final_date AS (SELECT storeStock.stockDate FROM thisWeek,storeStock WHERE storeStock.stockDate >= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN thisWeek.monday WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 7 END) ' \
              u'AND storeStock.stockDate <= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN CURRENT_DATE() WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 1 END) order by 1 limit 1) ' \
              u'select STORENO,STORENAME, DCNUMBER,DCNAME,ITEMSTATUS ,sum(numberOfItems) NUMBEROFITEMS from ' \
              u'(SELECT distinct s.BSNS_UNIT_CD STORENO,UPPER((IFNULL(l2.location_long_name,"-"))) STORENAME,s.FROM_BSNS_UNIT_CD DCNUMBER,UPPER((IFNULL(l1.location_long_name,"-")))  DCNAME, ' \
              u''+ constant.STATUS_FIELD +'' \
              u' AS ITEMSTATUS, ' \
              u'count(distinct PIN_SKU_ITEM_NBR) numberOfItems  ' \
              u'FROM ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_STORE_STOCK + '` s  ' \
              u'INNER JOIN  ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_PRODUCT_CURRENT+ '` p ' \
              u'on s.PIN_SKU_ITEM_NBR =  p.sku_item_number ' \
              u'INNER JOIN '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_LOCATION_CURRENT + '` l1 ' \
              u' on s.FROM_BSNS_UNIT_CD = l1.location_id ' \
              u'INNER JOIN '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_LOCATION_CURRENT + '` l2 ' \
              u' on s.BSNS_UNIT_CD = l2.location_id ' \
              u'where  TRX_DATE >= (select * from final_date) ' \
              u' and s.VNDR_NBR= "' + constant.VENDOR_NUMBER + '" ' \
              u' AND UPPER(IFNULL(BSNS_UNIT_CD,"NULLVALUE")) IN ("' + constant.STORE_ID + '") ' \
              u'AND UPPER(IFNULL(FROM_BSNS_UNIT_CD,"NULLVALUE")) IN (' + FILTER_DCS + ') ' \
              u'AND  case "' + constant.ITEM_STATUS + '" '  \
              u'when "discontinued" then  p.item_status="A" and (p.date_discontinued is not null and (p.delete_flag is  null OR p.delete_flag ="N")) ' \
              u'when "approved" then p.item_status="A" and (p.date_discontinued is null  and (p.delete_flag is  null OR p.delete_flag ="N")) ' \
              u'when "" then p.item_status ="A" ' \
              u'end ' \
              u'AND s.STORE_RANGE_IND = "Y" AND proc_plan_ind = "Y" AND WHS_ACT_DELIVERY_QTY IS NOT NULL AND WHS_ACT_DELIVERY_QTY > 0 '\
              u'GROUP BY 1,2,3,4,5) GROUP BY 1,2,3,4,5 '\
              u'ORDER BY DCNUMBER ASC '                                             

STOREITEM_DOWNLOAD_QUERY_CSVALL_DF= "SELECT DISTINCT STORENUMBER,STORENAME,DCNUMBER,DCNAME,ITEMNUMBER,ITEMDESCRIPTION"+FILTER_FOR_DOWNLOAD + " FROM ("+ STOREITEM_DOWNLOAD_QUERY_ALL + ")"

STOREITEM_DOWNLOAD_QUERY_CSVALL= "SELECT DISTINCT STORENUMBER,STORENAME, "+FILTER_FOR_DOWNLOAD +" FROM ( "+ STOREITEM_DOWNLOAD_QUERY_ALL + ")" 

STOREITEM_DOWNLOAD_QUERY_CSVALL_SELECTED_FIELDS_DF ="""
SELECT "Please Note - The SKUs for which there is no relevant data do not feature in this report." AS STORENUMBER,
"" AS STORENAME, "" AS DCNUMBER,"" AS DCNAME, "" ITEMNUMBER,
"" AS ITEMDESCRIPTION FIELD_ARAAY_SELECTED
UNION ALL 
SELECT "Supplier:SELECTED_SUPPLIER_NAME" AS STORENUMBER,
"" AS STORENAME, "" AS DCNUMBER,"" AS DCNAME, "" ITEMNUMBER,
"" AS ITEMDESCRIPTION FIELD_ARAAY_SELECTED
UNION ALL
SELECT "STORE_NO." AS STORENUMBER, "STORE_NAME" AS STORENAME,
"DC_NO." AS DCNUMBER,
"DC_NAME" AS DCNAME, "ITEM_NO" AS ITEMNUMBER,
"ITEM_DESCRIPTION" AS ITEMDESCRIPTION
FIELD_ARRAY
"""
STOREITEM_DOWNLOAD_QUERY_CSVALL_SELECTED_FIELDS = """
SELECT "Please Note - The SKUs for which there is no relevant data do not feature in this report." AS STORENUMBER,
"" AS STORENAME, "" FIELD_ARAAY_SELECTED 
UNION ALL 
SELECT "Supplier:SELECTED_SUPPLIER_NAME" AS STORENUMBER,"" AS STORENAME, ""  FIELD_ARAAY_SELECTED
UNION ALL
SELECT "STORE_NO."  AS STORENUMBER,"STORE_NAME" AS STORENAME, FIELD_ARRAY
"""

STOREITEM_DOWNLOAD_QUERY_CSV_SELECTED_FIELDS ="""
SELECT "Please Note - The SKUs for which there is no relevant data do not feature in this report." AS STORENO,
"" AS STORENAME,
"" AS DCNUMBER,
"" AS DCNAME, "" AS ITEMSTATUS, "" AS ITEMNUMBER,
"" AS ITEMDESCRIPTION
UNION ALL 
SELECT "Supplier:SELECTED_SUPPLIER_NAME" AS STORENO,
"" AS STORENAME,
"" AS DCNUMBER,
"" AS DCNAME, "" AS ITEMSTATUS, "" AS ITEMNUMBER,
"" AS ITEMDESCRIPTION
UNION ALL
SELECT "STORE_NO." AS STORENO, "STORE_NAME" AS STORENAME,
"DC_NO." AS DCNUMBER,
"DC_NAME" AS DCNAME, "ITEM_STATUS" AS ITEMSTATUS, "ITEM_NO." AS ITEMNUMBER,
"ITEM_DESCRIPTION" AS ITEMDESCRIPTION
"""
STORE_SUMMARY_DOWNLOAD_QUERY_CSV_SELECTED_FIELDS ="""
SELECT "Please Note - The SKUs for which there is no relevant data do not feature in this report." AS STORENO,
"" AS STORENAME,
"" AS DCNUMBER,
"" AS DCNAME, "" AS ITEMSTATUS, "" AS NUMBEROFITEMS
UNION ALL 
SELECT "Supplier: SELECTED_SUPPLIER_NAME" AS STORENO,
"" AS STORENAME,
"" AS DCNUMBER,
"" AS DCNAME, "" AS ITEMSTATUS, "" AS NUMBEROFITEMS
UNION ALL
SELECT "STORE_NO." AS STORENO, "STORE_NAME" AS STORENAME,
"DC_NO." AS DCNUMBER,
"DC_NAME" AS DCNAME, "ITEM_STATUS" AS ITEMSTATUS, "NO._OF_ITEMS" AS NUMBEROFITEMS
"""


QUERY_GET_BY_ITEM_SUMMARY = u'WITH thisWeek AS (SELECT DATE(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY), WEEK(MONDAY))) as monday) '\
                            u',storeStock AS (SELECT   DISTINCT DATE(TRX_DATE) as stockDate FROM '\
                            u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_STORE_STOCK + '`), '\
                            u'final_date AS ( '\
                            u'SELECT storeStock.stockDate, FROM thisWeek,storeStock WHERE storeStock.stockDate >= (CASE WHEN CURRENT_DATE() > thisWeek.monday '\
                            u'THEN thisWeek.monday WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 7 END) AND storeStock.stockDate <= '\
                            u'(CASE WHEN CURRENT_DATE() > thisWeek.monday THEN CURRENT_DATE() WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 1 END) '\
                            u'order by 1 limit 1), '\
                            u'curr_ind_barcode AS ( '\
                            u'SELECT sku_item_number, barcode, tuc from '\
                            u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_BARCODE_TUC + '` A  '\
                            u'inner join  '\
                            u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_POS_IDNT + '` B '\
                            u'ON A.sku_item_number = B.sku_item_nbr WHERE curr_ind = "Y") '\
                            u'SELECT PIN_SKU_ITEM_NBR itemNumber, tuc, itemDescription, itemStatus, '\
                            u'dc, SUM(NO_OF_STORES) numberOfStores FROM ( '\
                            u'SELECT PIN_SKU_ITEM_NBR,CASE ( PIN_SKU_ITEM_NBR=SKU_ITEM_NBR) when true then B.barcode else B.tuc end AS tuc, '\
                            u'Item_Description AS itemDescription,CASE item_status WHEN "A" THEN(CASE WHEN date_discontinued IS NOT NULL AND (delete_flag IS NULL OR delete_flag ="N") '\
                            u' THEN "DISCONTINUED" WHEN date_discontinued IS NULL AND (delete_flag IS NULL OR delete_flag ="N") THEN "APPROVED" END ) ELSE "-" END '\
                            u'AS itemStatus,"All DCs" dc, COUNT(DISTINCT A.BSNS_UNIT_CD) NO_OF_STORES FROM  '\
                            u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_STORE_STOCK + '` A '\
                            u'INNER JOIN  '\
                            u'curr_ind_barcode B '\
                            u'ON A.PIN_SKU_ITEM_NBR = TRIM(B.sku_item_number) '\
                            u'INNER JOIN '\
                            u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT+ '` C '\
                            u'ON A.PIN_SKU_ITEM_NBR = C.sku_item_number '\
                            u'INNER JOIN '\
                            u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` D '\
                            u'ON A.FROM_BSNS_UNIT_CD = D.location_id '\
                            u'WHERE TRX_DATE >= (SELECT * FROM final_date) '\
                            u'AND STORE_RANGE_IND = "Y" '\
                            u'AND proc_plan_ind = "Y" '\
                            u'AND UPPER(IFNULL(FROM_BSNS_UNIT_CD,"NULLVALUE")) IN (' + constant.DISTRIBUTING_DC + ') '\
                            u'AND CASE ' + constant.ITEM_STATUS + ' '\
                            u'WHEN "DISCONTINUED" THEN   C.item_status="A" AND (C.date_discontinued IS NOT NULL AND (C.delete_flag IS NULL OR C.delete_flag ="N")) '\
                            u'WHEN "APPROVED" THEN C.item_status="A" AND (C.date_discontinued IS NULL AND (C.delete_flag IS NULL OR C.delete_flag ="N")) '\
                            u'WHEN "" THEN C.item_status="A" '\
                            u'END '\
                            u'AND UPPER(IFNULL(A.VNDR_NBR,"NULLVALUE")) IN ("' + constant.VENDOR_NUMBER + '") '\
                            u'AND UPPER(IFNULL(B.tuc,barcode)) IN (' + FILTER_TUCS + ') '\
                            u'AND UPPER(IFNULL(commercial_category,"NULLVALUE")) IN (' + FILTER_CATEGORIES + ') '\
                            u'AND UPPER(IFNULL(commercial_class,"NULLVALUE")) IN  (' + FILTER_CLASSES + ') '\
                            u'AND UPPER(IFNULL(brand_name,"NULLVALUE")) IN (' + FILTER_BRANDS + ') '\
                            u'AND UPPER(IFNULL(PIN_SKU_ITEM_NBR,"NULLVALUE")) IN (' + FILTER_SKUS + ') '\
                            u'GROUP BY '\
                            u'"Group All DCs", '\
                            u'PIN_SKU_ITEM_NBR, '\
                            u'tuc,'\
                            u'Item_Description, '\
                            u'item_status, '\
                            u'D.location_long_name, '\
                            u'delete_flag, '\
                            u'date_discontinued, '\
                            u' SKU_ITEM_NBR,barcode ) '\
                            u'GROUP BY '\
                            u'PIN_SKU_ITEM_NBR, '\
                            u'tuc, '\
                            u'itemDescription, '\
                            u'itemStatus, '\
                            u'DC '\
                            u'ORDER BY '+ constant.SORT_DETAIL +''                        
QUERY_GET_BY_ITEM_DETAIL = u'WITH thisWeek AS (SELECT DATE(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY), WEEK(MONDAY))) as monday),storeStock AS '\
                           u'(SELECT  DISTINCT DATE(TRX_DATE) as stockDate FROM '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_STORE_STOCK + '`), '\
                           u'final_date AS ('\
                           u'SELECT storeStock.stockDate FROM thisWeek,storeStock WHERE storeStock.stockDate >= (CASE WHEN CURRENT_DATE() > thisWeek.monday '\
                           u'THEN thisWeek.monday WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 7 END) AND storeStock.stockDate <= '\
                           u'(CASE WHEN CURRENT_DATE() > thisWeek.monday THEN CURRENT_DATE() WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 1 END) '\
                           u'order by 1 limit 1), '\
                           u'curr_ind_barcode AS ( '\
                           u'SELECT sku_item_number, barcode, tuc from '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_BARCODE_TUC + '` A '\
                           u'inner join  '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_POS_IDNT + '` B '\
                           u'ON A.sku_item_number = B.sku_item_nbr WHERE curr_ind = "Y") '\
                           u'SELECT PIN_SKU_ITEM_NBR itemNumber,CASE ( PIN_SKU_ITEM_NBR=SKU_ITEM_NBR) when true then B.barcode else B.tuc end AS tuc,Item_Description itemDescription, '\
                           u'CASE item_status '\
                           u'WHEN "A" THEN(CASE WHEN date_discontinued IS NOT NULL AND (delete_flag IS NULL OR delete_flag ="N") THEN "DISCONTINUED"  '\
                           u'WHEN date_discontinued IS NULL AND (delete_flag IS NULL OR delete_flag ="N") THEN "APPROVED" END) '\
                           u'ELSE "-" END AS itemStatus, '\
                           u'A.FROM_BSNS_UNIT_CD || " - " || D.location_long_name dc, '\
                           u'A.BSNS_UNIT_CD || " - " || E.location_long_name stores FROM '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_STORE_STOCK + '` A '\
                           u'INNER JOIN '\
                           u'curr_ind_barcode B '\
                           u'ON  A.PIN_SKU_ITEM_NBR = TRIM(B.sku_item_number) '\
                           u'INNER JOIN '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT+ '` C '\
                           u'ON A.PIN_SKU_ITEM_NBR = C.sku_item_number '\
                           u'INNER JOIN  '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` D '\
                           u'ON A.FROM_BSNS_UNIT_CD = D.location_id  '\
                           u'INNER JOIN '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` E '\
                           u'ON A.BSNS_UNIT_CD = E.location_id '\
                           u'WHERE TRX_DATE >= (SELECT * FROM final_date ) '\
                           u'AND STORE_RANGE_IND = "Y"  '\
                           u'AND proc_plan_ind = "Y" '\
                           u'AND UPPER(IFNULL(A.VNDR_NBR,"NULLVALUE")) IN ("' + constant.VENDOR_NUMBER + '") '\
                           u'AND UPPER(IFNULL(FROM_BSNS_UNIT_CD,"NULLVALUE"))  IN (' + constant.DISTRIBUTING_DC + ') '\
                           u'AND CASE ' + constant.ITEM_STATUS + ' WHEN "DISCONTINUED" THEN  C.item_status="A" AND (C.date_discontinued IS NOT NULL AND (C.delete_flag IS NULL OR C.delete_flag ="N")) '\
                           u'WHEN "APPROVED" THEN  C.item_status="A" AND (C.date_discontinued IS NULL AND (C.delete_flag IS NULL OR C.delete_flag ="N")) '\
                           u'WHEN "" THEN C.item_status="A" END '\
                           u'AND UPPER(IFNULL(B.tuc,barcode)) IN (' + constant.ITEM_TUC + ') '\
                           u'AND UPPER(IFNULL(PIN_SKU_ITEM_NBR,"NULLVALUE")) IN ("' + constant.ITEM_NUMBER + '")  '\
                           u'GROUP BY PIN_SKU_ITEM_NBR, '\
                           u'tuc, '\
                           u'Item_Description, '\
                           u'item_status, '\
                           u'FROM_BSNS_UNIT_CD, '\
                           u'E.location_long_name, '\
                           u'A.BSNS_UNIT_CD, '\
                           u'D.location_long_name, '\
                           u'delete_flag, '\
                           u'date_discontinued,SKU_ITEM_NBR,barcode '\
                           u'ORDER BY '+ constant.SORT_DETAIL +''

QUERY_BY_ITEM_SUMMARY_DL  = u'WITH thisWeek AS (SELECT DATE(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY), WEEK(MONDAY))) as monday) '\
                            u',storeStock AS (SELECT   DISTINCT DATE(TRX_DATE) as stockDate FROM '\
                            u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_STORE_STOCK + '`), '\
                            u'final_date AS ( '\
                            u'SELECT storeStock.stockDate, FROM thisWeek,storeStock WHERE storeStock.stockDate >= (CASE WHEN CURRENT_DATE() > thisWeek.monday '\
                            u'THEN thisWeek.monday WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 7 END) AND storeStock.stockDate <= '\
                            u'(CASE WHEN CURRENT_DATE() > thisWeek.monday THEN CURRENT_DATE() WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 1 END) '\
                            u'order by 1 limit 1), '\
                            u'curr_ind_barcode AS ( '\
                            u'SELECT sku_item_number, barcode, tuc from '\
                            u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_BARCODE_TUC + '` A  '\
                            u'inner join '\
                            u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_POS_IDNT + '` B '\
                            u'ON A.sku_item_number = B.sku_item_nbr WHERE curr_ind = "Y") '\
                            u'SELECT UPPER((IFNULL(commercial_category,"-"))) commercial_category,UPPER((IFNULL(commercial_class,"-"))) commercial_class,UPPER((IFNULL(brand_name,"-"))) brand_name,PIN_SKU_ITEM_NBR itemNumber, tuc,UPPER((IFNULL(itemDescription,"-"))) itemDescription, itemStatus, '\
                            u'dc,SUM(NO_OF_STORES) numberOfStores FROM ( '\
                            u'SELECT PIN_SKU_ITEM_NBR,CASE ( PIN_SKU_ITEM_NBR=SKU_ITEM_NBR) WHEN TRUE THEN B.barcode ELSE B.tuc END AS tuc,C.commercial_category,C.commercial_class,C.brand_name, '\
                            u'Item_Description AS itemDescription,CASE item_status WHEN "A" THEN(CASE WHEN date_discontinued IS NOT NULL AND (delete_flag IS NULL OR delete_flag ="N") '\
                            u' THEN "DISCONTINUED" WHEN date_discontinued IS NULL AND (delete_flag IS NULL OR delete_flag ="N") THEN "APPROVED" END ) ELSE "-" END '\
                            u'AS itemStatus,"All DCs" dc, COUNT(DISTINCT A.BSNS_UNIT_CD) NO_OF_STORES FROM  '\
                            u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_STORE_STOCK + '` A '\
                            u'INNER JOIN '\
                            u'curr_ind_barcode B '\
                            u'ON A.PIN_SKU_ITEM_NBR = TRIM(B.sku_item_number) '\
                            u'INNER JOIN '\
                            u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT+ '` C '\
                            u'ON A.PIN_SKU_ITEM_NBR = C.sku_item_number  '\
                            u'INNER JOIN '\
                            u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` D '\
                            u'ON A.FROM_BSNS_UNIT_CD = D.location_id '\
                            u'WHERE TRX_DATE >= (SELECT * FROM final_date) '\
                            u'AND STORE_RANGE_IND = "Y" '\
                            u'AND proc_plan_ind = "Y" '\
                            u'AND UPPER(IFNULL(FROM_BSNS_UNIT_CD,"NULLVALUE")) IN (' + constant.DISTRIBUTING_DC + ') '\
                            u'AND case ' + constant.ITEM_STATUS + ' '  \
                            u'when "discontinued" then  C.item_status="A" and (C.date_discontinued is not null and (C.delete_flag is  null OR C.delete_flag ="N")) ' \
                            u'when "approved" then C.item_status="A" and (C.date_discontinued is null  and (C.delete_flag is  null OR C.delete_flag ="N")) ' \
                            u'ELSE  C.item_status ="A" ' \
                            u'END '\
                            u'AND UPPER(IFNULL(A.VNDR_NBR,"NULLVALUE")) IN ("' + constant.VENDOR_NUMBER + '") '\
                            u'AND UPPER(IFNULL(B.tuc,barcode)) IN (' + FILTER_TUCS + ') '\
                            u'AND UPPER(IFNULL(commercial_category,"NULLVALUE")) IN (' + FILTER_CATEGORIES + ') '\
                            u'AND UPPER(IFNULL(commercial_class,"NULLVALUE")) IN  (' + FILTER_CLASSES + ') '\
                            u'AND UPPER(IFNULL(brand_name,"NULLVALUE")) IN (' + FILTER_BRANDS + ') '\
                            u'AND UPPER(IFNULL(PIN_SKU_ITEM_NBR,"NULLVALUE")) IN (' + FILTER_SKUS + ') '\
                            u'GROUP BY '\
                            u'"Group All DCs", '\
                            u'PIN_SKU_ITEM_NBR, '\
                            u'tuc,SKU_ITEM_NBR,Item_Description, ' \
                            u'item_status, '\
                            u'D.location_long_name, '\
                            u'delete_flag, '\
                            u'date_discontinued,C.commercial_category,C.commercial_class,C.brand_name,barcode ) '\
                            u'GROUP BY '\
                            u'PIN_SKU_ITEM_NBR, '\
                            u'tuc, '\
                            u'itemDescription, '\
                            u'itemStatus, '\
                            u'DC,commercial_category,commercial_class,brand_name ORDER BY PIN_SKU_ITEM_NBR ASC'\

QUERY_BYITEM_DC_DL = u'WITH thisWeek AS (SELECT DATE(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY), WEEK(MONDAY))) as monday),storeStock AS '\
                           u'(SELECT DISTINCT DATE(TRX_DATE) as stockDate FROM '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_STORE_STOCK + '`), '\
                           u'final_date AS ('\
                           u'SELECT  storeStock.stockDate FROM thisWeek,storeStock WHERE storeStock.stockDate >= (CASE WHEN CURRENT_DATE() > thisWeek.monday '\
                           u'THEN thisWeek.monday WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 7 END) AND storeStock.stockDate <= '\
                           u'(CASE WHEN CURRENT_DATE() > thisWeek.monday THEN CURRENT_DATE() WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 1 END) '\
                           u'order by 1 limit 1), '\
                           u'curr_ind_barcode AS ( '\
                           u'SELECT sku_item_number, barcode, tuc from '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_BARCODE_TUC + '` A  '\
                           u'inner join '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_POS_IDNT + '` B '\
                           u'ON A.sku_item_number = B.sku_item_nbr WHERE curr_ind = "Y") '\
                           u'SELECT UPPER((IFNULL(C.commercial_category,"-"))) commercial_category,UPPER((IFNULL(C.commercial_class,"-"))) commercial_class,UPPER((IFNULL( C.brand_name,"-"))) brand_name, PIN_SKU_ITEM_NBR itemNumber,CASE ( PIN_SKU_ITEM_NBR=SKU_ITEM_NBR) when true then B.barcode else B.tuc end  AS TUC,UPPER((IFNULL(C.Item_Description,"-"))) itemDescription, '\
                           u'CASE item_status '\
                           u'WHEN "A" THEN(CASE WHEN date_discontinued IS NOT NULL AND (delete_flag IS NULL OR delete_flag ="N") THEN "DISCONTINUED"  '\
                           u'WHEN date_discontinued IS NULL AND (delete_flag IS NULL OR delete_flag ="N") THEN "APPROVED" END) '\
                           u'ELSE "-" END AS itemStatus, '\
                           u'A.FROM_BSNS_UNIT_CD || " - " || D.location_long_name dc, '\
                           u'A.BSNS_UNIT_CD || " - " || E.location_long_name stores FROM '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_STORE_STOCK + '` A '\
                           u'INNER JOIN '\
                           u'curr_ind_barcode B '\
                           u'ON A.PIN_SKU_ITEM_NBR = TRIM(B.sku_item_number)  '\
                           u'INNER JOIN '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT+ '` C '\
                           u'ON A.PIN_SKU_ITEM_NBR = C.sku_item_number '\
                           u'INNER JOIN '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` D '\
                           u'ON A.FROM_BSNS_UNIT_CD = D.location_id '\
                           u'INNER JOIN '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` E '\
                           u'ON A.BSNS_UNIT_CD = E.location_id '\
                           u'WHERE TRX_DATE >= (SELECT * FROM final_date ) '\
                           u'AND  STORE_RANGE_IND = "Y"  '\
                           u'AND proc_plan_ind = "Y" '\
                           u'AND UPPER(IFNULL(A.VNDR_NBR,"NULLVALUE")) IN ("' + constant.VENDOR_NUMBER + '") '\
                           u'AND UPPER(IFNULL(FROM_BSNS_UNIT_CD,"NULLVALUE")) IN (' + constant.DISTRIBUTING_DC + ') '\
                           u'AND case ' + constant.ITEM_STATUS + ''  \
                           u'when "discontinued" then  C.item_status="A" and (C.date_discontinued is not null and (C.delete_flag is  null OR C.delete_flag ="N")) ' \
                           u'when "approved" then C.item_status="A" and (C.date_discontinued is null  and (C.delete_flag is  null OR C.delete_flag ="N")) ' \
                           u'ELSE  C.item_status ="A" END ' \
                           u'AND UPPER(IFNULL(B.tuc,barcode)) IN (' + constant.ITEM_TUC + ') '\
                           u'AND UPPER(IFNULL(PIN_SKU_ITEM_NBR,"NULLVALUE")) IN ("' + constant.ITEM_NUMBER + '")  '\
                           u'GROUP BY PIN_SKU_ITEM_NBR, '\
                           u'TUC,SKU_ITEM_NBR, '\
                           u'Item_Description, '\
                           u'item_status, '\
                           u'FROM_BSNS_UNIT_CD, '\
                           u'E.location_long_name, '\
                           u'A.BSNS_UNIT_CD, '\
                           u'D.location_long_name, '\
                           u'C.commercial_category,C.commercial_class,C.brand_name, '\
                           u'C.date_discontinued,C.delete_flag,barcode '\
                           u'ORDER BY A.BSNS_UNIT_CD'                            
                           
QUERY_BY_ITEM_SUMMARY_SELECTED_FIELDS= """
SELECT "Kindly note that in a particular week a store may be sourced from more than one depot. Thus the total number of stores displayed in the report may not necessarily reflect the actual number of WM stores." AS commercial_category,
"" AS commercial_class,
"" AS brand_name,
"" AS itemNumber, "" AS TUC, "" AS Item_Description,
"" AS itemStatus,"" AS DC, "" AS numberOfStores
UNION ALL 
SELECT "Supplier:SELECTED_SUPPLIER_NAME" AS commercial_category,
"" AS commercial_class,
"" AS brand_name,
"" AS itemNumber, "" AS TUC, "" AS Item_Description,
"" AS itemStatus,"" AS DC, "" AS numberOfStores
UNION ALL
SELECT "CATEGORY" AS commercial_category, "SUBCATEGORY" AS commercial_class,
"BRAND" AS brand_name,
"ITEM_NO." AS itemNumber, "TUC" AS TUC, "ITEM_DESCRIPTION" AS Item_Description,
"ITEM_STATUS" AS itemStatus,"DC" AS DC, "NO._OF_STORES" AS numberOfStores
"""  

QUERY_BYITEM_DC_SELECTED_FIELDS= """
SELECT "Kindly note that in a particular week a store may be sourced from more than one depot. Thus the total number of stores displayed in the report may not necessarily reflect the actual number of WM stores." AS commercial_category,
"" AS commercial_class,
"" AS brand_name,
"" AS itemNumber, "" AS TUC, "" AS Item_Description,
"" AS itemStatus,"" AS dc, "" AS stores
UNION ALL 
SELECT "Supplier:SELECTED_SUPPLIER_NAME" AS commercial_category,
"" AS commercial_class,
"" AS brand_name,
"" AS itemNumber, "" AS TUC, "" AS Item_Description,
"" AS itemStatus,"" AS dc, "" AS stores
UNION ALL
SELECT "CATEGORY" AS commercial_category, "SUBCATEGORY" AS commercial_class,
"BRAND" AS brand_name,
"ITEM_NO." AS itemNumber, "TUC" AS TUC, "ITEM_DESCRIPTION" AS Item_Description,
"ITEM_STATUS" AS itemStatus,"DC" AS dc, "STORES" AS stores
"""

QUERY_BYITEM_CSVALL_DL = u'WITH thisWeek AS  (SELECT DATE(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY), WEEK(MONDAY))) as monday),storeStock AS '\
                           u'(SELECT DISTINCT DATE(TRX_DATE) as stockDate FROM '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_STORE_STOCK + '`), '\
                           u'final_date AS ('\
                           u'SELECT storeStock.stockDate FROM thisWeek,storeStock WHERE storeStock.stockDate >= (CASE WHEN CURRENT_DATE() > thisWeek.monday '\
                           u'THEN thisWeek.monday WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 7 END) AND storeStock.stockDate <= '\
                           u'(CASE WHEN CURRENT_DATE() > thisWeek.monday THEN CURRENT_DATE() WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 1 END) '\
                           u'order by 1 limit 1), '\
                           u'curr_ind_barcode AS ( '\
                           u'SELECT sku_item_number, barcode, tuc  from '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_BARCODE_TUC + '` A '\
                           u'inner join '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_POS_IDNT + '` B '\
                           u'ON A.sku_item_number = B.sku_item_nbr WHERE curr_ind = "Y") '\
                           u'SELECT UPPER((IFNULL(C.commercial_category,"-")))  CATEGORY,UPPER((IFNULL(C.commercial_class,"-"))) SUBCATEGORY,UPPER((IFNULL(C.brand_name,"-"))) BRAND, PIN_SKU_ITEM_NBR ITEMNUMBER,CASE ( PIN_SKU_ITEM_NBR=SKU_ITEM_NBR) when true then B.barcode else B.tuc end  AS TUC,UPPER((IFNULL(C.Item_Description,"-"))) ITEMDESCRIPTION, '\
                           u'CASE item_status '\
                           u'WHEN "A" THEN(CASE WHEN date_discontinued IS NOT NULL AND (delete_flag IS NULL OR delete_flag ="N") THEN "DISCONTINUED"  '\
                           u'WHEN date_discontinued IS NULL AND (delete_flag IS NULL OR delete_flag ="N") THEN "APPROVED" END) '\
                           u'ELSE "-" END AS ITEMSTATUS, '\
                           u'A.FROM_BSNS_UNIT_CD || " - " || D.location_long_name DC, '\
                           u'A.BSNS_UNIT_CD || " - " || E.location_long_name STORES FROM '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_STORE_STOCK + '` A '\
                           u'INNER JOIN '\
                           u'curr_ind_barcode B '\
                           u'ON A.PIN_SKU_ITEM_NBR = TRIM(B.sku_item_number) '\
                           u'INNER JOIN '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT+ '` C '\
                           u'ON  A.PIN_SKU_ITEM_NBR = C.sku_item_number '\
                           u'INNER JOIN '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` D '\
                           u'ON  A.FROM_BSNS_UNIT_CD = D.location_id '\
                           u'INNER JOIN '\
                           u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` E '\
                           u'ON A.BSNS_UNIT_CD = E.location_id '\
                           u'WHERE TRX_DATE >= (SELECT * FROM final_date ) '\
                           u'AND STORE_RANGE_IND = "Y"  '\
                           u'AND  proc_plan_ind = "Y" '\
                           u'AND UPPER(IFNULL(A.VNDR_NBR,"NULLVALUE")) IN ("' + constant.VENDOR_NUMBER + '") '\
                           u'GROUP BY PIN_SKU_ITEM_NBR, '\
                           u'TUC, '\
                           u'Item_Description, '\
                           u'item_status, '\
                           u'FROM_BSNS_UNIT_CD, '\
                           u'E.location_long_name, '\
                           u'A.BSNS_UNIT_CD, '\
                           u'D.location_long_name, '\
                           u'C.commercial_category,C.commercial_class,C.brand_name, '\
                           u'C.date_discontinued,C.delete_flag '\
                           u'ORDER BY A.BSNS_UNIT_CD ASC'

QUERY_BYITEM_CSVALL_DF= "SELECT DISTINCT CATEGORY,SUBCATEGORY,BRAND,ITEMNUMBER,ITEMDESCRIPTION,DC,STORES"+FILTER_FOR_DOWNLOAD + " FROM  ("+ QUERY_BYITEM_CSVALL_DL + ")"

QUERY_BYITEM_CSVALL= "SELECT DISTINCT CATEGORY,SUBCATEGORY,BRAND, "+FILTER_FOR_DOWNLOAD +"  FROM ("+ QUERY_BYITEM_CSVALL_DL + ")" 

BYITEM_QUERY_CSVALL_SELECTED_FIELDS_DF ="""
SELECT "Kindly note that in a particular week a store may be sourced from more than one depot. Thus the total number of stores displayed in the report may not necessarily reflect the actual number of WM stores." AS CATEGORY,
"" AS SUBCATEGORY, "" AS BRAND,"" AS ITEMNUMBER, "" ITEMDESCRIPTION,
"" AS DC,"" AS STORES
FIELD_ARAAY_SELECTED
UNION ALL 
SELECT "Supplier:SELECTED_SUPPLIER_NAME" AS CATEGORY,
"" AS SUBCATEGORY, "" AS BRAND,"" AS ITEMNUMBER, "" ITEMDESCRIPTION,
"" AS DC,"" AS STORES
FIELD_ARAAY_SELECTED
UNION ALL
SELECT "CATEGORY" AS CATEGORY, "SUBCATEGORY" AS SUBCATEGORY,
"BRAND" AS BRAND,
"ITEM_NO." AS ITEMNUMBER, "ITEM_DESCRIPTION" ITEMDESCRIPTION,
"DC" AS DC,"STORES" AS STORES
FIELD_ARRAY
"""
BYITEM_QUERY_CSVALL_SELECTED_FIELDS = """
SELECT "Kindly note that in a particular week a store may be sourced from more than one depot. Thus the total number of stores displayed in the report may not necessarily reflect the actual number of WM stores."  AS CATEGORY,
"" AS SUBCATEGORY, "" AS BRAND,"" FIELD_ARAAY_SELECTED 
UNION ALL  
SELECT "Supplier:SELECTED_SUPPLIER_NAME" AS CATEGORY,
"" AS SUBCATEGORY, "" AS BRAND,"" FIELD_ARAAY_SELECTED 
UNION ALL
SELECT  "CATEGORY" AS CATEGORY,
"SUBCATEGORY" AS SUBCATEGORY, "BRAND" AS BRAND, FIELD_ARRAY
"""                                                 
