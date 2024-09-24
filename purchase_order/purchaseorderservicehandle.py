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
import logging 

client = google.cloud.logging.Client()
client.setup_logging()


FILTER_PURCHASE_ORDER_STATUSES = 'filter_purchase_order_statuses'
FILTER_LOCATIONIDS = 'filter_locationids'
FILTER_SKUS = 'filter_skus'
BQ_CASE = 'UPPER('
PURCHASE_ORDER = 'purchaseOrder'
LOCATION = 'location'
PRODUCT = 'product'
VNDR_ITEM = 'dwr_vndr_item'


COLUMN_PCHSE_ORDR_STATUS = 'pchse_ordr_status'
COLUMN_LOCATION_ID = 'location_id'
COLUMN_SKU_ITEM_NUMBER = 'sku_item_number'
# we replaced the item_name to item_description
# we replaced the ordr_qty to ORDR_QTY_REPORTED in where clause to every query
# we replaced the qty_received to QTY_RECEIVED_REPORTED in all query
# we replaced the ordr_qty to (CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END)
PURCHASE_ORDER_QUERY = u'select t1.purchase_order_number,t2.order_date,t2.location_id,t2.location_short_name,t2.location_long_name,t2.location_type_code,t2.pchse_ordr_status,t2.pchse_ordr_status_desc,t2.scheduled_delivery_date,t2.scheduled_delivery_time,t2.lines_order,t2.cases_order,t2.cases_delivered from(SELECT DISTINCT purchaseOrder.PCHSE_ORDR_NBR purchase_order_number FROM ' \
                u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_ITEM_STATE_GCP + '` '+ PURCHASE_ORDER +', ' \
             u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` '+ LOCATION +', ' \
               u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` '+ PRODUCT +', ' \
                  u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_REF_VNDR_ITEM + '` '+ VNDR_ITEM +', ' \
                u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_PO_STATUS_MAP + '` purchase_order_status_map ' \
                u'WHERE ' \
                u'DATE(purchaseOrder.trx_dt) >= "' + constant.START_DATE + '" ' \
                u'AND DATE(purchaseOrder.trx_dt)  <= "' + constant.END_DATE + '" ' \
                u'AND CAST(purchaseOrder.bsns_unit_cd AS INT64) = CAST(location.location_id AS INT64) ' \
                u'AND CAST(purchaseOrder.sku_item_nbr AS INT64) = CAST(product.sku_item_number AS INT64) ' \
                u'AND dwr_vndr_item.vndr_item_nbr = product.sku_item_number ' \
                u'AND DATE(purchaseOrder.trx_dt) = dwr_vndr_item.ACTIVE_DATE ' \
                u'AND UPPER(purchase_order_status_map.pchse_ordr_status) = UPPER(purchaseOrder.pchse_ordr_status) ' \
                u'AND ORDR_QTY_REPORTED > 0 ' \
                u'AND purchaseOrder.pchse_ordr_status IN (' + FILTER_PURCHASE_ORDER_STATUSES + ') ' \
                u'AND UPPER(location.location_id)  IN (' + FILTER_LOCATIONIDS + ') ' \
                u'AND UPPER(IFNULL(purchaseOrder.sku_item_nbr,"NULLVALUE")) IN (' + FILTER_SKUS + ') ' \
                u'AND UPPER(location.status_code) != "I" ' \
                u'AND dwr_vndr_item.vndr_nbr = "' + constant.VENDOR_NUMBER + '" ' \
                u'AND purchaseOrder.VNDR_NBR  = "' + constant.VENDOR_NUMBER + '") t1 ' \
                u'inner join  (SELECT ' \
                u'pchse_ordr_nbr purchase_order_number, ' \
                u'SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 1, 4) || ' \
                       u'"-"||SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 5, 2) || ' \
                       u'"-"||SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 7, 2) ' \
                u'order_date, ' \
                u'bsns_unit_cd AS location_id, ' \
                u'location_short_name, ' \
                u'location_long_name, ' \
                u'location_type_code, ' \
                u'UPPER(purchase_order_status_map.pchse_ordr_status) pchse_ordr_status, ' \
                u'purchase_order_status_map.pchse_ordr_status_desc pchse_ordr_status_desc, ' \
                u'CAST(DATE(schl_dlvry_dt) AS STRING) scheduled_delivery_date, ' \
                u'CAST(TIME(schl_dlvry_dt) AS STRING) scheduled_delivery_time, ' \
                       u'COUNT(1) AS lines_order, SUM(CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END) cases_order, SUM(CASE WHEN QTY_RECEIVED_REPORTED IS NULL THEN 0 ELSE QTY_RECEIVED_REPORTED END) cases_delivered ' \
             u'FROM ' \
                u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_ITEM_STATE_GCP + '` '+ PURCHASE_ORDER +', ' \
              u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` '+ LOCATION +', ' \
                 u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` '+ PRODUCT +', ' \
                   u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_REF_VNDR_ITEM + '` '+ VNDR_ITEM +', ' \
             u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_PO_STATUS_MAP + '` purchase_order_status_map ' \
             u'WHERE ' \
                u'DATE(purchaseOrder.trx_dt) >=  "' + constant.START_DATE + '" ' \
                u'AND DATE(purchaseOrder.trx_dt) <= "' + constant.END_DATE + '" ' \
                u'AND CAST(purchaseOrder.bsns_unit_cd AS INT64) = CAST(location.location_id AS INT64) ' \
                u'AND CAST(purchaseOrder.sku_item_nbr AS INT64) = CAST(product.sku_item_number AS INT64) ' \
                u'AND dwr_vndr_item.vndr_item_nbr = product.sku_item_number ' \
                u'AND DATE(purchaseOrder.trx_dt) = dwr_vndr_item.ACTIVE_DATE ' \
                u'AND UPPER(purchase_order_status_map.pchse_ordr_status) = UPPER(purchaseOrder.pchse_ordr_status) ' \
                u'AND ORDR_QTY_REPORTED > 0 ' \
                u'AND purchaseOrder.pchse_ordr_status IN (' + FILTER_PURCHASE_ORDER_STATUSES + ') ' \
                u'AND UPPER(location.location_id) IN  (' + FILTER_LOCATIONIDS + ') ' \
                u'AND UPPER(location.status_code) != "I" ' \
                u'AND  dwr_vndr_item.vndr_nbr = "' + constant.VENDOR_NUMBER + '" ' \
                u'AND purchaseOrder.VNDR_NBR = "' + constant.VENDOR_NUMBER + '" ' \
            u'GROUP BY ' \
                u'PCHSE_ORDR_NBR, ORGNL_ORDR_DT_KEY, BSNS_UNIT_CD, LOCATION_SHORT_NAME, LOCATION_LONG_NAME, LOCATION_TYPE_CODE, purchase_order_status_map.pchse_ordr_status, purchase_order_status_map.pchse_ordr_status_desc, SCHL_DLVRY_DT) t2 on t1.purchase_order_number =t2.purchase_order_number ' \
            u'ORDER BY ' \
                u'purchase_order_number, order_date, location_id, LOCATION_SHORT_NAME, LOCATION_LONG_NAME, LOCATION_TYPE_CODE, pchse_ordr_status, pchse_ordr_status_desc, scheduled_delivery_date, scheduled_delivery_time ASC'

PURCHASE_ORDER_SEARCH_QUERY = u'SELECT distinct ' \
                u'pchse_ordr_nbr purchase_order_number, ' \
                u'SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 1, 4) || ' \
                       u'"-"||SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 5, 2) || ' \
                       u'"-"||SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 7, 2) ' \
                u'order_date, ' \
                u'bsns_unit_cd AS location_id, ' \
                u'location_short_name, ' \
                u'location_long_name, ' \
                u'location_type_code, ' \
                u'sku_item_nbr morrisons_item_number, ' \
                u't.tuc tuc, ' \
                u'item_description item_name, ' \
                u'(CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END) order_quantity, ' \
                u'CASE ' \
                    u'WHEN QTY_RECEIVED_REPORTED IS NULL THEN 0 ' \
                    u'ELSE QTY_RECEIVED_REPORTED ' \
                u'END AS received_quantity, ' \
                u'CASE ' \
                    u'WHEN QTY_RECEIVED_REPORTED IS NULL OR QTY_RECEIVED_REPORTED = 0 THEN "-" ' \
                    u'ELSE CAST(ROUND(((QTY_RECEIVED_REPORTED) / (CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END)) * 100, 2) AS STRING) ' \
                u'END AS percentage_delivered, ' \
                u'CASE ' \
                    u'WHEN QTY_RECEIVED_REPORTED IS NULL OR QTY_RECEIVED_REPORTED = 0 THEN "-" ' \
                    u'WHEN (CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END) < QTY_RECEIVED_REPORTED THEN "0" ' \
                    u'ELSE CAST(ROUND((((CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END) - QTY_RECEIVED_REPORTED) / (CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END)) * 100, 2) AS STRING) ' \
                u'END AS percentage_short, ' \
                u'CASE ' \
                    u'WHEN QTY_RECEIVED_REPORTED IS NULL OR QTY_RECEIVED_REPORTED = 0 THEN "-" ' \
                    u'WHEN (CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END) > QTY_RECEIVED_REPORTED THEN "0" ' \
                    u'ELSE CAST(ROUND(((QTY_RECEIVED_REPORTED - (CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END)) / (CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END)) * 100, 2) AS STRING) ' \
                u'END AS percentage_over, ' \
                u'UPPER(purchase_order_status_map.pchse_ordr_status) pchse_ordr_status, ' \
                u'purchase_order_status_map.pchse_ordr_status_desc pchse_ordr_status_desc, ' \
                u'CAST(DATE(schl_dlvry_dt) AS STRING) scheduled_delivery_date, ' \
                u'CAST(TIME(schl_dlvry_dt) AS STRING) scheduled_delivery_time ' \
             u'FROM ' \
                u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_ITEM_STATE_GCP + '` '+ PURCHASE_ORDER +', ' \
               u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.supply_chain_tables.Item_Supplier_TUC`  t, ' \
                u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` '+ LOCATION +', ' \
                  u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` '+ PRODUCT +', ' \
                u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_REF_VNDR_ITEM + '` '+ VNDR_ITEM +', ' \
              u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_PO_STATUS_MAP + '` purchase_order_status_map ' \
                u'WHERE DATE(purchaseOrder.trx_dt) IN (SELECT DISTINCT DATE(purchaseOrder.trx_dt)  FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_ITEM_STATE_GCP + '` purchaseOrder WHERE DATE(purchaseOrder.trx_dt ) >= CURRENT_DATE() - 731 AND DATE(purchaseOrder.trx_dt) <= CURRENT_DATE() AND  pchse_ordr_nbr = "' + constant.PO_NUMBER + '") AND CAST(purchaseOrder.bsns_unit_cd AS INT64) = CAST(location.location_id AS INT64) AND UPPER(location.status_code) != "I" AND CAST(purchaseOrder.sku_item_nbr AS INT64) = CAST(product.sku_item_number AS INT64) AND CAST(t.ITEM AS INT64) = CAST(purchaseOrder.sku_item_nbr AS INT64) AND t.ebs_supplier_number=CAST("' + constant.VENDOR_NUMBER + '" AS INT64) AND dwr_vndr_item.vndr_item_nbr = product.sku_item_number AND DATE(purchaseOrder.trx_dt) = dwr_vndr_item.ACTIVE_DATE AND UPPER(purchase_order_status_map.pchse_ordr_status) = UPPER(purchaseOrder.pchse_ordr_status) AND ORDR_QTY_REPORTED > 0 AND purchaseOrder.VNDR_NBR = "' + constant.VENDOR_NUMBER + '" AND  dwr_vndr_item.vndr_nbr = "' + constant.VENDOR_NUMBER + '" AND pchse_ordr_nbr = "' + constant.PO_NUMBER + '" ORDER BY purchase_order_number, ORDER_DATE, morrisons_item_number, location_id, order_quantity, pchse_ordr_status, scheduled_delivery_date ASC'


PURCHASE_ORDER_SEARCH_QUERY_SUMMARY = u'SELECT distinct ' \
                u'pchse_ordr_nbr purchase_order_number, ' \
                u'SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 1, 4) || ' \
                       u'"-"||SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 5, 2) || ' \
                       u'"-"||SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 7, 2) ' \
                u'order_date, ' \
                u'bsns_unit_cd AS location_id, ' \
                u'location_short_name, ' \
                u'location_long_name, ' \
                u'location_type_code, COUNT(1) lines_ordered, ' \
                u'SUM(CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END) total_number_of_cases_ordered, ' \
                u'SUM(CASE ' \
                    u'WHEN QTY_RECEIVED_REPORTED IS NULL THEN 0 ' \
                    u'ELSE QTY_RECEIVED_REPORTED ' \
                u'END) AS total_number_of_cases_delivered, ' \
                u'UPPER(purchase_order_status_map.pchse_ordr_status) pchse_ordr_status, ' \
                u'purchase_order_status_map.pchse_ordr_status_desc pchse_ordr_status_desc, ' \
                u'CAST(DATE(schl_dlvry_dt) AS STRING) scheduled_delivery_date, ' \
                u'CAST(TIME(schl_dlvry_dt) AS STRING) scheduled_delivery_time ' \
             u'FROM ' \
                u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_ITEM_STATE_GCP + '` '+ PURCHASE_ORDER +', ' \
                 u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` '+ LOCATION +', ' \
                   u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` '+ PRODUCT +', ' \
             u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_REF_VNDR_ITEM + '` '+ VNDR_ITEM +', ' \
               u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_PO_STATUS_MAP + '` purchase_order_status_map ' \
                u'WHERE DATE(purchaseOrder.trx_dt) IN (SELECT DISTINCT DATE(purchaseOrder.trx_dt) FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_ITEM_STATE_GCP + '` purchaseOrder WHERE DATE(purchaseOrder.trx_dt ) >= CURRENT_DATE() - 731 AND DATE(purchaseOrder.trx_dt) <= CURRENT_DATE() AND pchse_ordr_nbr = "' + constant.PO_NUMBER + '") AND CAST(purchaseOrder.bsns_unit_cd AS INT64) = CAST(location.location_id AS INT64) AND UPPER(location.status_code) != "I" AND CAST(purchaseOrder.sku_item_nbr AS INT64) = CAST(product.sku_item_number AS INT64) AND dwr_vndr_item.vndr_item_nbr = product.sku_item_number AND DATE(purchaseOrder.trx_dt) = dwr_vndr_item.ACTIVE_DATE AND UPPER(purchase_order_status_map.pchse_ordr_status) = UPPER(purchaseOrder.pchse_ordr_status) AND ORDR_QTY_REPORTED > 0 AND purchaseOrder.VNDR_NBR = "' + constant.VENDOR_NUMBER + '" AND dwr_vndr_item.vndr_nbr = "' + constant.VENDOR_NUMBER + '" AND pchse_ordr_nbr = "' + constant.PO_NUMBER + '" ' \
                u'GROUP BY ' \
                u'PCHSE_ORDR_NBR, ORGNL_ORDR_DT_KEY, BSNS_UNIT_CD, LOCATION_SHORT_NAME, LOCATION_LONG_NAME, LOCATION_TYPE_CODE, purchase_order_status_map.pchse_ordr_status, purchase_order_status_map.pchse_ordr_status_desc, SCHL_DLVRY_DT ' \
                u'ORDER BY ' \
                u'purchase_order_number, order_date, location_id, LOCATION_SHORT_NAME, LOCATION_LONG_NAME, LOCATION_TYPE_CODE, pchse_ordr_status, pchse_ordr_status_desc, scheduled_delivery_date, scheduled_delivery_time ASC'



PURCHASE_ORDER_SEARCH_QUERY_DL = u'SELECT distinct ' \
                u'pchse_ordr_nbr purchase_Order_Number, ' \
                u'SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 1, 4) || ' \
                       u'"-"||SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 5, 2) || ' \
                       u'"-"||SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 7, 2) ' \
                u'order_date, ' \
                u'bsns_unit_cd AS location_Id, ' \
                u'location_long_name location_Long_Name, ' \
                u'sku_item_nbr Sku, ' \
                u't.tuc tuc, ' \
                u'item_description item_name, ' \
                u'(CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END) order_Quantity, ' \
                u'CASE ' \
                    u'WHEN QTY_RECEIVED_REPORTED IS NULL THEN 0 ' \
                    u'ELSE QTY_RECEIVED_REPORTED ' \
                u'END AS received_Quantity, ' \
                u'CASE ' \
                    u'WHEN QTY_RECEIVED_REPORTED IS NULL OR QTY_RECEIVED_REPORTED = 0 THEN "0 %" ' \
                    u'ELSE CONCAT(CAST(ROUND(((QTY_RECEIVED_REPORTED) / (CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END))* 100, 2)  AS STRING)," %") ' \
                u'END AS percentage_Delivered, ' \
                u'purchase_order_status_map.pchse_ordr_status_desc purchase_Order_Status, ' \
                u'CAST(DATE(schl_dlvry_dt) AS STRING) scheduled_Delivery_Date, ' \
                u'CAST(TIME(schl_dlvry_dt) AS STRING) scheduled_Delivery_Time ' \
             u'FROM ' \
                u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_ITEM_STATE_GCP + '` '+ PURCHASE_ORDER +', ' \
                  u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.supply_chain_tables.Item_Supplier_TUC` t,  ' \
                u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` '+ LOCATION +', ' \
                u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` '+ PRODUCT +', ' \
              u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_REF_VNDR_ITEM + '` '+ VNDR_ITEM +', ' \
                 u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_PO_STATUS_MAP + '` purchase_order_status_map ' \
             u'WHERE DATE(purchaseOrder.trx_dt) IN (SELECT DISTINCT DATE(purchaseOrder.trx_dt) FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_ITEM_STATE_GCP + '` purchaseOrder WHERE DATE(purchaseOrder.trx_dt ) >= CURRENT_DATE() - 731 AND DATE(purchaseOrder.trx_dt) <= CURRENT_DATE() AND pchse_ordr_nbr = "' + constant.PO_NUMBER + '") AND CAST(purchaseOrder.bsns_unit_cd AS INT64) = CAST(location.location_id AS INT64)  AND UPPER(location.status_code) != "I" AND CAST(purchaseOrder.sku_item_nbr AS INT64) = CAST(product.sku_item_number AS INT64) AND CAST(t.ITEM as INT64) = CAST(purchaseOrder.sku_item_nbr AS INT64) AND t.ebs_supplier_number=CAST("' + constant.VENDOR_NUMBER + '" AS INT64) AND dwr_vndr_item.vndr_item_nbr = product.sku_item_number AND DATE(purchaseOrder.trx_dt) = dwr_vndr_item.ACTIVE_DATE AND UPPER(purchase_order_status_map.pchse_ordr_status) = UPPER(purchaseOrder.pchse_ordr_status) AND pchse_ordr_nbr = "' + constant.PO_NUMBER + '" AND ORDR_QTY_REPORTED > 0 AND purchaseOrder.VNDR_NBR = "' + constant.VENDOR_NUMBER + '" AND   dwr_vndr_item.vndr_nbr = "' + constant.VENDOR_NUMBER + '" ORDER BY order_date DESC,PURCHASE_ORDER_NUMBER ASC'

PURCHASE_ORDER_QUERY_SUMMARY_DL=u'SELECT distinct ' \
                u'pchse_ordr_nbr purchase_order_number, ' \
                u'SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 1, 4) || ' \
                       u'"-"||SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 5, 2) || ' \
                       u'"-"||SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 7, 2) ' \
                u'order_date, ' \
                u'bsns_unit_cd AS location_id, ' \
                u'location_short_name, ' \
                u'location_long_name, ' \
                u'location_type_code, COUNT(1) lines_ordered, ' \
                u'SUM(CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END) total_number_of_cases_ordered, ' \
                u'SUM(CASE ' \
                    u'WHEN QTY_RECEIVED_REPORTED IS NULL THEN 0 ' \
                    u'ELSE QTY_RECEIVED_REPORTED ' \
                u'END) AS total_number_of_cases_delivered, ' \
                u'UPPER(purchase_order_status_map.pchse_ordr_status) pchse_ordr_status, ' \
                u'purchase_order_status_map.pchse_ordr_status_desc pchse_ordr_status_desc, ' \
                u'CAST(DATE(schl_dlvry_dt) AS STRING) scheduled_delivery_date, ' \
                u'CAST(TIME(schl_dlvry_dt) AS STRING) scheduled_delivery_time ' \
             u'FROM ' \
                u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_ITEM_STATE_GCP + '` '+ PURCHASE_ORDER +', ' \
                   u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` '+ LOCATION +', ' \
             u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` '+ PRODUCT +', ' \
               u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_REF_VNDR_ITEM + '` '+ VNDR_ITEM +', ' \
                  u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_PO_STATUS_MAP + '` purchase_order_status_map ' \
                 u'WHERE  DATE(purchaseOrder.trx_dt) IN (SELECT DISTINCT DATE(purchaseOrder.trx_dt) FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_ITEM_STATE_GCP + '` purchaseOrder WHERE DATE(purchaseOrder.trx_dt ) >= CURRENT_DATE() - 731 AND DATE(purchaseOrder.trx_dt) <= CURRENT_DATE()  AND pchse_ordr_nbr IN (select distinct purchase_Order_Number from detail)) AND CAST(purchaseOrder.bsns_unit_cd AS INT64) = CAST(location.location_id AS INT64)  AND UPPER(location.status_code) != "I" AND CAST(purchaseOrder.sku_item_nbr AS INT64) = CAST(product.sku_item_number AS INT64) AND dwr_vndr_item.vndr_item_nbr = product.sku_item_number AND DATE(purchaseOrder.trx_dt) = dwr_vndr_item.ACTIVE_DATE AND UPPER(purchase_order_status_map.pchse_ordr_status) = UPPER(purchaseOrder.pchse_ordr_status) AND ORDR_QTY_REPORTED > 0 AND purchaseOrder.VNDR_NBR = "' + constant.VENDOR_NUMBER + '"  AND dwr_vndr_item.vndr_nbr = "' + constant.VENDOR_NUMBER + '" AND pchse_ordr_nbr IN (select distinct purchase_Order_Number from detail) ' \
                u'GROUP BY ' \
                u'PCHSE_ORDR_NBR, ORGNL_ORDR_DT_KEY, BSNS_UNIT_CD, LOCATION_SHORT_NAME, LOCATION_LONG_NAME, LOCATION_TYPE_CODE, purchase_order_status_map.pchse_ordr_status, purchase_order_status_map.pchse_ordr_status_desc, SCHL_DLVRY_DT ' \
                u'ORDER BY ' \
                u'purchase_order_number, order_date, location_id, LOCATION_SHORT_NAME, LOCATION_LONG_NAME, LOCATION_TYPE_CODE, pchse_ordr_status, pchse_ordr_status_desc, scheduled_delivery_date, scheduled_delivery_time ASC'

PO_SELECT_QUERY_DL="""
SELECT
  detail.purchase_Order_Number PURCHASEORDERNUMBER,
  FORMAT_DATE("%d/%m/%Y",DATE(detail.order_date)) AS ORDERDATE,
  detail.location_Id AS LOCATIONID,
  detail.location_Long_Name AS LOCATIONLONGNAME,
  FORMAT_DATE("%d/%m/%Y",DATE(detail.scheduled_Delivery_Date)) AS SCHEDULEDDELIVERYDATE,
  detail.scheduled_Delivery_Time AS SCHEDULEDDELIVERYTIME,
  purchase_Order_Status AS PURCHASEORDERSTATUS,
  lines_ordered AS TOTAL_LINES_ORDER,
  total_number_of_cases_ordered AS TOTAL_CASES_ORDER,
  total_number_of_cases_delivered AS TOTAL_CASES_DELIVERED,
  CASE
    WHEN total_number_of_cases_delivered IS NULL OR total_number_of_cases_delivered = 0 THEN "0 %"
  ELSE
  CONCAT(CAST(ROUND(((total_number_of_cases_delivered) / total_number_of_cases_ordered)* 100, 2) AS STRING)," %")
END
  AS TOTAL_PERCENTAGE_DELIVERED,
  sku AS SKU,
  tuc AS TUC,
  item_Name ITEMNAME,
  order_Quantity ORDERQUANTITY,
  received_Quantity AS RECEIVEDQUANTITY,
  percentage_Delivered AS PERCENTAGEDELIVERED
FROM
  detail,
  summary
WHERE
  detail.purchase_Order_Number=summary.purchase_Order_Number
  AND detail.order_date=summary.order_date
GROUP BY
  detail.purchase_Order_Number,
  detail.order_date,
  detail.location_Id,
  detail.location_Long_Name,
  summary.lines_ordered,
  summary.total_number_of_cases_ordered,
  sku,
  detail.scheduled_Delivery_Date,
  total_number_of_cases_delivered,
  detail.scheduled_Delivery_Time,
  purchase_Order_Status,
  percentage_Delivered,
  tuc,
  item_name,
  order_Quantity,
  received_Quantity
ORDER BY
  OrderDate,
  detail.purchase_Order_Number
"""

PURCHASE_ORDER_QUERY_DL = u'SELECT distinct ' \
                u'pchse_ordr_nbr purchase_Order_Number, ' \
                u'SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 1, 4) || ' \
                       u'"-"||SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 5, 2) || ' \
                       u'"-"||SUBSTR(CAST(orgnl_ordr_dt_key AS STRING), 7, 2) ' \
                u'order_date, ' \
                u'bsns_unit_cd location_Id, ' \
                u'location_long_name location_Long_Name, ' \
                u'sku_item_nbr sku, ' \
                u't.tuc tuc, ' \
                u'item_description item_name, ' \
                u'(CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END) order_Quantity, ' \
                u'CASE ' \
                    u'WHEN QTY_RECEIVED_REPORTED IS NULL THEN 0 ' \
                    u'ELSE QTY_RECEIVED_REPORTED ' \
                u'END AS received_Quantity, ' \
                u'CASE ' \
                    u'WHEN QTY_RECEIVED_REPORTED IS NULL OR QTY_RECEIVED_REPORTED = 0 THEN "0 %" ' \
                    u'ELSE CONCAT(CAST(ROUND(((QTY_RECEIVED_REPORTED) / (CASE WHEN CNCL_DT_KEY >= SCHL_DLVRY_DT_KEY THEN (IFNULL(ORDR_QTY,0) + IFNULL(CNCL_QTY,0)) ELSE IFNULL(ORDR_QTY,0) END))* 100, 2)  AS STRING)," %") ' \
                u'END AS percentage_Delivered, ' \
                u'purchase_order_status_map.pchse_ordr_status_desc purchase_Order_Status, ' \
                u'CAST(DATE(schl_dlvry_dt) AS STRING) scheduled_Delivery_Date, ' \
                u'CAST(TIME(schl_dlvry_dt) AS STRING) scheduled_Delivery_Time ' \
             u'FROM ' \
                u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_ITEM_STATE_GCP + '` '+ PURCHASE_ORDER +', ' \
                u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.supply_chain_tables.Item_Supplier_TUC` t, ' \
                u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_LOCATION_CURRENT + '` '+ LOCATION +', ' \
              u'`' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` '+ PRODUCT +', ' \
                 u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_REF_VNDR_ITEM + '` '+ VNDR_ITEM +', ' \
                   u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_PO_STATUS_MAP + '` purchase_order_status_map ' \
             u'WHERE ' \
                u'DATE(purchaseOrder.trx_dt)  >= "' + constant.START_DATE + '" ' \
                u'AND DATE(purchaseOrder.trx_dt) <=  "' + constant.END_DATE + '" ' \
                u'AND CAST(purchaseOrder.bsns_unit_cd AS INT64) = CAST(location.location_id AS INT64) ' \
                u'AND CAST(purchaseOrder.sku_item_nbr AS INT64) = CAST(product.sku_item_number AS INT64) ' \
                u'AND CAST(t.ITEM AS INT64) = CAST(purchaseOrder.sku_item_nbr AS INT64) ' \
                u'AND t.ebs_supplier_number=CAST("' + constant.VENDOR_NUMBER + '"  AS INT64) ' \
                u'AND dwr_vndr_item.vndr_item_nbr = product.sku_item_number ' \
                u'AND DATE(purchaseOrder.trx_dt) = dwr_vndr_item.ACTIVE_DATE ' \
                u'AND UPPER(purchase_order_status_map.pchse_ordr_status) = UPPER(purchaseOrder.pchse_ordr_status) ' \
                u'AND ORDR_QTY_REPORTED > 0 ' \
                u'AND purchaseOrder.pchse_ordr_status IN (' + FILTER_PURCHASE_ORDER_STATUSES + ') ' \
                u'AND UPPER(location.location_id) IN (' + FILTER_LOCATIONIDS + ') ' \
                u'AND UPPER(IFNULL(purchaseOrder.sku_item_nbr,"NULLVALUE")) IN (' + FILTER_SKUS + ') ' \
                u'AND  UPPER(location.status_code) != "I" ' \
                u'AND dwr_vndr_item.vndr_nbr = "' + constant.VENDOR_NUMBER + '" ' \
                u'AND  purchaseOrder.VNDR_NBR = "' + constant.VENDOR_NUMBER + '" ' \
            u'ORDER BY ' \
                u'order_date DESC,purchase_Order_Number ASC'

PURCHASE_ORDER_DOWNLOAD_ALL_QUERY="WITH DETAIL AS ("+PURCHASE_ORDER_QUERY_DL +"),SUMMARY AS ("+PURCHASE_ORDER_QUERY_SUMMARY_DL+")"+PO_SELECT_QUERY_DL

PURCHASE_ORDER_DOWNLOAD_PO_QUERY="WITH DETAIL AS ("+PURCHASE_ORDER_SEARCH_QUERY_DL +"),SUMMARY AS ("+PURCHASE_ORDER_QUERY_SUMMARY_DL+")"+PO_SELECT_QUERY_DL


PURCHASE_ORDER_DOWNLOAD_QUERY = PURCHASE_ORDER_QUERY_DL

PURCHASE_ORDER_SEARCH_QUERY_DOWNLOAD = PURCHASE_ORDER_SEARCH_QUERY_DL

PURCHASE_ORDER_STATUS_QUERY = u'SELECT ' \
                                    u'pchse_ordr_status, ' \
                                    u'pchse_ordr_status_desc ' \
                                u'FROM ' \
                                    u'`' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_PO_STATUS_MAP + '` ' \
                                u'WHERE ' \
                                    u'UPPER(pchse_ordr_status_desc) LIKE UPPER("' + constant.STARTS_WITH + '%") ' \
                                u'ORDER BY pchse_ordr_status_desc ASC'

PURCHASE_ORDER_QUERY_DL_SELECTED_FIELDS="""
SELECT "Please Note - The SKUs for which there is no relevant data do not feature in this report." PURCHASEORDERNUMBER,
"" AS ORDERDATE, 
"" AS LOCATIONID, "" AS LOCATIONLONGNAME,  
"" AS SCHEDULEDDELIVERYDATE, "" AS SCHEDULEDDELIVERYTIME, 
"" AS PURCHASEORDERSTATUS,
"" AS TOTAL_LINES_ORDER,"" AS TOTAL_CASES_ORDER,
"" AS TOTAL_CASES_DELIVERED,"" AS TOTAL_PERCENTAGE_DELIVERED,
"" AS SKU, "" AS TUC, "" ITEMNAME, 
"" ORDERQUANTITY,"" AS RECEIVEDQUANTITY, 
"" AS PERCENTAGEDELIVERED
UNION ALL
SELECT "Supplier: SELECTED_SUPPLIER_NAME" PURCHASEORDERNUMBER,
"" AS ORDERDATE, 
"" AS LOCATIONID, "" AS LOCATIONLONGNAME,  
"" AS SCHEDULEDDELIVERYDATE, "" AS SCHEDULEDDELIVERYTIME, 
"" AS PURCHASEORDERSTATUS,
"" AS TOTAL_LINES_ORDER,"" AS TOTAL_CASES_ORDER,
"" AS TOTAL_CASES_DELIVERED,"" AS TOTAL_PERCENTAGE_DELIVERED,
"" AS SKU, "" AS TUC, "" ITEMNAME, 
"" ORDERQUANTITY,"" AS RECEIVEDQUANTITY, 
"" AS PERCENTAGEDELIVERED
UNION ALL
SELECT "PURCHASE_ORDER_NUMBER" PURCHASEORDERNUMBER, 
"ORDER_DATE" AS ORDERDATE, 
"LOCATION_ID" AS LOCATIONID, "LOCATION_NAME" AS LOCATIONLONGNAME,  
"SCHEDULED_DELIVERY_DATE" AS SCHEDULEDDELIVERYDATE, "SCHEDULED_DELIVERY_TIME" AS SCHEDULEDDELIVERYTIME, 
"PURCHASE_ORDER_STATUS_DESC" AS PURCHASEORDERSTATUS,
"TOTAL_LINES_ORDER" AS TOTAL_LINES_ORDER,"TOTAL_CASES_ORDER" AS TOTAL_CASES_ORDER,
"TOTAL_CASES_DELIVERED" AS TOTAL_CASES_DELIVERED,"TOTAL_PERCENTAGE_DELIVERED" AS TOTAL_PERCENTAGE_DELIVERED,
"ITEM_NUMBER" AS SKU, "TUC" AS TUC, "ITEM_DESCRIPTION" ITEMNAME, 
"CASES_ORDER" ORDERQUANTITY,"CASES_DELIVERED" AS RECEIVEDQUANTITY, 
"PERCENTAGE_DELIVERED" AS PERCENTAGEDELIVERED
ORDER BY
  CASE
    WHEN PURCHASEORDERNUMBER LIKE "Please%" THEN 0
    WHEN PURCHASEORDERNUMBER LIKE "Supp%" THEN 1
  ELSE 2 END
"""



class PurchaseOrderServiceHandle:
    # This query must have some ORDER BY clause to work with limit and offset...

    @staticmethod
    def _get_po_status(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            
            startswith = request_dictionary[constant.STARTS_WITH]

            query = PURCHASE_ORDER_STATUS_QUERY.replace(constant.STARTS_WITH, startswith)
            

            final_result = BigQueryUtil.search_and_display(query, limit)
            return final_result
        except Exception as e:
            logging.error('Exception in _get_po_status - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _get_po_data(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]

            start_date = request_dictionary[constant.START_DATE]
            end_date = request_dictionary[constant.END_DATE]
            vendornumber = request_dictionary[constant.VENDOR_NUMBER]

            filter_locationids = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_LOCATION_ID)
            filter_purchase_order_statuses = CommonUtil.get_filter_value_as_string(request_dictionary,
                                                                                COLUMN_PCHSE_ORDR_STATUS)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM_NUMBER)
            
            query = PURCHASE_ORDER_QUERY.replace(constant.START_DATE, start_date)
            query = query.replace(constant.END_DATE, end_date)
            query = query.replace(constant.VENDOR_NUMBER, vendornumber)
            query = query.replace(FILTER_SKUS, filter_skus)
            
            if filter_purchase_order_statuses == ''+ BQ_CASE+ '' + COLUMN_PCHSE_ORDR_STATUS + ')':
                query = query.replace(FILTER_PURCHASE_ORDER_STATUSES,
                                    'UPPER(purchaseOrder.' + COLUMN_PCHSE_ORDR_STATUS + ')')
            elif filter_purchase_order_statuses == constant.UPPER_IFNULL + COLUMN_PCHSE_ORDR_STATUS + constant.NULL_VALUE:
                query = query.replace(FILTER_PURCHASE_ORDER_STATUSES, 'UPPER(IFNULL(purchaseOrder.' + COLUMN_PCHSE_ORDR_STATUS + constant.NULL_VALUE)
            else:
                query = query.replace(FILTER_PURCHASE_ORDER_STATUSES, filter_purchase_order_statuses)

            # This fix has been given to remove the ambiguous column error...
            if filter_locationids == ''+ BQ_CASE+ '' + COLUMN_LOCATION_ID + ')':
                query = query.replace(FILTER_LOCATIONIDS, 'UPPER(location.' + COLUMN_LOCATION_ID + ')')
            elif filter_locationids == constant.UPPER_IFNULL + COLUMN_LOCATION_ID + constant.NULL_VALUE:
                query = query.replace(FILTER_LOCATIONIDS, 'UPPER(IFNULL(location.' + COLUMN_LOCATION_ID + constant.NULL_VALUE)
            else:
                query = query.replace(FILTER_LOCATIONIDS, filter_locationids)
            
            final_result = BigQueryUtil.search_and_display(query, limit, page)
            
            return final_result
        except Exception as e:
            logging.error('Exception in _get_po_data - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _get_po_search_data(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]

            vendornumber = request_dictionary[constant.VENDOR_NUMBER]
            ponumber = request_dictionary[constant.PO_NUMBER]

            
            summary_query = PURCHASE_ORDER_SEARCH_QUERY_SUMMARY.replace(constant.VENDOR_NUMBER, vendornumber)
            summary_query = summary_query.replace(constant.PO_NUMBER, ponumber)
            
            query = PURCHASE_ORDER_SEARCH_QUERY.replace(constant.VENDOR_NUMBER, vendornumber)
            query = query.replace(constant.PO_NUMBER, ponumber)
            

            final_result_summary = BigQueryUtil.search_and_display(summary_query, limit)
            final_result_details = BigQueryUtil.search_and_display(query, limit, page)


            return PurchaseOrderServiceHandle._format_po_search_data(final_result_details, final_result_summary)
        except Exception as e:
            logging.error('Exception in _get_po_search_data - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _download_po_search_data(request_dictionary):


        try:
            vendornumber = request_dictionary[constant.VENDOR_NUMBER]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]
            ponumber = request_dictionary[constant.PO_NUMBER]
            username = request_dictionary[constant.USERNAME]

            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM_NUMBER)
            query = PURCHASE_ORDER_DOWNLOAD_PO_QUERY.replace(constant.VENDOR_NUMBER, vendornumber)
            query = query.replace(constant.PO_NUMBER, ponumber)
            query = query.replace(FILTER_SKUS, filter_skus)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('po_' + ponumber.strip() + '_' , username)
            query_columns =PURCHASE_ORDER_QUERY_DL_SELECTED_FIELDS.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)

            destination_blob=file_and_table_name
            query = query.replace(constant.TABLE, file_and_table_name)


            final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendornumber, 'csv')
            time.sleep(1)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('po_' + ponumber.strip() + '_' , username)
            final_result = BigQueryUtil.create_download_table(query,destination_blob, file_and_table_name, vendornumber, 'csv')
            return final_result
        except Exception as e:
            logging.error('Exception in _download_po_search_data - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _format_po_search_data(in_final_result, final_result_summary) -> object:
        final_result = in_final_result

        # Get the data from the output dictionary...
        records = final_result['data']
        summary_records = {}
        if len(final_result_summary['data']) > 0:
            summary_records = final_result_summary['data'][0]

        final_records = {'summary': summary_records, 'detail': records}
        final_result['data'] = final_records

        return final_result
        
    @staticmethod
    def _download_po_data(request_dictionary):


        try:
            start_date = request_dictionary[constant.START_DATE]
            end_date = request_dictionary[constant.END_DATE]
            vendornumber = request_dictionary[constant.VENDOR_NUMBER]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]
            username = request_dictionary[constant.USERNAME]

            filter_locationids = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_LOCATION_ID)
            filter_purchase_order_statuses = CommonUtil.get_filter_value_as_string(request_dictionary,
                                                                                COLUMN_PCHSE_ORDR_STATUS)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM_NUMBER)

            query = PURCHASE_ORDER_DOWNLOAD_ALL_QUERY.replace(constant.START_DATE, start_date)
            query = query.replace(constant.END_DATE, end_date)
            query = query.replace(constant.VENDOR_NUMBER, vendornumber)
            query = query.replace(FILTER_SKUS, filter_skus)

            if filter_purchase_order_statuses == ''+ BQ_CASE+ '' + COLUMN_PCHSE_ORDR_STATUS + ')':
                query = query.replace(FILTER_PURCHASE_ORDER_STATUSES,
                                    'UPPER(purchaseOrder.' + COLUMN_PCHSE_ORDR_STATUS + ')')
            elif filter_purchase_order_statuses == constant.UPPER_IFNULL + COLUMN_PCHSE_ORDR_STATUS + constant.NULL_VALUE:
                query = query.replace(FILTER_PURCHASE_ORDER_STATUSES, 'UPPER(IFNULL(purchaseOrder.' + COLUMN_PCHSE_ORDR_STATUS + constant.NULL_VALUE)
            else:
                query = query.replace(FILTER_PURCHASE_ORDER_STATUSES, filter_purchase_order_statuses)

            # This fix has been given to remove the ambiguous column error...
            if filter_locationids == ''+ BQ_CASE+ '' + COLUMN_LOCATION_ID + ')':
                query = query.replace(FILTER_LOCATIONIDS, 'UPPER(location.' + COLUMN_LOCATION_ID + ')')
            elif filter_locationids == constant.UPPER_IFNULL + COLUMN_LOCATION_ID + constant.NULL_VALUE:
                query = query.replace(FILTER_LOCATIONIDS, 'UPPER(IFNULL(location.' + COLUMN_LOCATION_ID + constant.NULL_VALUE)
            else:
                query = query.replace(FILTER_LOCATIONIDS, filter_locationids)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('po_', username)
            query = query.replace(constant.TABLE, file_and_table_name)
            query_columns =PURCHASE_ORDER_QUERY_DL_SELECTED_FIELDS.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)


            destination_blob=file_and_table_name
            final_result = BigQueryUtil.create_download_table(query_columns,destination_blob, file_and_table_name, vendornumber, 'csv')
            time.sleep(1)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('po_', username)

            final_result = BigQueryUtil.create_download_table(query,destination_blob, file_and_table_name, vendornumber, 'csv')
            return final_result
        except Exception as e:
            logging.error('Exception in _download_po_data - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def get_handle(request_dictionary):
        path = request_dictionary[constant.PATH]

        if request_dictionary[constant.VENDOR_NUMBER] == constant.DEFAULT_VENDOR_NUMBER:
            return {constant.MESSAGE: constant.ERROR_NO_VENDOR_SPECIFIED}
        elif path == '/po/search':
            final_result = PurchaseOrderServiceHandle._get_po_data(request_dictionary)
            return final_result
        elif path == '/searchbypono':
            if request_dictionary[constant.PO_NUMBER] == constant.DEFAULT_PO_NUMBER:
                return {constant.MESSAGE: constant.ERROR_NO_PO_SPECIFIED}

            final_result = PurchaseOrderServiceHandle._get_po_search_data(request_dictionary)
            return final_result
        elif path == '/searchbypono/download':
            if request_dictionary[constant.PO_NUMBER] == constant.DEFAULT_PO_NUMBER:
                return {constant.MESSAGE: constant.ERROR_NO_PO_SPECIFIED}

            if request_dictionary[constant.USERNAME] == constant.DEFAULT_USER_NAME:
                return {constant.MESSAGE: constant.ERROR_NO_USERID_SPECIFIED}

            final_result = PurchaseOrderServiceHandle._download_po_search_data(request_dictionary)
            return final_result
        elif path == '/po/download':
            if request_dictionary[constant.USERNAME] == constant.DEFAULT_USER_NAME:
                return {constant.MESSAGE: constant.ERROR_NO_USERID_SPECIFIED}

            final_result = PurchaseOrderServiceHandle._download_po_data(request_dictionary)
            return final_result
        elif path == '/orderstatus':
            final_result = PurchaseOrderServiceHandle._get_po_status(request_dictionary)
            return final_result
        else:
            return {constant.MESSAGE: constant.ERROR_INVALID_PATH}
