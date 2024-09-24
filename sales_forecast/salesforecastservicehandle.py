from bigqueryutil import BigQueryUtil
from commonutil import CommonUtil
import constant as constant
import time
import datetime
import google.cloud.logging
import logging

client = google.cloud.logging.Client()
client.setup_logging()

FILTER_CATEGORIES = 'filter_categories'
FILTER_CLASSES = 'filter_classes'
FILTER_BRANDS = 'filter_brands'
FILTER_SKUS = 'filter_skus'
Filter = 'filter'

COLUMN_COMMERCIAL_CATEGORY = 'commercial_category'
COLUMN_COMMERCIAL_CLASS = 'commercial_class'
COLUMN_BRAND_NAME = 'brand_name'
COLUMN_SKU_ITEM_NUMBER = 'sku_item_number'

VENDOR_QUERY = 'WITH vendor_min AS ( SELECT MIN_SKU_ITEM_NBR, ACT_DLVRY_DT sc_vendor, VNDR_NBR FROM `' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) +'' + constant.TABLE_SDH_VNDR_ITEM + '` WHERE vndr_nbr = "'+ constant.VENDOR_NUMBER + '" AND DATE(ACT_DLVRY_DT) >= CURRENT_DATE()-29 AND DATE(ACT_DLVRY_DT) <= CURRENT_DATE()-2 ), vendorminlessthan28 AS ( SELECT MIN_SKU_ITEM_NBR, ACT_DLVRY_DT, VNDR_NBR FROM `' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) +'' + constant.TABLE_SDH_VNDR_ITEM + '` WHERE VNDR_NBR = "'+ constant.VENDOR_NUMBER + '" AND MIN_SKU_ITEM_NBR NOT IN ( SELECT DISTINCT MIN_SKU_ITEM_NBR FROM vendor_min)), othervndersameminlessthan28 AS ( SELECT a.MIN_SKU_ITEM_NBR, a.ACT_DLVRY_DT, a.VNDR_NBR FROM `' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) +'' + constant.TABLE_SDH_VNDR_ITEM + '` as a WHERE a.vndr_nbr <> "'+ constant.VENDOR_NUMBER + '"  AND a.MIN_SKU_ITEM_NBR IN (SELECT MIN_SKU_ITEM_NBR FROM vendorminlessthan28) ), othervendorminexclude AS( SELECT vendorminlessthan28.MIN_SKU_ITEM_NBR, vendorminlessthan28.VNDR_NBR FROM othervndersameminlessthan28, vendorminlessthan28 WHERE vendorminlessthan28.MIN_SKU_ITEM_NBR=othervndersameminlessthan28.MIN_SKU_ITEM_NBR AND DATE(vendorminlessthan28.ACT_DLVRY_DT)< DATE(othervndersameminlessthan28.ACT_DLVRY_DT)) select * from ( SELECT MIN_SKU_ITEM_NBR as VNDR_ITEM_NBR, VNDR_NBR FROM vendor_min UNION DISTINCT ( SELECT vendorminlessthan28.MIN_SKU_ITEM_NBR as VNDR_ITEM_NBR, vendorminlessthan28.VNDR_NBR FROM vendorminlessthan28 WHERE vendorminlessthan28.MIN_SKU_ITEM_NBR NOT IN ( SELECT othervendorminexclude.MIN_SKU_ITEM_NBR FROM othervendorminexclude ) )) union distinct SELECT DISTINCT VNDR_ITEM_NBR, VNDR_NBR FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_SOURCE_VNDR_ITEM + '` WHERE VNDR_NBR= "'+ constant.VENDOR_NUMBER + '" AND (DATE(EFF_FROM_DT)<= CURRENT_DATE()-2 AND DATE(EFF_TO_DT)>= CURRENT_DATE()-1) AND XWMM_PRIMARY_VNDR_IND="Y" '

RFORECAST_QUERY = 'SELECT MAX(Sales.R_FORECASTDATE)  as rforecast_date FROM Sales'

SALES_QUERY = 'select * from  `' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '.sdhdatamart.sales_forecast_staging`,vendor where VNDR_ITEM_NBR = R_SKU'

CURRENT_BASE_QUERY = 'SELECT sls.R_SKU AS sku, prd.item_description AS skuName,ROUND(SUM(sls.R_FORECASTVOLUME), 2) AS currentForecast FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` AS prd, RFORECAST_DATE, Sales AS sls WHERE sls.R_DATE BETWEEN (DATE(RFORECAST_DATE.rforecast_date)) AND DATE_ADD(DATE(RFORECAST_DATE.rforecast_date), INTERVAL 20 DAY) AND sls.R_FORECASTDATE = DATE(RFORECAST_DATE.rforecast_date) AND prd.sku_item_number = sls.R_SKU AND UPPER(prd.commercial_category) IN ('+FILTER_CATEGORIES +')  AND UPPER(prd.commercial_class) IN ('+ FILTER_CLASSES +') AND UPPER(prd.brand_name) IN ( '+ FILTER_BRANDS +')   AND UPPER(prd.sku_item_number) IN (' + FILTER_SKUS + ')  GROUP BY sku, skuName'

COMPARABLE_LATEST_BASE_QUERY = 'SELECT sls.R_SKU AS sku, prd.item_description AS skuName, ROUND(SUM(sls.R_FORECASTVOLUME), 2) AS currentForecast FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` AS prd, Sales AS sls, RFORECAST_DATE WHERE sls.R_DATE BETWEEN (DATE(RFORECAST_DATE.rforecast_date)) AND DATE_ADD(DATE(RFORECAST_DATE.rforecast_date), INTERVAL 19 DAY) AND sls.R_FORECASTDATE = (RFORECAST_DATE.rforecast_date) AND prd.sku_item_number = sls.R_SKU AND UPPER(prd.commercial_category) IN ('+FILTER_CATEGORIES +')   AND  UPPER(prd.commercial_class) IN ('+ FILTER_CLASSES +') AND UPPER(prd.brand_name) IN ('+ FILTER_BRANDS +')  AND UPPER(prd.sku_item_number) IN (' + FILTER_SKUS + ')   GROUP BY sku, skuName'

COMPARABLE_PREVIOUS_BASE_QUERY = 'SELECT sls.R_SKU AS sku, prd.item_description AS skuName,ROUND(SUM(sls.R_FORECASTVOLUME), 2) AS currentForecast FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` AS prd, Sales AS sls, RFORECAST_DATE WHERE sls.R_DATE BETWEEN (DATE(RFORECAST_DATE.rforecast_date)) AND DATE_ADD(DATE(RFORECAST_DATE.rforecast_date) - 1, INTERVAL 20 DAY) AND sls.R_FORECASTDATE = (RFORECAST_DATE.rforecast_date) - 1 AND prd.sku_item_number = sls.R_SKU AND UPPER(prd.commercial_category) IN ('+FILTER_CATEGORIES +') AND UPPER(prd.commercial_class) IN ('+ FILTER_CLASSES +') AND UPPER(prd.brand_name) IN ('+ FILTER_BRANDS +') AND UPPER(prd.sku_item_number) IN (' + FILTER_SKUS + ') GROUP BY sku, skuName'

SKU_FORECAST_QUERY = 'WITH  vendor AS ('+VENDOR_QUERY+'),   Sales AS ('+ SALES_QUERY +'),   RFORECAST_DATE AS ('+RFORECAST_QUERY+'),   Latest21Days AS('+ CURRENT_BASE_QUERY +'),   Latest20Comparable AS('+ COMPARABLE_LATEST_BASE_QUERY +'),   Previous20Comparable AS('+ COMPARABLE_PREVIOUS_BASE_QUERY +'),   FINAL AS(   SELECT     coalesce(la.sku,lc.sku, pc.sku) AS sku,     coalesce(la.skuName,lc.skuName, pc.skuName) AS skuName,     IFNULL(CAST(la.currentForecast AS INT64),0) AS currentForecast,     IFNULL(CAST(lc.currentForecast AS INT64),0) AS comparableLatestForecast,     IFNULL(CAST(pc.currentForecast AS INT64),0) AS comparablePreviousForecast,     IFNULL(CAST(lc.currentForecast AS INT64),0) - IFNULL(CAST(pc.currentForecast AS INT64),0) AS forecastVariance   FROM     Latest21Days AS la   full JOIN     Latest20Comparable AS lc   ON     la.sku = lc.sku   full JOIN     Previous20Comparable AS pc   ON     lc.sku = pc.sku) SELECT   * FROM   FINAL ORDER BY   '+ constant.SORT_DETAIL +''

SUMMARY_21_FORECAST_QUERY = 'SELECT   IFNULL(SUM(currentForecast),0) AS currentForecastlatestValue,   IFNULL(SUM(previousForecast),0) AS previousForecastlatestValue,   IFNULL(SUM(comparableLatestForecast),0) AS comparablePeriodForecastlatestValue,   IFNULL(SUM(comparablePreviousForecast),0) AS comparablePeriodForecastpreviousValue,   IFNULL(SUM(comparableLatestForecast) - SUM(comparablePreviousForecast),0) AS comparableChangeValue,   IFNULL(SUM(lavg_),0) AS dailyAverageForecastlatestValue,   IFNULL(SUM(pavg_),0) AS dailyAverageForecastpreviousValue,   IFNULL(SUM(lavg_) - SUM(pavg_),0) AS dailyAverageChange,   IFNULL(SUM(comparableLatestForecast) - SUM(comparablePreviousForecast),0) AS absoluteChangeValue,   IFNULL((CASE       WHEN (SUM(comparableLatestForecast) - SUM(comparablePreviousForecast)) = 0 THEN 0     ELSE     ROUND(((SUM(comparableLatestForecast) - SUM(comparablePreviousForecast))/(case when SUM(comparablePreviousForecast) = 0 then 1 else SUM(comparablePreviousForecast) end) * 100), 2)   END     ),0) AS absoluteChangePercentage FROM (   WITH     vendor AS ('+VENDOR_QUERY+'),     Sales AS ('+ SALES_QUERY +'),     RFORECAST_DATE AS ('+RFORECAST_QUERY+'),     Latest21Days AS(     SELECT       sls.R_SKU AS sku,       prd.item_description AS skuName, ROUND(SUM(sls.R_FORECASTVOLUME), 2) AS currentForecast     FROM       `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` AS prd,       RFORECAST_DATE,       Sales AS sls     WHERE       sls.R_DATE BETWEEN (DATE(RFORECAST_DATE.rforecast_date))       AND DATE_ADD(DATE(RFORECAST_DATE.rforecast_date), INTERVAL 20 DAY)       AND sls.R_FORECASTDATE = DATE(RFORECAST_DATE.rforecast_date)       AND prd.sku_item_number = sls.R_SKU       AND UPPER(prd.commercial_category) IN ('+FILTER_CATEGORIES +')    AND UPPER(prd.commercial_class)  IN ('+ FILTER_CLASSES +')     AND UPPER(prd.brand_name) IN ( '+ FILTER_BRANDS +')      AND UPPER(prd.sku_item_number) IN (' + FILTER_SKUS + ')     GROUP BY       sku,       skuName     ORDER BY       sku),     Previous21Days AS(     SELECT       sls.R_SKU AS sku,       prd.item_description AS skuName,   ROUND(SUM(sls.R_FORECASTVOLUME), 2) AS currentForecast     FROM       `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` AS prd,       RFORECAST_DATE,       Sales AS sls     WHERE       sls.R_DATE BETWEEN (DATE(RFORECAST_DATE.rforecast_date)) - 1       AND DATE_ADD(DATE(RFORECAST_DATE.rforecast_date) - 1, INTERVAL 20 DAY)       AND sls.R_FORECASTDATE = DATE(RFORECAST_DATE.rforecast_date) - 1       AND prd.sku_item_number = sls.R_SKU       AND UPPER(prd.commercial_category) IN ('+FILTER_CATEGORIES +')       AND UPPER(prd.commercial_class) IN ('+ FILTER_CLASSES +')    AND UPPER(prd.brand_name)  IN ( '+ FILTER_BRANDS +')  AND  UPPER(prd.sku_item_number) IN (' + FILTER_SKUS + ')     GROUP BY       sku,       skuName     ORDER BY       sku),     Latest20Comparable AS(     SELECT       sls.R_SKU AS sku,       prd.item_description AS skuName,   ROUND(SUM(sls.R_FORECASTVOLUME), 2) AS currentForecast,       COUNT(DISTINCT(sls.R_date)) AS R_date     FROM       `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` AS prd,       Sales AS sls,       RFORECAST_DATE     WHERE       sls.R_DATE BETWEEN DATE(RFORECAST_DATE.rforecast_date)       AND DATE_ADD(DATE(RFORECAST_DATE.rforecast_date), INTERVAL 19 DAY)       AND sls.R_FORECASTDATE = DATE(RFORECAST_DATE.rforecast_date)       AND prd.sku_item_number = sls.R_SKU       AND UPPER(prd.commercial_category) IN ('+FILTER_CATEGORIES +')      AND UPPER(prd.commercial_class) IN ('+ FILTER_CLASSES +')       AND UPPER(prd.brand_name) IN ( '+ FILTER_BRANDS +') AND  UPPER(prd.sku_item_number) IN (' + FILTER_SKUS + ')     GROUP BY       sku,       skuName     ORDER BY       sku),     Previous20Comparable AS(     SELECT       sls.R_SKU AS sku,       prd.item_description AS skuName,     ROUND(SUM(sls.R_FORECASTVOLUME), 2) AS currentForecast,       COUNT(DISTINCT(sls.R_date)) AS R_date     FROM       `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` AS prd,       Sales AS sls,       RFORECAST_DATE     WHERE       sls.R_DATE BETWEEN (DATE(RFORECAST_DATE.rforecast_date))       AND DATE_ADD(DATE(RFORECAST_DATE.rforecast_date) - 1, INTERVAL 20 DAY)       AND sls.R_FORECASTDATE = DATE(RFORECAST_DATE.rforecast_date) - 1       AND prd.sku_item_number = sls.R_SKU       AND UPPER(prd.commercial_category) IN ('+FILTER_CATEGORIES +')    AND UPPER(prd.commercial_class) IN ('+ FILTER_CLASSES +')      AND UPPER(prd.brand_name) IN ( '+ FILTER_BRANDS +')       AND UPPER(prd.sku_item_number) IN (' + FILTER_SKUS + ')     GROUP BY       sku,       skuName     ORDER BY       sku),     FINAL AS(     SELECT       coalesce(la.sku,lc.sku,pc.sku,pa.sku) AS sku,       coalesce(la.skuName,lc.skuName,pc.skuName,pa.skuName) AS skuName,       CAST((IFNULL(lc.currentForecast,0)/lc.R_date) AS INT64) AS lavg_,       CAST((IFNULL(pc.currentForecast,0)/pc.R_date) AS INT64) AS pavg_,       IFNULL(CAST(la.currentForecast AS INT64),0) AS currentForecast,       IFNULL(CAST(pa.currentForecast AS INT64),0) AS previousForecast,       IFNULL(CAST(lc.currentForecast AS INT64),0) AS comparableLatestForecast,       IFNULL(CAST(pc.currentForecast AS INT64),0) AS comparablePreviousForecast     FROM       Latest21Days AS la     full JOIN       Latest20Comparable AS lc     ON       la.sku = lc.sku     full JOIN       Previous20Comparable AS pc     ON       lc.sku = pc.sku     full JOIN       Previous21Days AS pa     ON       pc.sku = pa.sku)   SELECT     *   FROM     FINAL)'

GRAPH_21_FORECAST = 'WITH vendor AS ('+VENDOR_QUERY+'),   Sales AS ('+ SALES_QUERY +'),   RFORECAST_DATE AS ('+RFORECAST_QUERY+'),   Latest21Days AS(   SELECT     DATE(sls.R_date) AS r_date,     ROUND(SUM(sls.R_FORECASTVOLUME), 2) AS currentForecast   FROM     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` AS prd,     Sales AS sls,     RFORECAST_DATE   WHERE     sls.R_DATE BETWEEN DATE(RFORECAST_DATE.rforecast_date)     AND DATE_ADD(DATE(RFORECAST_DATE.rforecast_date), INTERVAL 20 DAY)     AND sls.R_FORECASTDATE = DATE(RFORECAST_DATE.rforecast_date)     AND prd.sku_item_number = sls.R_SKU     AND UPPER(prd.commercial_category) IN ('+FILTER_CATEGORIES +')  AND  UPPER(prd.commercial_class) IN ('+ FILTER_CLASSES +')    AND UPPER(prd.brand_name) IN ( '+ FILTER_BRANDS +')     AND UPPER(prd.sku_item_number) IN (' + FILTER_SKUS + ')   GROUP BY     r_date),   Previous21Days AS(   SELECT     sls.R_date AS r_date,     ROUND(SUM(sls.R_FORECASTVOLUME), 2) AS currentForecast   FROM     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` AS prd,     Sales AS sls,     RFORECAST_DATE   WHERE     sls.R_DATE BETWEEN DATE(RFORECAST_DATE.rforecast_date)     AND DATE_ADD(DATE(RFORECAST_DATE.rforecast_date), INTERVAL 20 DAY)     AND sls.R_FORECASTDATE = DATE(RFORECAST_DATE.rforecast_date) - 1     AND prd.sku_item_number = sls.R_SKU     AND UPPER(prd.commercial_category) IN ('+FILTER_CATEGORIES +')   AND UPPER(prd.commercial_class) IN ('+ FILTER_CLASSES +')   AND UPPER(prd.brand_name) IN ( '+ FILTER_BRANDS +')    AND UPPER(prd.sku_item_number) IN (' + FILTER_SKUS + ')   GROUP BY     r_date), final_calc AS (     SELECT      CAST(if(la.currentForecast<1,la.currentForecast,CAST(la.currentForecast AS int64)) AS NUMERIC) AS latestForecast,     CAST(if(pa.currentForecast<1,pa.currentForecast,CAST( pa.currentForecast AS int64)) AS NUMERIC) AS previousForecast,       IFNULL(DATE(la.r_date),         DATE(pa.r_date)) AS date     FROM       Latest21Days AS la     FULL OUTER JOIN       Previous21Days AS pa     ON       ifnull(la.r_date,         "2500-12-01" )= ifnull(pa.r_date,         "2500-12-01"))   SELECT     IFNULL(latestForecast,       0) AS latestForecast,     IFNULL(previousForecast,       0) AS previousForecast,     ifnull(date,       DATE(clndr_dt)) AS date   FROM     final_calc   FULL OUTER JOIN     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.source_data_partitioned.DWV_TIME_DAY` dtd   ON     final_calc.date = DATE(dtd.clndr_dt)   WHERE     DATE(dtd.clndr_dt) >=(     SELECT       DATE(RFORECAST_DATE.rforecast_date)     FROM       RFORECAST_DATE)     AND DATE(dtd.clndr_dt) <=(     SELECT       DATE_ADD(DATE(RFORECAST_DATE.rforecast_date), INTERVAL 20 DAY)     FROM       RFORECAST_DATE) ORDER BY   date ASC'

LONG_RANGED_SALES_QUERY = 'WITH   vendor AS ('+VENDOR_QUERY+'), pack_size as ( SELECT SKU_ITEM_NBR, IF((pack_size IS NULL OR pack_size = 0),1,pack_size) pack_size FROM (SELECT mbr_sku_item_nbr SKU_ITEM_NBR,perassembly_cnt pack_size,ROW_NUMBER() OVER (PARTITION BY mbr_sku_item_nbr ORDER BY sku_item_collctn_key DESC) rn FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.source_data_partitioned.DWR_SKU_ITEM_COLLCTN` WHERE curr_ind = "Y" ) WHERE rn = 1 ),   prd AS (   SELECT     item_description AS skuName,     sku_item_number AS sku   FROM     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '`,     vendor   WHERE     vendor.VNDR_ITEM_NBR = sku_item_number     AND UPPER(commercial_category) IN ('+FILTER_CATEGORIES +')     AND UPPER(commercial_class) IN ('+ FILTER_CLASSES +')     AND UPPER(brand_name) IN ( '+ FILTER_BRANDS +')     AND UPPER(sku_item_number) IN (' + FILTER_SKUS + ')),   table_1 AS (   SELECT     DATE(sls.week_start) AS date_,     ROUND(SUM(value/cast(stc.pack_size as int64)),2)  AS Value,     sls.Item AS sku   FROM     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.supply_chain.item_store_week_mat_view` AS sls, pack_size AS stc,     vendor   WHERE     DATE(sls.week_start) BETWEEN DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) + 7     AND DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) + 189     AND sls.Item = VNDR_ITEM_NBR AND stc.SKU_ITEM_NBR=sls.Item   GROUP BY     date_,     sku),   cal AS (   SELECT     *   FROM     UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) + 7,DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) + 189,INTERVAL 1 week)) AS week_date ),   cal_prd AS (   SELECT     DISTINCT prd.sku,     prd.skuName,     cal.week_date   FROM     cal,     prd,     table_1   WHERE     table_1.sku = prd.sku) SELECT   * FROM (   SELECT     DISTINCT cal_prd.week_date,     CAST(IFNULL(table_1.Value,       0) as INT64) Value,     cal_prd.sku,     cal_prd.skuName   FROM     table_1   RIGHT JOIN     cal_prd   ON     (cal_prd.week_date = table_1.date_       AND cal_prd.sku = table_1.sku)) AS main PIVOT(SUM(Value) FOR week_date IN '+constant.DATES+') ORDER BY   '+ constant.SORT_DETAIL +''

SUMMARY_LONG_RANGED_QUERY = 'SELECT   SUM(averageValue) AS weeklyAverage FROM (   WITH     vendor AS ('+VENDOR_QUERY+') ,pack_size as ( SELECT SKU_ITEM_NBR, IF((pack_size IS NULL OR pack_size = 0),1,pack_size) pack_size FROM (SELECT mbr_sku_item_nbr SKU_ITEM_NBR,perassembly_cnt pack_size,ROW_NUMBER() OVER (PARTITION BY mbr_sku_item_nbr ORDER BY sku_item_collctn_key DESC) rn FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.source_data_partitioned.DWR_SKU_ITEM_COLLCTN` WHERE curr_ind = "Y" ) WHERE rn = 1 )  SELECT     CAST(SUM(dw.value/cast(stc.pack_size as int64)) / COUNT(DISTINCT(DATE(dw.week_start))) AS INT64) AS averageValue,     dw.Item AS sku,     prd.item_description AS skuName   FROM     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.supply_chain.item_store_week_mat_view` AS dw, pack_size AS stc,    `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` AS prd,     vendor   WHERE     DATE(dw.week_start) BETWEEN DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) + 7     AND DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) + 189     AND dw.Item = VNDR_ITEM_NBR     AND prd.sku_item_number = dw.Item  AND stc.SKU_ITEM_NBR=dw.Item    AND UPPER(prd.commercial_category) IN ('+FILTER_CATEGORIES +')     AND UPPER(prd.commercial_class) IN ('+ FILTER_CLASSES +')  AND UPPER(prd.brand_name) IN ( '+ FILTER_BRANDS +')   AND  UPPER(prd.sku_item_number) IN (' + FILTER_SKUS + ')   GROUP BY     sku,     skuName   ORDER BY     sku ASC)'

GRAPH_LONG_RANGED = 'WITH   vendor  AS ('+VENDOR_QUERY+'), pack_size as ( SELECT SKU_ITEM_NBR, IF((pack_size IS NULL OR pack_size = 0),1,pack_size) pack_size FROM (SELECT mbr_sku_item_nbr SKU_ITEM_NBR,perassembly_cnt pack_size,ROW_NUMBER() OVER (PARTITION BY mbr_sku_item_nbr ORDER BY sku_item_collctn_key DESC) rn FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.source_data_partitioned.DWR_SKU_ITEM_COLLCTN` WHERE curr_ind = "Y" ) WHERE rn = 1 ),  sales_week AS( SELECT   DATE(dw.week_start) AS week_date,   ROUND(SUM(dw.value/cast(stc.pack_size as int64)),2) AS latestForecast FROM   `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.supply_chain.item_store_week_mat_view` AS dw, pack_size AS stc,   `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` AS prd,   vendor WHERE   DATE(dw.week_start) BETWEEN DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) + 7   AND DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) + 189   AND dw.Item = VNDR_ITEM_NBR  AND stc.SKU_ITEM_NBR=dw.Item   AND prd.sku_item_number = dw.Item   AND UPPER(prd.commercial_category) IN ('+FILTER_CATEGORIES +') AND UPPER(prd.commercial_class)  IN ('+ FILTER_CLASSES +')   AND UPPER(prd.brand_name) IN ( '+ FILTER_BRANDS +')   AND   UPPER(prd.sku_item_number) IN (' + FILTER_SKUS + ') GROUP BY   week_date),   final_calc AS(   SELECT     *   FROM     UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) + 7,DATE_TRUNC(CURRENT_DATE(),WEEK(MONDAY)) + 189,INTERVAL 1 week)) AS all_week) SELECT   all_week AS week_date,   IFNULL(CAST(if(latestForecast<1,latestForecast,cast(latestForecast as int64)) as NUMERIC),     0) AS latestForecast FROM   sales_week RIGHT JOIN   final_calc ON   final_calc.all_week=sales_week.week_date   ORDER BY week_date ASC'

SALES_DATE_QUERY = 'SELECT   DATE(MAX(R_FORECASTDATE)) - 1 AS date_ FROM (   WITH   vendor AS ('+VENDOR_QUERY+')   ('+SALES_QUERY+'))'

DOWNLOAD_API_21DAYS = """WITH   vendor AS ("""+VENDOR_QUERY+"""),   Sales AS("""+ SALES_QUERY +"""),   RFORECAST_DATE AS ("""+RFORECAST_QUERY+"""),   Latest21Days AS(   SELECT     sls.R_SKU AS sku,     prd.item_description AS skuName,     prd.commercial_category AS lcommercial_category,     prd.commercial_class AS lcommercial_class,     prd.brand_name AS lbrandname,     ROUND(SUM(sls.R_FORECASTVOLUME), 2) AS currentForecast,     sls.R_DATE AS l_date,   FROM     `""" + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + """""" + constant.TABLE_PRODUCT_CURRENT + """` AS prd,     RFORECAST_DATE,     Sales AS sls   WHERE     sls.R_DATE BETWEEN (DATE(RFORECAST_DATE.rforecast_date))     AND DATE_ADD(DATE(RFORECAST_DATE.rforecast_date), INTERVAL 20 DAY)     AND sls.R_FORECASTDATE = DATE(RFORECAST_DATE.rforecast_date)     AND prd.sku_item_number = sls.R_SKU     AND UPPER(prd.commercial_category) IN ("""+FILTER_CATEGORIES +""")     AND UPPER(prd.commercial_class) IN ("""+ FILTER_CLASSES +""")     AND UPPER(prd.brand_name) IN ( """+ FILTER_BRANDS +""")     AND UPPER(prd.sku_item_number) IN (""" + FILTER_SKUS + """)   GROUP BY     sku,     skuName,     l_date,     lcommercial_category,     lcommercial_class,     lbrandname),   Previous21Days AS(   SELECT     sls.R_SKU AS sku,     prd.item_description AS skuName,     prd.commercial_category AS pcommercial_category,     prd.commercial_class AS pcommercial_class,     prd.brand_name AS pbrandname,     ROUND(SUM(sls.R_FORECASTVOLUME), 2) AS previousForecast,     sls.R_DATE AS p_date,   FROM     `""" + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + """""" + constant.TABLE_PRODUCT_CURRENT + """` AS prd,     RFORECAST_DATE,     Sales AS sls   WHERE     sls.R_DATE BETWEEN (DATE(RFORECAST_DATE.rforecast_date)) - 1     AND DATE_ADD(DATE(RFORECAST_DATE.rforecast_date) - 1, INTERVAL 20 DAY)     AND sls.R_FORECASTDATE = DATE(RFORECAST_DATE.rforecast_date) - 1     AND prd.sku_item_number = sls.R_SKU     AND UPPER(prd.commercial_category) IN ("""+FILTER_CATEGORIES +""")     AND UPPER(prd.commercial_class) IN ("""+ FILTER_CLASSES +""")     AND UPPER(prd.brand_name) IN ( """+ FILTER_BRANDS +""")     AND UPPER(prd.sku_item_number) IN (""" + FILTER_SKUS + """)   GROUP BY     sku,     skuName,     p_date,     pcommercial_category,     pcommercial_class,     pbrandname) SELECT   DISTINCT * FROM (   SELECT     UPPER(COALESCE(la.lcommercial_category,         pa.pcommercial_category)) AS CATEGORY,     UPPER(COALESCE(la.lcommercial_class,         pa.pcommercial_class)) AS SUBCATEGORY,     UPPER(COALESCE(la.lbrandname,         pa.pbrandname)) AS BRAND,     COALESCE(la.sku,       pa.sku) AS SKU,     UPPER(COALESCE(la.skuName,         pa.skuName)) AS SKU_DESCRIPTION,     IF(la.currentForecast<1,cast(la.currentForecast as string),FORMAT("%'d",IFNULL(CAST(la.currentForecast AS INT64),         0)))  AS currentForcast,     IF(pa.previousForecast<1,cast(pa.previousForecast as string),FORMAT("%'d",IFNULL(CAST(pa.previousForecast AS INT64),         0)))  AS previousForecast,     COALESCE(la.l_date,       pa.p_date) AS dates   FROM     Latest21Days AS la   FULL OUTER JOIN     Previous21Days AS pa   ON     la.sku = pa.sku     AND la.l_date = pa.p_date   ORDER BY     Sku ASC,     dates ASC) AS main PIVOT(STRING_AGG(currentForcast) AS LATEST_FORECAST,     STRING_AGG(CASE         WHEN dates = (select * from RFORECAST_DATE)+20 THEN NULL       ELSE       previousForecast     END       ) AS PREV_FORECAST FOR dates IN """+constant.DATES+""")"""

DOWNLOAD_API_27_WEEKS = """WITH   vendor AS ("""+VENDOR_QUERY+"""), pack_size as ( SELECT SKU_ITEM_NBR, IF((pack_size IS NULL OR pack_size = 0),1,pack_size) pack_size FROM (SELECT mbr_sku_item_nbr SKU_ITEM_NBR,perassembly_cnt pack_size,ROW_NUMBER() OVER (PARTITION BY mbr_sku_item_nbr ORDER BY sku_item_collctn_key DESC) rn FROM `""" + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + """.source_data_partitioned.DWR_SKU_ITEM_COLLCTN` WHERE curr_ind = "Y" ) WHERE rn = 1 ),  prd AS (   SELECT     item_description AS skuName,     sku_item_number AS sku,     UPPER(commercial_category) AS CATEGORY,     UPPER(commercial_class) AS SUBCATEGORY,     UPPER(brand_name) AS BRAND   FROM     `""" + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + """""" + constant.TABLE_PRODUCT_CURRENT + """`,   vendor   WHERE     vendor.VNDR_ITEM_NBR = sku_item_number     AND UPPER(commercial_category) IN ("""+FILTER_CATEGORIES +""")     AND UPPER(commercial_class) IN ("""+ FILTER_CLASSES +""")     AND UPPER(brand_name) IN ( """+ FILTER_BRANDS +""")     AND UPPER(sku_item_number) IN (""" + FILTER_SKUS + """)),   table_1 AS (   SELECT     DATE(sls.week_start) AS date_,     ROUND(SUM(value/cast(stc.pack_size as int64)),2) AS Value,     sls.Item AS sku   FROM     `""" + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + """.supply_chain.item_store_week_mat_view` AS sls, pack_size AS stc,    vendor   WHERE     DATE(sls.week_start) BETWEEN DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) + 7     AND DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) + 189     AND sls.Item = VNDR_ITEM_NBR  AND stc.SKU_ITEM_NBR=sls.Item  GROUP BY     date_,     sku),   cal AS (   SELECT     *   FROM     UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) + 7,DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) + 189,INTERVAL 1 week)) AS week_date ),   cal_prd AS (   SELECT     DISTINCT prd.sku,     prd.skuName,     prd.CATEGORY,     prd.SUBCATEGORY,     prd.BRAND,     cal.week_date   FROM     cal,     prd,     table_1   WHERE     table_1.sku = prd.sku) SELECT   * FROM (   SELECT     cal_prd.CATEGORY,     cal_prd.SUBCATEGORY,     cal_prd.BRAND,     cal_prd.sku AS SKU,     cal_prd.skuName AS SKU_DESCRIPTION, cal_prd.week_date,     IF(SUM(value)<1,cast(SUM(value) as string),FORMAT("%'d",IFNULL(CAST (SUM(value) AS INT64),         0)))  AS Value   FROM     table_1   RIGHT JOIN     cal_prd   ON     (cal_prd.week_date = table_1.date_       AND cal_prd.sku = table_1.sku)   GROUP BY     week_date,     SKU,     SKU_DESCRIPTION,     BRAND,     SUBCATEGORY,     CATEGORY   ORDER BY     sku ASC,     week_date ASC) AS main PIVOT(STRING_AGG(Value) AS WK_COMMENCING FOR week_date IN """+constant.DATES+""")"""

DOWNLOAD_QUERY_COL_LATEST = 'SELECT   "SALES_VOLUME_CASES" CATEGORY,   "" SUBCATEGORY,   "" BRAND,   "" sku,   "" skuName,   '+constant.COLUMN1+' UNION ALL SELECT   "Please Note - These are sales forecasts only and may change." CATEGORY,   "" SUBCATEGORY,   "" BRAND,   "" sku,   "" skuName,   '+constant.COLUMN1+' UNION ALL SELECT   "Please Note - The SKUs for which there is no relevant data do not feature in this report." CATEGORY,   "" SUBCATEGORY,   "" BRAND,   "" sku,   "" skuName,   '+constant.COLUMN1+' UNION ALL SELECT   "Supplier: SELECTED_SUPPLIER_NAME" CATEGORY,   "" SUBCATEGORY,   "" BRAND,   "" sku,   "" skuName,   '+constant.COLUMN1+' UNION ALL SELECT   "CATEGORY" AS CATEGORY,   "SUBCATEGORY" AS SUBCATEGORY,   "BRAND" AS BRAND,   "SKU" AS sku,   "SKU_DESCRIPTION" AS skuName,   '+constant.COLUMN2+''

LAST_REFRESH_DATE = 'SELECT   FORMAT_DATE("'+constant.REV_DATE_FORMAT+'",updatedDate) AS date_ FROM (   SELECT     table_id,     CAST(TIMESTAMP_MILLIS(last_modified_time) AS DATE) AS updatedDate   FROM     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.supply_chain.__TABLES__`   WHERE     table_id IN ("item_store_week_mat_view")) ORDER BY   updatedDate LIMIT   1'

class SalesForecastServiceHandle:
    @staticmethod
    def _get_sku_sales_forecast(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]
            vendornumber = request_dictionary[constant.VENDOR_NUMBER]

            sort_by = request_dictionary[constant.SORT_BY]
            sort_direction = request_dictionary[constant.SORT_DIRECTION]
            sort_detail = CommonUtil.get_sort_details(sort_by, sort_direction)

            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM_NUMBER)

            query = SKU_FORECAST_QUERY.replace(FILTER_CATEGORIES, filter_categories)
            query = query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.VENDOR_NUMBER, vendornumber)
            query = query.replace(constant.SORT_DETAIL, sort_detail)
            logging.info("----query-----")
            logging.info(query)
            final_result = BigQueryUtil.search_and_display(query, limit, page)
        
            return final_result
        except Exception as e:
            logging.error('Exception in _get_sku_sales_forecast - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}


    @staticmethod
    def _get_combined_sales_sku_forecast(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]

            vendornumber = request_dictionary[constant.VENDOR_NUMBER]
            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM_NUMBER)
            
            summary_query = SUMMARY_21_FORECAST_QUERY.replace(FILTER_CATEGORIES, filter_categories)
            query = summary_query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.VENDOR_NUMBER, vendornumber)
            logging.info("----query-----")
            logging.info(query)
            summary_result = BigQueryUtil.search_and_display(query, limit, page)
            if 'data' in summary_result and summary_result['data'] and len(summary_result['data']) > 0 :
                summary_result = summary_result['data'][0]
            else:
                summary_result = None
            
            graph_query = GRAPH_21_FORECAST.replace(FILTER_CATEGORIES, filter_categories)
            query = graph_query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.VENDOR_NUMBER, vendornumber)
            logging.info("----query-----")
            logging.info(query)
            graph_result = BigQueryUtil.search_and_display(query, limit, page)
            for each in graph_result['data']:
                each['date'] = each['date'].strftime(constant.REV_DATE_FORMAT)
                each["latestForecast"] = float(each["latestForecast"]) if float(each["latestForecast"]) < 1 else int(each["latestForecast"])
                each["previousForecast"] = float(each["previousForecast"]) if float(each["previousForecast"]) < 1 else int(each["previousForecast"])
            if 'data' in graph_result and graph_result['data'] and len(graph_result['data']) > 0 :
                graph_result = graph_result['data']
            else:
                graph_result = None

            get_formatted_result = CommonUtil._format_output(summary_result,graph_result)
            final_result = {}
            if not get_formatted_result:
                final_result['message'] = constant.ERROR_NO_DATA_FOUND
                final_result['data'] = []
            else:
                
                final_result = get_formatted_result
                final_result['message'] = constant.SUCCESS_MESSAGE
            return final_result
        except Exception as e:
            logging.error('Exception in _get_combined_sales_sku_forecast - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _get_long_range_sku_forecast(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]
            vendornumber = request_dictionary[constant.VENDOR_NUMBER]
            sort_by_27 = request_dictionary[constant.SORT_BY_27]
            sort_direction_27 = request_dictionary[constant.SORT_DIRECTION_27]
            sort_detail = CommonUtil.get_sort_details(sort_by_27, sort_direction_27)

            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM_NUMBER)

            today = datetime.date.today()
            column_header = []
            for i in range(1, 28):
                dates = today + datetime.timedelta(days=-today.weekday(), weeks=i)
                column_header.append(dates.strftime(constant.DEFAULT_DATE_FORMAT))

            dates = str(tuple(column_header))

            query = LONG_RANGED_SALES_QUERY.replace(FILTER_CATEGORIES, filter_categories)
            query = query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.VENDOR_NUMBER, vendornumber)
            query = query.replace(constant.SORT_DETAIL, sort_detail)
            query = query.replace(constant.DATES, dates)
            logging.info("----query-----")
            logging.info(query)
            final_result = BigQueryUtil.search_and_display(query, limit, page)
        
            final_list = []    
            for each in final_result['data']:
                result = {}
                each1 = each.items()
                each1 = dict(each1)
                result['sku'] = each1['sku']
                result['skuName'] = each1['skuName']
                dates_list = []
                each1.pop('sku', None)
                each1.pop('skuName', None)
                each1.pop('full_count', None)
                for date, value in each1.items():
                    temp = {}
                    date = date.replace("_","-")
                    date= date[1:]
                    date = datetime.datetime.strptime(date, constant.DEFAULT_DATE_FORMAT)
                    date1 = str(date.date())
                    date1 = datetime.datetime.strptime(date1, constant.DEFAULT_DATE_FORMAT).strftime('%d-%m-%Y')
                    temp['date'] = date1
                    temp['value'] = value
                    dates_list.append(temp)
                result['currentForecast'] = dates_list
                final_list.append(result)
            if 'data' in final_result and final_result['data'] and len(final_result['data']) > 0:
                final_list[0]['full_count']=final_result['data'][0]['full_count']
            else:
                final_list = None
            final_result = BigQueryUtil._get_output_as_dict(final_list, limit, page)
            dates_l = []
            for each in column_header:
                each = each.replace("_","-")
                date1 = datetime.datetime.strptime(each, constant.DEFAULT_DATE_FORMAT).strftime('%d-%m-%Y')
                dates_l.append(date1)
            final_result['dateColumns'] = dates_l
            return final_result
        except Exception as e:
            logging.error('Exception in _get_long_range_sku_forecast - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    
    @staticmethod
    def _get_long_ranged_combined_forecast(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]
            vendornumber = request_dictionary[constant.VENDOR_NUMBER]
            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM_NUMBER)
            
            graph_query = GRAPH_LONG_RANGED.replace(FILTER_CATEGORIES, filter_categories)
            query = graph_query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.VENDOR_NUMBER, vendornumber)
            logging.info("----query-----")
            logging.info(query)
            graph_result = BigQueryUtil.search_and_display(query, limit, page)
            for each in graph_result['data']:
                each['date'] = each['week_date'].strftime(constant.REV_DATE_FORMAT)
                each["latestForecast"] = float(each["latestForecast"]) if float(each["latestForecast"]) < 1 else int(each["latestForecast"])
                del each['week_date']
            if 'data' in graph_result and graph_result['data'] and len(graph_result['data']) > 0 :
                graph_result = graph_result['data']
            else:
                graph_result = None
            
            summary_query = SUMMARY_LONG_RANGED_QUERY.replace(FILTER_CATEGORIES, filter_categories)
            query = summary_query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.VENDOR_NUMBER, vendornumber)
            logging.info("----query-----")
            logging.info(query)
            summary_result = BigQueryUtil.search_and_display(query, limit, page)
            logging.info("-----summary_result------")
            logging.info(summary_result)
            if 'data' in summary_result and summary_result['data'] and len(summary_result['data']) > 0 :
                summary_result = summary_result['data'][0]
            else:
                summary_result = None        
            last_refresh_date = BigQueryUtil.search_and_display(LAST_REFRESH_DATE, limit, page)
            logging.info("-----last_refresh_date-------")
            logging.info(last_refresh_date)
            last_refresh_date = last_refresh_date['data'][0]['date_']
            get_formatted_result = SalesForecastServiceHandle._format_output(summary_result,graph_result, last_refresh_date)
            logging.info("-----get_formatted_result-----")
            logging.info(get_formatted_result)
            final_result = {}
            if not get_formatted_result:
                final_result['message'] = constant.ERROR_NO_DATA_FOUND
                final_result['data'] = []
            else:
                final_result = get_formatted_result
                final_result['message'] = constant.SUCCESS_MESSAGE
            
            return final_result
        except Exception as e:
            logging.error('Exception in _get_long_ranged_combined_forecast - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _format_output(summary_result, graph_result, last_refresh_date):
        graph_result = SalesForecastServiceHandle.get_chart_date(graph_result)
        if summary_result and graph_result:
            final_data = {
                "summary": {
                    "weeklyAverage": {
                        "latestValue": summary_result["weeklyAverage"]
                    }
                },
                "chartData": graph_result,
                "lastDataRefresh":last_refresh_date
            }
            return final_data
        else:
            return {}

    @staticmethod
    def get_chart_date(graph_result):
        t = graph_result[0]['date']
        t1 = graph_result[-1]['date']
        graph_result[-1]['previousForecast'] = None
        count = graph_result[0]['full_count']
        t=datetime.datetime.strptime(t,constant.REV_DATE_FORMAT)
        t1=datetime.datetime.strptime(t1,constant.REV_DATE_FORMAT)
        t = t.date() - datetime.timedelta(weeks=1)
        t1= t1.date() + datetime.timedelta(weeks=1)
        t=datetime.datetime.strptime(str(t),constant.DEFAULT_DATE_FORMAT).strftime(constant.REV_DATE_FORMAT)
        t1=datetime.datetime.strptime(str(t1),constant.DEFAULT_DATE_FORMAT).strftime(constant.REV_DATE_FORMAT)
        temp = {'latestForecast': None, 'previousForecast': None, 'date': t, 'full_count': count}
        temp1 = {'latestForecast': None, 'previousForecast': None, 'date': t1, 'full_count': count}
        graph_result.insert(0,temp)
        graph_result.append(temp1)
        return graph_result


    @staticmethod
    def _download_forecast_21_days(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]

            vendornumber = request_dictionary[constant.VENDOR_NUMBER]
            username = request_dictionary[constant.USERNAME]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]

            date_query = SALES_DATE_QUERY.replace(constant.VENDOR_NUMBER, vendornumber)
            latest_date = BigQueryUtil.search_and_display(date_query, limit, page)
            logging.info("-----------latest_date-------") 
            logging.info(latest_date)
            latest_date = latest_date['data'][0]['date_']
            column_header = []
            column_1 = ""
            column_2 = ""
            for i in range(1, 22):
                dates = latest_date + datetime.timedelta(days=i)
                column_header.append(dates.strftime(constant.DEFAULT_DATE_FORMAT))
                column = '""'+" `LATEST_FORECAST_"+str(dates.strftime("%Y_%m_%d"))+"`,"+'""'+" `PREV_FORECAST_"+str(dates.strftime("%Y_%m_%d"))+"`,"
                column_1 = column_1 + column
                column1 = '"'+str(dates.strftime("%d/%m/%Y"))+'_LATEST_FORECAST"'+" `LATEST_FORECAST_"+str(dates.strftime("%Y_%m_%d"))+"`, "+'"'+str(dates.strftime("%d/%m/%Y"))+'_PREV_FORECAST"'+" `PREV_FORECAST_"+str(dates.strftime("%Y_%m_%d"))+"`,"
                column_2 = column_2 + column1
            
            dates = str(tuple(column_header))

            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM_NUMBER)
    
            
            download_query = DOWNLOAD_API_21DAYS.replace(FILTER_CATEGORIES, filter_categories)
            query = download_query.replace(constant.DATES, dates)
            query = query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.VENDOR_NUMBER, vendornumber)
            logging.info("--------query--------")
            logging.info(query)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('salesForecast_21Days_', username)

            query_columns = DOWNLOAD_QUERY_COL_LATEST.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
            query1 = query_columns.replace(constant.COLUMN1, column_1)
            query1 = query1.replace(constant.COLUMN2, column_2)
            logging.info("-----query_columns------")
            logging.info(query1)
            destination_blob=file_and_table_name
            download_result =  BigQueryUtil.create_download_table(query1,destination_blob, file_and_table_name, vendornumber, 'csv')
            time.sleep(1)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('salesForecast_21Days_', username)
            logging.info("------query------")
            logging.info(query)
            download_result = BigQueryUtil.create_download_table(query, destination_blob, file_and_table_name, vendornumber, 'csv')
            logging.info("-------download_result-------")
            logging.info(download_result)
            return download_result
        except Exception as e:
            logging.error('Exception in _download_forecast_21_days - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def _download_forecast_27_weeks(request_dictionary):
        try:
            vendornumber = request_dictionary[constant.VENDOR_NUMBER]
            username = request_dictionary[constant.USERNAME]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]
            today = datetime.date.today()
            column_header = []
            column_1 = ""
            column_2 = ""
            for i in range(1, 28):
                dates = today + datetime.timedelta(days=-today.weekday(), weeks=i)
                column_header.append(dates.strftime(constant.DEFAULT_DATE_FORMAT))
                column = '""'+" `WK_COMMENCING_"+str(dates.strftime("%Y_%m_%d"))+"`,"
                column_1 = column_1 + column
                column1 = '"WK_COMMENCING_'+str(dates.strftime("%d/%m/%Y"))+'"'+" `WK_COMMENCING_"+str(dates.strftime("%Y_%m_%d"))+"`,"
                column_2 = column_2 + column1

            dates = str(tuple(column_header))
            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM_NUMBER)
        
            
            download_query = DOWNLOAD_API_27_WEEKS.replace(FILTER_CATEGORIES, filter_categories)
            query = download_query.replace(constant.DATES, dates)
            query = query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.VENDOR_NUMBER, vendornumber)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('salesForecast_27weeks_', username)
            query_columns = DOWNLOAD_QUERY_COL_LATEST.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
            query1 = query_columns.replace(constant.COLUMN1, column_1)
            query1 = query1.replace(constant.COLUMN2, column_2)
            logging.info("-----query_columns------")
            logging.info(query1)
            destination_blob=file_and_table_name
            download_result = BigQueryUtil.create_download_table(query1,destination_blob, file_and_table_name, vendornumber, 'csv')
            time.sleep(1)
            logging.info("------query------")
            logging.info(query)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('salesForecast_27weeks_', username)
            download_result = BigQueryUtil.create_download_table(query, destination_blob, file_and_table_name, vendornumber, 'csv')
            return download_result
        except Exception as e:
            logging.error('Exception in _download_forecast_27_weeks - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}


    @staticmethod
    def get_handle(request_dictionary):
        path = request_dictionary[constant.PATH]

        if request_dictionary[constant.VENDOR_NUMBER] == constant.DEFAULT_VENDOR_NUMBER:
            return {constant.MESSAGE: constant.ERROR_NO_VENDOR_SPECIFIED}

        elif path == '/getSkuSalesForecast':
            final_result = SalesForecastServiceHandle._get_sku_sales_forecast(request_dictionary)
            return final_result

        elif path == '/getLongRangeSkuSalesForecast':
            final_result = SalesForecastServiceHandle._get_long_range_sku_forecast(request_dictionary)
            return final_result

        elif path == '/getCombinedSalesForecast':
            final_result = SalesForecastServiceHandle._get_combined_sales_sku_forecast(request_dictionary)
            return final_result

        elif path == '/getLongRangeCombinedSalesForecast':
            final_result = SalesForecastServiceHandle._get_long_ranged_combined_forecast(request_dictionary)
            return final_result

        elif path == '/getSkuSalesForecast/download':
            final_result = SalesForecastServiceHandle._download_forecast_21_days(request_dictionary)
            return final_result

        elif path == '/getLongRangeSkuSalesForecast/download':
            final_result = SalesForecastServiceHandle._download_forecast_27_weeks(request_dictionary)
            return final_result
       
        else:
            return {constant.MESSAGE: constant.ERROR_INVALID_PATH}