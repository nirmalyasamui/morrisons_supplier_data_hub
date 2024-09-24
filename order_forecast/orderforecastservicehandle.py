from bigqueryutil import BigQueryUtil
from commonutil import CommonUtil
import constant as constant
import time
import datetime

import google.cloud.logging
import logging

client = google.cloud.logging.Client()
client.setup_logging()

# Define local constants...
# Define local constants...
FILTER_CATEGORIES = 'filter_categories'
FILTER_CLASSES = 'filter_classes'
FILTER_BRANDS = 'filter_brands'
FILTER_SKUS = 'filter_skus'

COLUMN_COMMERCIAL_CATEGORY = 'commercial_category'
COLUMN_COMMERCIAL_CLASS = 'commercial_class'
COLUMN_BRAND_NAME = 'brand_name'
COLUMN_SKU_ITEM_NUMBER = 'sku_item_number'

ORDER_FORECAST_BASE_QUERY = 'select   R_FORECASTDATE,   R_DATE,   R_LOCATIONID,   R_SKU,   R_FORECASTVOLUME from ( SELECT   R_FORECASTDATE,   R_FORECASTTYPE,   R_DATE,   R_LOCATIONID,   R_LOCATIONTYPE,   s_item.rms_sku_number AS R_SKU,   SUM(R_FORECASTVOLUME) AS R_FORECASTVOLUME,   R_SUPPLIER FROM (   SELECT     DISTINCT DATE(nco.last_updt_dt) AS R_FORECASTDATE,     "Orders" AS R_FORECASTTYPE,     DATE(nco.rls_dt) AS R_DATE,     SUBSTR(sp2.stcking_pnt_nbr, 2) AS R_LOCATIONID,     "Depot" AS R_LOCATIONTYPE, nco.non_cntnts_qty   AS R_FORECASTVOLUME,     nco.vndr_nbr AS R_SUPPLIER,     commodity_id   FROM     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.source_data_partitioned.DWB_X_NON_CNTNTS_ORDR` nco,     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.source_data_partitioned.AIP_DWI_COMMODITY_MAPPING` cm,     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.source_data_partitioned.DWR_X_STCKING_POINT` sp1,     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.source_data_partitioned.DWR_X_STCKING_POINT` sp2   WHERE  nco.vndr_nbr = "' + constant.VENDOR_NUMBER + '"  AND  nco.src_typ ="V"     AND CAST(nco.cmdty_cd AS INT64) = CAST(cm.commodity_id AS INT64)     AND nco.pck_sz =cm.pack_size     AND nco.stcking_pnt_cd = sp1.stcking_pnt_cd     AND sp2.stcking_pnt_cd = sp1.prnt_stcking_pnt_cd     AND nco.non_cntnts_status_cd = "U"     AND sp1.curr_ind ="Y"     AND sp2.curr_ind ="Y"     AND CAST( DATE(nco.last_updt_dt) AS DATE) BETWEEN CURRENT_DATE() - 2     AND CURRENT_DATE() - 1     AND CAST( DATE(nco.load_dt) AS DATE) BETWEEN CAST(DATE_SUB( DATE(CURRENT_TIMESTAMP()), INTERVAL 20 DAY) AS DATE)     AND CAST( DATE(CURRENT_TIMESTAMP()) AS DATE)     AND DATE(nco.rls_dt) <= DATE_ADD(DATE(nco.last_updt_dt), INTERVAL 14 DAY) ) AS all_dt LEFT OUTER JOIN (   SELECT     commodity_id,     rms_sku_number   FROM     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.source_data_partitioned.AIP_DWI_COMMODITY_MAPPING`   WHERE     pack_size = 1 ) AS s_item ON   (s_item.commodity_id=all_dt.commodity_id) GROUP BY   R_FORECASTDATE,   R_DATE,   R_LOCATIONID,   R_SKU,   R_FORECASTTYPE,   R_LOCATIONTYPE,   R_SUPPLIER UNION DISTINCT SELECT   PARSE_DATE("%Y%m%d",     day_key) AS R_FORECASTDATE,   "Orders" AS R_FORECASTTYPE,   PARSE_DATE("%Y%m%d",     CAST(ordr_dt_key AS STRING)) AS R_DATE,   bsns_unit_cd AS R_LOCATIONID,   "Depot" AS R_LOCATIONTYPE,   sku_item_nbr AS R_SKU,   qty_units AS R_FORECASTVOLUME,   vndr_cd AS R_SUPPLIER FROM (   SELECT     day_key,     ordr_dt_key,     bsns_unit_cd,     sku_item_nbr,     vndr_cd,     qty_units,     ROW_NUMBER() OVER (PARTITION BY day_key, CAST(ordr_dt_key AS int64),       bsns_unit_cd,       sku_item_nbr,       vndr_cd     ORDER BY       extract_run DESC) rn,     extract_run   FROM (     SELECT       day_key,       ordr_dt_key,       bsns_unit_cd,       sku_item_nbr,       vndr_cd,       extract_run,       SUM(QTY_CASES) qty_units     FROM       `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.source_data_partitioned.DWB_X_RLX_PRJCTD_ORDRS`     WHERE       1=1       AND CAST( DATE(trx_dt) AS DATE) BETWEEN CAST(DATE_SUB( DATE(CURRENT_TIMESTAMP()), INTERVAL 20 DAY) AS DATE)       AND CAST( DATE(CURRENT_TIMESTAMP()) AS DATE)       AND CAST( DATE(insight_platform_load_timestamp) AS DATE) BETWEEN CURRENT_DATE() - 2       AND CURRENT_DATE() - 1 AND VNDR_CD="' + constant.VENDOR_NUMBER + '"    GROUP BY       day_key,       ordr_dt_key,       bsns_unit_cd,       sku_item_nbr,       vndr_cd,       extract_run ) ) WHERE   rn=1)'

LAST_MAX_DATE_QUERY = 'WITH  f_order AS ('+ORDER_FORECAST_BASE_QUERY+') SELECT   MAX(DATE(R_FORECASTDATE)) + 1 AS refDate,   MAX(DATE(R_DATE)) AS forecastDate FROM   f_order'

MAX_DATE_QUERY = 'SELECT   MAX(DATE(R_FORECASTDATE)) AS refDate,   MAX(DATE(R_DATE)) AS forecastDate FROM   f_order'

CURRENT_BASE_QUERY = 'SELECT   CAST(SUM(R_FORECASTVOLUME) AS INT64) AS latest14DaysForecast,   R_SKU AS sku,   dim_prd.item_description AS skuName FROM   `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.data_marts_dimensions.DIM_PRODUCT_CURRENT` AS dim_prd,   Maxdate,   f_order WHERE   DATE(R_DATE) <= Maxdate.refDate + 14   AND DATE(R_FORECASTDATE) = Maxdate.refDate   AND R_SKU = dim_prd.SKU_ITEM_NUMBER   AND UPPER(dim_prd.commercial_category) IN ('+FILTER_CATEGORIES +')  AND UPPER(dim_prd.commercial_class) IN ('+ FILTER_CLASSES +')  AND UPPER(dim_prd.brand_name) IN ( '+ FILTER_BRANDS +')   AND UPPER(dim_prd.sku_item_number) IN (' + FILTER_SKUS + ')  GROUP BY   sku,   skuName'

COMPARABLE_LATEST_BASE_QUERY = 'SELECT   CAST(SUM(R_FORECASTVOLUME) AS INT64) AS latest13DaysForecast,   R_SKU AS sku,   dim_prd.item_description AS skuName,   COUNT(DISTINCT(R_DATE)) AS dayDate FROM   f_order,   `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.data_marts_dimensions.DIM_PRODUCT_CURRENT` AS dim_prd,   Maxdate WHERE   DATE(R_DATE) >= Maxdate.refDate + 1   AND DATE(R_DATE) <= Maxdate.refDate + 13   AND DATE(R_FORECASTDATE) = Maxdate.refDate   AND R_SKU = dim_prd.SKU_ITEM_NUMBER   AND UPPER(dim_prd.commercial_category) IN ('+FILTER_CATEGORIES +') AND UPPER(dim_prd.commercial_class) IN ('+ FILTER_CLASSES +') AND UPPER(dim_prd.brand_name) IN ( '+ FILTER_BRANDS +')  AND UPPER(dim_prd.sku_item_number) IN (' + FILTER_SKUS + ') GROUP BY  sku,   skuName'

COMPARABLE_PREVIOUS_BASE_QUERY = 'SELECT   CAST(SUM(R_FORECASTVOLUME) AS INT64) AS previous13DaysForecast,   R_SKU AS sku,   dim_prd.item_description AS skuName,   COUNT(DISTINCT(R_DATE)) AS dayDate FROM   `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.data_marts_dimensions.DIM_PRODUCT_CURRENT` AS dim_prd,   Maxdate,   f_order WHERE   DATE(R_DATE) >= Maxdate.refDate + 1  AND DATE(R_DATE) <= Maxdate.refDate + 13   AND DATE(R_FORECASTDATE) = Maxdate.refDate - 1   AND R_SKU = dim_prd.SKU_ITEM_NUMBER   AND UPPER(dim_prd.commercial_category) IN ('+FILTER_CATEGORIES +')   AND UPPER(dim_prd.commercial_class) IN ('+ FILTER_CLASSES +')   AND UPPER(dim_prd.brand_name) IN ( '+ FILTER_BRANDS +') AND UPPER(dim_prd.sku_item_number) IN (' + FILTER_SKUS + ') GROUP BY   sku,   skuName'

SUMMARY_FORECAST_QUERY = 'SELECT   IFNULL(SUM(currentForecast),0) AS currentForecastlatestValue,   IFNULL(SUM(previousForecast),0) AS previousForecastlatestValue,   IFNULL(SUM(comparableLatestForecast),0) AS comparablePeriodForecastlatestValue,   IFNULL(SUM(comparablePreviousForecast),0) AS comparablePeriodForecastpreviousValue,   IFNULL(SUM(comparableLatestForecast) - SUM(comparablePreviousForecast),0) AS comparableChangeValue,   IFNULL(SUM(latestComparableAverage),0) AS dailyAverageForecastlatestValue,   IFNULL(SUM(previousComparableAverage),0) AS dailyAverageForecastpreviousValue,   IFNULL(SUM(latestComparableAverage) - SUM(previousComparableAverage),0) AS dailyAverageChange,   IFNULL(SUM(comparableLatestForecast) - SUM(comparablePreviousForecast),0) AS absoluteChangeValue,   IFNULL((CASE       WHEN (SUM(comparableLatestForecast) - SUM(comparablePreviousForecast)) = 0 THEN 0     ELSE     ROUND(((SUM(comparableLatestForecast) - SUM(comparablePreviousForecast))/(CASE             WHEN SUM(comparablePreviousForecast) = 0 THEN 1           ELSE           SUM(comparablePreviousForecast)         END           ) * 100), 2)   END     ),0) AS absoluteChangePercentage FROM (   WITH     f_order AS ('+ORDER_FORECAST_BASE_QUERY+'),     Maxdate AS('+MAX_DATE_QUERY+'),     Latest14days AS ('+CURRENT_BASE_QUERY+'),     Previous14days AS (     SELECT       CAST(SUM(R_FORECASTVOLUME) AS INT64) AS previous14DaysForecast,       R_SKU AS sku,       dim_prd.item_description AS skuName     FROM       `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.data_marts_dimensions.DIM_PRODUCT_CURRENT` AS dim_prd,       Maxdate,       f_order     WHERE    DATE(R_DATE) >= Maxdate.refDate AND   DATE(R_DATE) <= Maxdate.refDate + 13        AND DATE(R_FORECASTDATE) = Maxdate.refDate - 1       AND R_SKU = dim_prd.SKU_ITEM_NUMBER       AND UPPER(dim_prd.commercial_category) IN ('+FILTER_CATEGORIES +')       AND UPPER(dim_prd.commercial_class) IN ('+ FILTER_CLASSES +')       AND UPPER(dim_prd.brand_name) IN ( '+ FILTER_BRANDS +')       AND UPPER(dim_prd.sku_item_number) IN (' + FILTER_SKUS + ')     GROUP BY       sku,       skuName),     LatestComparable AS( '+COMPARABLE_LATEST_BASE_QUERY+'),     PreviousComparable AS( '+COMPARABLE_PREVIOUS_BASE_QUERY+'),     final AS (     SELECT       coalesce(n.sku,         lo.sku,         o.sku,         n1.sku),       coalesce(n.skuName,         lo.skuName,         o.skuName,         n1.skuName) AS skuName,       IFNULL(n.latest14DaysForecast,         0) AS currentForecast,       IFNULL(n1.previous14DaysForecast,         0) AS previousForecast,       IFNULL(o.latest13DaysForecast,         0) AS comparableLatestForecast,       IFNULL(lo.previous13DaysForecast,         0) AS comparablePreviousForecast,       CAST(IFNULL(o.latest13DaysForecast,           0)/o.dayDate AS INT64) AS latestComparableAverage,       CAST(IFNULL(lo.previous13DaysForecast,           0)/lo.dayDate AS INT64) AS previousComparableAverage     FROM       LatestComparable AS o     FULL JOIN       Latest14days AS n     ON       n.sku = o.sku     FULL JOIN       PreviousComparable AS lo     ON       n.sku = lo.sku     FULL JOIN       Previous14days AS n1     ON       lo.sku = n1.sku)   SELECT     *   FROM     final)'

GRAPH_FORECAST_QUERY ='WITH   f_order AS ('+ORDER_FORECAST_BASE_QUERY+'),  Maxdate AS('+MAX_DATE_QUERY+'),   LatestForecast AS (   SELECT     DATE(R_DATE) AS l_date,     CAST(SUM(R_FORECASTVOLUME) AS INT64) AS latestDaysForecast   FROM     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.data_marts_dimensions.DIM_PRODUCT_CURRENT` AS dim_prd,     Maxdate,     f_order   WHERE     DATE(R_DATE) <= Maxdate.refDate + 14     AND DATE(R_FORECASTDATE) = Maxdate.refDate     AND R_SKU = dim_prd.SKU_ITEM_NUMBER     AND UPPER(dim_prd.commercial_category) IN ('+FILTER_CATEGORIES +')     AND UPPER(dim_prd.commercial_class) IN ('+ FILTER_CLASSES +')     AND UPPER(dim_prd.brand_name) IN ( '+ FILTER_BRANDS +')     AND UPPER(dim_prd.sku_item_number) IN (' + FILTER_SKUS + ')   GROUP BY     l_date),   PreviousForecast AS (   SELECT     DATE(R_DATE) AS p_date,     CAST(SUM(R_FORECASTVOLUME) AS INT64) AS previousDaysForecast   FROM     `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.data_marts_dimensions.DIM_PRODUCT_CURRENT` AS dim_prd,     Maxdate,     f_order   WHERE     DATE(R_DATE) >= Maxdate.refDate + 1     AND DATE(R_DATE) <= Maxdate.refDate + 13     AND DATE(R_FORECASTDATE) = Maxdate.refDate - 1     AND R_SKU = dim_prd.SKU_ITEM_NUMBER     AND UPPER(dim_prd.commercial_category) IN ('+FILTER_CATEGORIES +')     AND UPPER(dim_prd.commercial_class) IN ('+ FILTER_CLASSES +')     AND UPPER(dim_prd.brand_name) IN ( '+ FILTER_BRANDS +')     AND UPPER(dim_prd.sku_item_number) IN (' + FILTER_SKUS + ')   GROUP BY     p_date),   final_calc AS (   SELECT     CAST(la.latestDaysForecast AS INT64) AS latestForecast,     CAST(pa.previousDaysForecast AS INT64) AS previousForecast,     IFNULL(DATE(la.l_date),       DATE(pa.p_date)) AS date   FROM     LatestForecast AS la   FULL OUTER JOIN     PreviousForecast AS pa   ON     ifnull(la.l_date,       "2500-12-01" )= ifnull(pa.p_date,       "2500-12-01")) SELECT   IFNULL(latestForecast,     0) AS latestForecast,   IFNULL(previousForecast,     0) AS previousForecast,   ifnull(date,     DATE(clndr_dt)) AS date FROM   final_calc FULL OUTER JOIN   `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '.source_data_partitioned.DWV_TIME_DAY` dtd ON   final_calc.date = DATE(dtd.clndr_dt) WHERE   DATE(dtd.clndr_dt) >(   SELECT     refDate   FROM     Maxdate)   AND DATE(dtd.clndr_dt) <=(   SELECT     refDate + 14   FROM     Maxdate) ORDER BY   date ASC'

SKU_FORECAST_QUERY = 'WITH  f_order AS ('+ORDER_FORECAST_BASE_QUERY+'), Maxdate AS('+MAX_DATE_QUERY+'),   Latest14days AS ('+CURRENT_BASE_QUERY+'),   LatestComparable AS('+COMPARABLE_LATEST_BASE_QUERY+'),   PreviousComparable AS('+COMPARABLE_PREVIOUS_BASE_QUERY+'),   final AS (   SELECT     coalesce(n.sku,lo.sku,o.sku) AS sku,      coalesce(n.skuName,lo.skuName,o.skuName) as skuName,     IFNULL(n.latest14DaysForecast,0) AS currentForecast,     IFNULL(o.latest13DaysForecast,0) AS comparableLatestForecast,     IFNULL(lo.previous13DaysForecast,0) AS comparablePreviousForecast,     (IFNULL(o.latest13DaysForecast,0) - IFNULL(lo.previous13DaysForecast,0)) AS forecastVariance   FROM     LatestComparable AS o   FULL JOIN     Latest14days AS n   ON      n.sku = o.sku   FULL JOIN     PreviousComparable AS lo   ON     n.sku = lo.sku) SELECT   * FROM   final ORDER BY   '+ constant.SORT_DETAIL +''

DOWNLOAD_ORDER_FORECAST_QUERY = """WITH
  f_order_1 AS 
  ("""+ORDER_FORECAST_BASE_QUERY+"""),
  Maxdate AS(
  SELECT
    MAX(DATE(R_FORECASTDATE)) AS refDate,
    MAX(DATE(R_DATE)) AS forecastDate
  FROM
    f_order_1),
  date_range AS (
  SELECT
    d_date_range
  FROM
    UNNEST(GENERATE_DATE_ARRAY((
SELECT
    refDate
    FROM
    Maxdate), (
    SELECT
    refDate + 14
    FROM
    Maxdate), INTERVAL 1 DAY)) d_date_range),
  forcaste_date AS (
  SELECT
    f_date
  FROM
    UNNEST(GENERATE_DATE_ARRAY((
        SELECT
          Maxdate.refDate
        FROM
          Maxdate)-1, (
        SELECT
          refDate
        FROM
          Maxdate), INTERVAL 1 DAY))f_date),
  f_order AS(
  SELECT
    a.R_FORECASTDATE,
    a.R_DATE,
    IFNULL(b.R_LOCATIONID,
      ' '),
    a.R_SKU,
    IFNULL(b.R_FORECASTVOLUME,
      0)R_FORECASTVOLUME,
  FROM (
    SELECT
      f_date R_FORECASTDATE,
      d_date_range R_DATE,
      R_SKU
    FROM (
      SELECT
        DISTINCT R_SKU,
      FROM
        f_order_1 ),
      date_range,
      forcaste_date
    WHERE
      1=1 ) a
  LEFT JOIN
    f_order_1 b
  ON
    ( a.R_SKU=b.R_SKU
      AND b.R_FORECASTDATE=a.R_FORECASTDATE
      AND a.R_DATE=b.R_DATE) ),   latestForecast AS (   SELECT     CAST(SUM(R_FORECASTVOLUME) AS INT64) AS latestDaysForecast,     DATE(R_DATE) AS l_date,     R_SKU AS sku,     dim_prd.item_description AS skuName,     dim_prd.commercial_category AS lcommercial_category,     dim_prd.commercial_class AS lcommercial_class,     dim_prd.brand_name AS lbrand_name   FROM     `""" + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + """.data_marts_dimensions.DIM_PRODUCT_CURRENT` AS dim_prd,     Maxdate,     f_order   WHERE     DATE(R_DATE) <= Maxdate.refDate + 14     AND DATE(R_FORECASTDATE) = Maxdate.refDate     AND R_SKU = dim_prd.SKU_ITEM_NUMBER     AND UPPER(dim_prd.commercial_category) IN ("""+FILTER_CATEGORIES +""")     AND UPPER(dim_prd.commercial_class) IN ("""+ FILTER_CLASSES +""")     AND UPPER(dim_prd.brand_name) IN ( """+ FILTER_BRANDS +""")     AND UPPER(dim_prd.sku_item_number) IN ("""+ FILTER_SKUS + """)   GROUP BY     sku,     skuName,     l_date,     lcommercial_category,     lcommercial_class,     lbrand_name),   previousForecast AS(   SELECT     CAST(SUM(R_FORECASTVOLUME) AS INT64) AS previousDaysForecast,     DATE(R_DATE) AS p_date,     R_SKU AS sku,     dim_prd.item_description AS skuName,     dim_prd.commercial_category AS pcommercial_category,     dim_prd.commercial_class AS pcommercial_class,     dim_prd.brand_name AS pbrand_name   FROM     `""" + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + """.data_marts_dimensions.DIM_PRODUCT_CURRENT` AS dim_prd,     Maxdate,     f_order   WHERE     DATE(R_DATE) >= Maxdate.refDate     AND DATE(R_DATE) <= Maxdate.refDate + 13     AND DATE(R_FORECASTDATE) = Maxdate.refDate - 1     AND R_SKU = dim_prd.SKU_ITEM_NUMBER     AND UPPER(dim_prd.commercial_category) IN ("""+FILTER_CATEGORIES +""")     AND UPPER(dim_prd.commercial_class) IN ("""+ FILTER_CLASSES +""")     AND UPPER(dim_prd.brand_name) IN ( """+ FILTER_BRANDS +""")     AND UPPER(dim_prd.sku_item_number) IN (""" + FILTER_SKUS + """)   GROUP BY     sku,     skuName,     p_date,     pcommercial_category,     pcommercial_class,     pbrand_name) SELECT   DISTINCT * FROM (   SELECT     UPPER(COALESCE(lf.lcommercial_category,       pf.pcommercial_category)) AS CATEGORY,     UPPER(COALESCE(lf.lcommercial_class,       pf.pcommercial_class)) AS SUBCATEGORY,     UPPER(COALESCE(lf.lbrand_name,       pf.pbrand_name)) AS BRAND,     coalesce(lf.sku,       pf.sku) AS sku,     UPPER(coalesce(lf.skuName,       pf.skuName)) AS skuName,     FORMAT("%'d",IFNULL(lf.latestDaysForecast,0)) AS l_value,     FORMAT("%'d",IFNULL(pf.previousDaysForecast,0)) p_value,     coalesce(lf.l_date,       pf.p_date) AS dates   FROM     latestForecast AS lf   FULL OUTER JOIN     previousForecast AS pf   ON     lf.sku = pf.sku     AND lf.l_date=pf.p_date   ORDER BY     sku )AS main PIVOT(string_agg(l_value) AS LATEST_FORECAST,     string_agg(case when dates=current_date()+13 then null else p_value end) AS PREV_FORECAST FOR dates IN """+constant.DATES+""")"""

DOWNLOAD_QUERY_COLUMNS= 'SELECT   "ORDER_VOLUME_CASES" CATEGORY,   "" SUBCATEGORY,   "" BRAND,   "" sku,   "" skuName,   '+constant.COLUMN1+' UNION ALL SELECT   "Please Note - These are order forecasts only and may change." CATEGORY,   "" SUBCATEGORY,   "" BRAND,   "" sku,   "" skuName,   '+constant.COLUMN1+' UNION ALL SELECT   "Please Note - The SKUs for which there is no relevant data do not feature in this report." CATEGORY,   "" SUBCATEGORY,   "" BRAND,   "" sku,   "" skuName,   '+constant.COLUMN1+' UNION ALL SELECT   "Supplier: SELECTED_SUPPLIER_NAME" CATEGORY,   "" SUBCATEGORY,   "" BRAND,   "" sku,   "" skuName,   '+constant.COLUMN1+' UNION ALL SELECT   "CATEGORY" AS CATEGORY,   "SUBCATEGORY" AS SUBCATEGORY,   "BRAND" AS BRAND,   "SKU" AS sku,   "SKU_DESCRIPTION" AS skuName,   '+constant.COLUMN2+''

AVAILABLE_DATE_COUNT = 'WITH f_order AS ('+ORDER_FORECAST_BASE_QUERY+'),   Maxdate AS('+MAX_DATE_QUERY+') SELECT   COUNT(DISTINCT(DATE(R_DATE))) dayCount FROM   f_order,   Maxdate WHERE   DATE(R_DATE) <= Maxdate.forecastDate   AND DATE(R_FORECASTDATE) = Maxdate.refDate'

class OrderForecastServiceHandle:
    @staticmethod
    def _get_sku_order_forecast(request_dictionary):
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
            logging.info("-----query------")
            logging.info(query)
            final_result = BigQueryUtil.search_and_display(query, limit, page)
            # Format out put JSON as per UI requirement...
            logging.info("-----final_result------")
            logging.info(final_result)
            return final_result
        except Exception as e:
            logging.error('Exception in _get_sku_order_forecast - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}
   
    @staticmethod
    def _get_combined_order_sku_forecast(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]
            vendornumber = request_dictionary[constant.VENDOR_NUMBER]

            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM_NUMBER)
        
            summary_query = SUMMARY_FORECAST_QUERY.replace(FILTER_CATEGORIES, filter_categories)
            summary_query = summary_query.replace(FILTER_CATEGORIES, filter_categories)
            query = summary_query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.VENDOR_NUMBER, vendornumber)
            logging.info("-----query------")
            logging.info(query)
            summary_result = BigQueryUtil.search_and_display(query, limit, page)
            if 'data' in summary_result and summary_result['data'] and len(summary_result['data']) > 0 :
                summary_result = summary_result['data'][0]
            else:
                summary_result = None
        
            graph_query = GRAPH_FORECAST_QUERY.replace(FILTER_CATEGORIES, filter_categories)
            query = graph_query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.VENDOR_NUMBER, vendornumber)
            logging.info("-----query------")
            logging.info(query)
            graph_result = BigQueryUtil.search_and_display(query, limit, page)
            for each in graph_result['data']:
                each['date'] = each['date'].strftime("%d-%m-%Y")
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
            
            logging.info("-----final_result------")
            logging.info(final_result)
            return final_result
        except Exception as e:
            logging.error('Exception in _get_combined_order_sku_forecast - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}



    @staticmethod
    def _download_order_forecast(request_dictionary):
        try:
            limit = request_dictionary[constant.LIMIT]
            page = request_dictionary[constant.PAGE]

            vendor_number = request_dictionary[constant.VENDOR_NUMBER]
            username = request_dictionary[constant.USERNAME]
            suppliername = request_dictionary[constant.SUPPLIER_NAME]

            date_count_query = AVAILABLE_DATE_COUNT.replace(constant.VENDOR_NUMBER, vendor_number)
            temp_max_date_query = LAST_MAX_DATE_QUERY.replace(constant.VENDOR_NUMBER, vendor_number)
            result = BigQueryUtil.search_and_display(date_count_query, limit, page)
            date_result = BigQueryUtil.search_and_display(temp_max_date_query, limit, page)
            logging.info("------result")
            logging.info(result) 
            logging.info("------date_result")
            logging.info(date_result)

            max_date = date_result['data'][0]['refDate']
            
            data = max_date-datetime.timedelta(days=1)
            column_header = []
            column_1 = ""
            column_2 = ""
            for i in range(1,15):
                dates = data + datetime.timedelta(days=i)
                column_header.append(dates.strftime('%Y-%m-%d'))
                column = '""'+" `LATEST_FORECAST_"+str(dates.strftime("%Y_%m_%d"))+"`,"+'""'+" `PREV_FORECAST_"+str(dates.strftime("%Y_%m_%d"))+"`,"
                column_1 = column_1 + column
                column1 = '"'+str(dates.strftime("%d/%m/%Y"))+'_LATEST_FORECAST"'+" `LATEST_FORECAST_"+str(dates.strftime("%Y_%m_%d"))+"`, "+'"'+str(dates.strftime("%d/%m/%Y"))+'_PREV_FORECAST"'+" `PREV_FORECAST_"+str(dates.strftime("%Y_%m_%d"))+"`,"
                column_2 = column_2 + column1
            
            dates = str(tuple(column_header))

            filter_categories = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CATEGORY)
            filter_classes = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_COMMERCIAL_CLASS)
            filter_brands = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_BRAND_NAME)
            filter_skus = CommonUtil.get_filter_value_as_string(request_dictionary, COLUMN_SKU_ITEM_NUMBER)
    
        
            download_query = DOWNLOAD_ORDER_FORECAST_QUERY.replace(FILTER_CATEGORIES, filter_categories)
            query = download_query.replace(constant.DATES, dates)
            query = query.replace(FILTER_CLASSES, filter_classes)
            query = query.replace(FILTER_BRANDS, filter_brands)
            query = query.replace(FILTER_SKUS, filter_skus)
            query = query.replace(constant.VENDOR_NUMBER, vendor_number)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('supplierOrderForecast_', username)

            query_columns = DOWNLOAD_QUERY_COLUMNS.replace(constant.SELECTED_SUPPLIER_NAME, suppliername)
            query1 = query_columns.replace(constant.COLUMN1, column_1)
            query1 = query1.replace(constant.COLUMN2, column_2)
            logging.info("-----query_columns------")
            logging.info(query1)
            destination_blob=file_and_table_name
            download_result =  BigQueryUtil.create_download_table(query1,destination_blob, file_and_table_name, vendor_number, 'csv')
            time.sleep(1)
            file_and_table_name = CommonUtil.get_temporary_file_table_name_with_user('supplierOrderForecast_', username)
            logging.info("-----download_query------")
            logging.info(query)
            download_result = BigQueryUtil.create_download_table(query, destination_blob, file_and_table_name, vendor_number, 'csv')
            logging.info("-------download_result-------")
            logging.info(download_result)
            return download_result
        except Exception as e:
            logging.error('Exception in _download_order_forecast - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}

    @staticmethod
    def get_handle(request_dictionary):
        path = request_dictionary[constant.PATH]
        logging.info("&&&request_dictionary&&&&")
        logging.info(request_dictionary)
        if path == '/getSkuOrderForecast':
            if request_dictionary[constant.VENDOR_NUMBER] == constant.DEFAULT_VENDOR_NUMBER:
                return {constant.MESSAGE: constant.ERROR_NO_VENDOR_SPECIFIED}

            final_result = OrderForecastServiceHandle._get_sku_order_forecast(request_dictionary)
            return final_result

        elif path == '/getCombinedOrderForecast':
            if request_dictionary[constant.VENDOR_NUMBER] == constant.DEFAULT_VENDOR_NUMBER:
                return {constant.MESSAGE: constant.ERROR_NO_VENDOR_SPECIFIED}

            final_result = OrderForecastServiceHandle._get_combined_order_sku_forecast(request_dictionary)
            return final_result

        elif path == '/getSkuOrderForecast/download':
            if request_dictionary[constant.VENDOR_NUMBER] == constant.DEFAULT_VENDOR_NUMBER:
                return {constant.MESSAGE: constant.ERROR_NO_VENDOR_SPECIFIED}

            final_result = OrderForecastServiceHandle._download_order_forecast(request_dictionary)
            return final_result
       
        else:
            return {constant.MESSAGE: constant.ERROR_INVALID_PATH}