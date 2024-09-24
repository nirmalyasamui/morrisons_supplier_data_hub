"""
This file does the operation of data handling of input and output related to bigquery

The final bigquery is concatenation of main + select + filters query in the code

The week query is used for week input and day query is used for 1 day input. It is selected using pick_query in code
"""

from req_res_controller import convert_to_dict,get_environment_variable
from bigquery_model import bigquery_dc_report
import constant as constant
import re

dc_cases_query = """
WITH
  curnt_dc_issues AS (
  SELECT    
    sku_item_nbr,
    depot_pick_confirm_dt,
    @depot AS from_bsns_unit_cd,
    SUM(cases) AS cases,
    DATE_DIFF(depot_pick_confirm_dt,@week_commence,day) AS day_rnk,
    CAST(CEIL((DATE_DIFF(depot_pick_confirm_dt,@week_commence,day)+1)/7) AS NUMERIC) AS week_rnk 
  FROM
    `DATA_INTEGRATION_PROJECT.dc_source_data_partitioned.load_sdh_dc_outbound_summary` dcs
  LEFT JOIN `GCLOUD_PROJECT.references.DWR_VNDR_ITEM` vi 
  on vi.vndr_nbr= dcs.vndr_nbr  
  and dcs.sku_item_nbr=vi.VNDR_ITEM_NBR
  WHERE
    depot_pick_confirm_dt BETWEEN @week_commence
    AND DATE_ADD(@week_commence,INTERVAL @period DAY) 
    AND vi.ACTIVE_DATE = depot_pick_confirm_dt 
    AND dcs.vndr_nbr=@vendor_nbr
    AND from_bsns_unit_cd=(CASE WHEN @depot !="all" THEN @depot ELSE from_bsns_unit_cd END)
  GROUP BY 1,2,3,5,6
  ),
  prevs_dc_issues AS(
  SELECT    
    sku_item_nbr,
    depot_pick_confirm_dt,
    @depot AS from_bsns_unit_cd,
    SUM(cases) AS cases,
    DATE_DIFF(depot_pick_confirm_dt,DATE_SUB(@week_commence,INTERVAL 7 DAY),day) AS day_rnk,
    CAST(CEIL((DATE_DIFF(depot_pick_confirm_dt,DATE_SUB(@week_commence,INTERVAL 7 DAY),day)+1)/7) AS NUMERIC) AS week_rnk 
  FROM
    `DATA_INTEGRATION_PROJECT.dc_source_data_partitioned.load_sdh_dc_outbound_summary` dcs
  LEFT JOIN `GCLOUD_PROJECT.references.DWR_VNDR_ITEM` vi 
  on vi.vndr_nbr= dcs.vndr_nbr  
  and dcs.sku_item_nbr=vi.VNDR_ITEM_NBR
  WHERE
    depot_pick_confirm_dt BETWEEN DATE_SUB(@week_commence,INTERVAL 7 DAY)
    AND DATE_ADD(DATE_SUB(@week_commence,INTERVAL 7 DAY),INTERVAL @period DAY) 
    AND dcs.vndr_nbr=@vendor_nbr
    AND vi.ACTIVE_DATE = depot_pick_confirm_dt
    AND from_bsns_unit_cd=(CASE WHEN @depot !="all" THEN @depot ELSE from_bsns_unit_cd END)
  GROUP BY 1,2,3,5,6  
    ),
  DRVNG AS(
  SELECT
    DISTINCT c.sku_item_nbr,
    t.tuc,
    dp.item_description,
    c.from_bsns_unit_cd AS R_DEPOTCODE,
    loc.location_long_name,
    c.depot_pick_confirm_dt AS R_DATE,
    CASE
      WHEN c.cases IS NULL THEN "-"
    ELSE
    CAST(c.cases AS STRING)
  END
    AS DCIssues,
    CASE
      WHEN p.cases IS NULL THEN "-"
    ELSE
    CAST(p.cases AS STRING)
  END
    AS prev_DCIssues,    
    CASE
      WHEN p.cases=0 OR p.cases IS NULL THEN "-"
    ELSE
    CONCAT(CAST(ROUND(((c.cases - p.cases)/ p.cases)*100) AS STRING)," %")
  END
    AS prev_week_index,
    SUM(c.cases) OVER (PARTITION BY c.depot_pick_confirm_dt, c.from_bsns_unit_cd ) AS totaldcissues,
    SUM(p.cases) OVER (PARTITION BY c.depot_pick_confirm_dt, c.from_bsns_unit_cd ) AS total_Prev_dcissues,
    SUM(c.cases) OVER (PARTITION BY c.from_bsns_unit_cd, c.week_rnk) AS period_totaldcissues,
    SUM(p.cases) OVER (PARTITION BY c.from_bsns_unit_cd, c.week_rnk ) AS Period_total_Prev_dcissues,
    c.week_rnk
  FROM
    curnt_dc_issues c
  LEFT JOIN
    prevs_dc_issues p
  ON
    c.sku_item_nbr=p.sku_item_nbr
    AND c.from_bsns_unit_cd=p.from_bsns_unit_cd
    AND c.day_rnk=p.day_rnk
    AND c.week_rnk=p.week_rnk
  LEFT JOIN
    `DATA_INTEGRATION_PROJECT.data_marts_dimensions.DIM_PRODUCT_CURRENT` AS dp
  ON
    c.sku_item_nbr=dp.sku_item_number
  LEFT JOIN (select item, tuc, ebs_supplier_number from `DATA_INTEGRATION_PROJECT.supply_chain_tables.Item_Supplier_TUC` qualify row_number() over (partition by ebs_supplier_number, item order by insight_platform_load_timestamp desc) = 1 ) AS t
  ON dp.sku_item_number=t.item  
  AND t.ebs_supplier_number=CAST(@vendor_nbr AS INT64)
  left join `DATA_INTEGRATION_PROJECT.data_marts_dimensions.DIM_LOCATION_CURRENT` loc 
  on c.from_bsns_unit_cd=loc.location_id ),
  avg_total_dc_issues AS(
  SELECT
    ROUND(AVG(period_totaldcissues),1) avrg_totaldcoutbound,
    CONCAT(ROUND(AVG(CASE
          WHEN Period_total_Prev_dcissues=0 OR Period_total_Prev_dcissues IS NULL THEN NULL
        ELSE
        ((period_totaldcissues-Period_total_Prev_dcissues)/Period_total_Prev_dcissues)*100
      END
        ))," %") AS avrg_total_prev_dcoutbound
  FROM (
    SELECT
      DISTINCT week_rnk,
      period_totaldcissues,
      Period_total_Prev_dcissues
    FROM
      DRVNG) )  
"""

dc_units_query="""
WITH
  curnt_dc_issues AS (
  SELECT    
    sku_item_nbr,
    depot_pick_confirm_dt,
    @depot AS from_bsns_unit_cd,
    SUM(actual_pick) AS actual_pick,
    DATE_DIFF(depot_pick_confirm_dt,@week_commence,day) AS day_rnk,
    CAST(CEIL((DATE_DIFF(depot_pick_confirm_dt,@week_commence,day)+1)/7) AS NUMERIC) AS week_rnk 
  FROM
    `DATA_INTEGRATION_PROJECT.dc_source_data_partitioned.load_sdh_dc_outbound_summary` dcs
  LEFT JOIN `GCLOUD_PROJECT.references.DWR_VNDR_ITEM` vi 
  on vi.vndr_nbr= dcs.vndr_nbr  
  and dcs.sku_item_nbr=vi.VNDR_ITEM_NBR    
  WHERE
    depot_pick_confirm_dt BETWEEN @week_commence
    AND DATE_ADD(@week_commence,INTERVAL @period DAY) 
    AND dcs.vndr_nbr=@vendor_nbr
    AND vi.ACTIVE_DATE = depot_pick_confirm_dt 
    AND from_bsns_unit_cd=(CASE WHEN @depot !="all" THEN @depot ELSE from_bsns_unit_cd END)
  GROUP BY 1,2,3,5,6
  ),
  prevs_dc_issues AS(
  SELECT    
    sku_item_nbr,
    depot_pick_confirm_dt,
    @depot AS from_bsns_unit_cd,
    SUM(actual_pick) AS actual_pick,
    DATE_DIFF(depot_pick_confirm_dt,DATE_SUB(@week_commence,INTERVAL 7 DAY),day) AS day_rnk,
    CAST(CEIL((DATE_DIFF(depot_pick_confirm_dt,DATE_SUB(@week_commence,INTERVAL 7 DAY),day)+1)/7) AS NUMERIC) AS week_rnk 
  FROM
    `DATA_INTEGRATION_PROJECT.dc_source_data_partitioned.load_sdh_dc_outbound_summary` dcs
  LEFT JOIN `GCLOUD_PROJECT.references.DWR_VNDR_ITEM` vi 
  on vi.vndr_nbr= dcs.vndr_nbr  
  and dcs.sku_item_nbr=vi.VNDR_ITEM_NBR
  WHERE
    depot_pick_confirm_dt BETWEEN DATE_SUB(@week_commence,INTERVAL 7 DAY)
    AND DATE_ADD(DATE_SUB(@week_commence,INTERVAL 7 DAY),INTERVAL @period DAY) 
    AND dcs.vndr_nbr=@vendor_nbr
    AND vi.ACTIVE_DATE = depot_pick_confirm_dt 
    AND from_bsns_unit_cd=(CASE WHEN @depot !="all" THEN @depot ELSE from_bsns_unit_cd END)
  GROUP BY 1,2,3,5,6
    ),
  DRVNG AS(
  SELECT
    DISTINCT c.sku_item_nbr,
    t.tuc,
    dp.item_description,
    c.from_bsns_unit_cd AS R_DEPOTCODE,
    loc.location_long_name,
    c.depot_pick_confirm_dt AS R_DATE,
    CASE
      WHEN c.actual_pick IS NULL THEN "-"
    ELSE
    CAST(c.actual_pick AS STRING)
  END
    AS DCIssues,
    CASE
      WHEN p.actual_pick IS NULL THEN "-"
    ELSE
    CAST(p.actual_pick AS STRING)
  END
    AS prev_DCIssues,    
    CASE
      WHEN p.actual_pick=0 OR p.actual_pick IS NULL THEN "-"
    ELSE
    CONCAT(CAST(ROUND(((c.actual_pick - p.actual_pick)/ p.actual_pick)*100) AS STRING)," %")
  END
    AS prev_week_index,
    SUM(c.actual_pick) OVER (PARTITION BY c.depot_pick_confirm_dt, c.from_bsns_unit_cd ) AS totaldcissues,
    SUM(p.actual_pick) OVER (PARTITION BY c.depot_pick_confirm_dt, c.from_bsns_unit_cd ) AS total_Prev_dcissues,
    SUM(c.actual_pick) OVER (PARTITION BY c.from_bsns_unit_cd, c.week_rnk) AS period_totaldcissues,
    SUM(p.actual_pick) OVER (PARTITION BY c.from_bsns_unit_cd, c.week_rnk ) AS Period_total_Prev_dcissues,
    c.week_rnk
  FROM
    curnt_dc_issues c
  LEFT JOIN
    prevs_dc_issues p
  ON
    c.sku_item_nbr=p.sku_item_nbr
    AND c.from_bsns_unit_cd=p.from_bsns_unit_cd
    AND c.day_rnk=p.day_rnk
    AND c.week_rnk=p.week_rnk
  LEFT JOIN
    `DATA_INTEGRATION_PROJECT.data_marts_dimensions.DIM_PRODUCT_CURRENT` AS dp
  ON
    c.sku_item_nbr=dp.sku_item_number
  LEFT JOIN (select item, tuc, ebs_supplier_number from `DATA_INTEGRATION_PROJECT.supply_chain_tables.Item_Supplier_TUC` qualify row_number() over (partition by ebs_supplier_number, item order by insight_platform_load_timestamp desc) = 1 ) AS t
  ON dp.sku_item_number=t.item 
  AND t.ebs_supplier_number=CAST(@vendor_nbr AS INT64)
  left join `DATA_INTEGRATION_PROJECT.data_marts_dimensions.DIM_LOCATION_CURRENT` loc 
  on c.from_bsns_unit_cd=loc.location_id  ),
  avg_total_dc_issues AS(
  SELECT
    ROUND(AVG(period_totaldcissues),1) avrg_totaldcoutbound,
    CONCAT(ROUND(AVG(CASE
          WHEN Period_total_Prev_dcissues=0 OR Period_total_Prev_dcissues IS NULL THEN NULL
        ELSE
        ((period_totaldcissues-Period_total_Prev_dcissues)/Period_total_Prev_dcissues)*100
      END
        ))," %") AS avrg_total_prev_dcoutbound
  FROM (
    SELECT
      DISTINCT week_rnk,
      period_totaldcissues,
      Period_total_Prev_dcissues
    FROM
      DRVNG) )
"""


dc_cases_one_day_query="""
WITH
  curnt_dc_issues AS (
  SELECT
    sku_item_nbr,
    depot_pick_confirm_dt,
    @depot AS from_bsns_unit_cd,
    SUM(cases) AS cases
  FROM
    `DATA_INTEGRATION_PROJECT.dc_source_data_partitioned.load_sdh_dc_outbound_summary` dcs
  LEFT JOIN `GCLOUD_PROJECT.references.DWR_VNDR_ITEM` vi 
  on vi.vndr_nbr= dcs.vndr_nbr  
  and dcs.sku_item_nbr=vi.VNDR_ITEM_NBR    
  WHERE
    depot_pick_confirm_dt =  DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY) 
    AND from_bsns_unit_cd=(CASE WHEN @depot !="all" THEN @depot ELSE from_bsns_unit_cd END)
    AND dcs.vndr_nbr=@vendor_nbr
    AND vi.ACTIVE_DATE = depot_pick_confirm_dt 
  GROUP BY 1,2,3
  ),
  prevs_dc_issues AS(
  SELECT
    sku_item_nbr,
    depot_pick_confirm_dt,
    @depot AS from_bsns_unit_cd,
    SUM(cases) AS cases
  FROM
    `DATA_INTEGRATION_PROJECT.dc_source_data_partitioned.load_sdh_dc_outbound_summary` dcs
  LEFT JOIN `GCLOUD_PROJECT.references.DWR_VNDR_ITEM` vi 
  on vi.vndr_nbr= dcs.vndr_nbr  
  and dcs.sku_item_nbr=vi.VNDR_ITEM_NBR
  WHERE
    depot_pick_confirm_dt = DATE_SUB(CURRENT_DATE(),INTERVAL 8 DAY) 
    AND from_bsns_unit_cd=(CASE WHEN @depot !="all" THEN @depot ELSE from_bsns_unit_cd END)
    AND dcs.vndr_nbr=@vendor_nbr
    AND vi.ACTIVE_DATE = depot_pick_confirm_dt 
  GROUP BY 1,2,3
    ),
  DRVNG AS(
  SELECT
    DISTINCT c.sku_item_nbr,
    t.tuc,
    dp.item_description,
    c.from_bsns_unit_cd AS R_DEPOTCODE,
    loc.location_long_name,
    c.depot_pick_confirm_dt AS R_DATE,
    CASE
      WHEN c.cases IS NULL THEN "-"
    ELSE
    CAST(c.cases AS STRING)
  END
    AS DCIssues,
    CASE
      WHEN p.cases IS NULL THEN "-"
    ELSE
    CAST(p.cases AS STRING)
  END
    AS prev_DCIssues,    
    CASE
      WHEN p.cases=0 OR p.cases IS NULL THEN "-"
    ELSE
    CONCAT(CAST(ROUND(((c.cases - p.cases)/ p.cases)*100) AS STRING)," %")
  END
    AS prev_week_index,
    SUM(c.cases) OVER (PARTITION BY c.depot_pick_confirm_dt)  AS totaldcissues,
    SUM(p.cases) OVER (PARTITION BY c.depot_pick_confirm_dt) AS total_Prev_dcissues
  FROM
    curnt_dc_issues c
  LEFT JOIN
    prevs_dc_issues p
  ON
    c.sku_item_nbr=p.sku_item_nbr
    AND c.from_bsns_unit_cd=p.from_bsns_unit_cd    
  LEFT JOIN
    `DATA_INTEGRATION_PROJECT.data_marts_dimensions.DIM_PRODUCT_CURRENT` AS dp
  ON
    c.sku_item_nbr=dp.sku_item_number
  LEFT JOIN (select item, tuc, ebs_supplier_number from `DATA_INTEGRATION_PROJECT.supply_chain_tables.Item_Supplier_TUC` qualify row_number() over (partition by ebs_supplier_number, item order by insight_platform_load_timestamp desc) = 1 ) AS t
  ON dp.sku_item_number=t.item  
  AND t.ebs_supplier_number=CAST(@vendor_nbr AS INT64)
  left join `DATA_INTEGRATION_PROJECT.data_marts_dimensions.DIM_LOCATION_CURRENT` loc 
  on c.from_bsns_unit_cd=loc.location_id )
"""


dc_units_one_day_query="""
WITH
  curnt_dc_issues AS (
  SELECT
    sku_item_nbr,
    depot_pick_confirm_dt,
    @depot AS from_bsns_unit_cd,
    SUM(actual_pick) AS actual_pick
  FROM
    `DATA_INTEGRATION_PROJECT.dc_source_data_partitioned.load_sdh_dc_outbound_summary` dcs
  LEFT JOIN `GCLOUD_PROJECT.references.DWR_VNDR_ITEM` vi 
  on vi.vndr_nbr= dcs.vndr_nbr  
  and dcs.sku_item_nbr=vi.VNDR_ITEM_NBR
  WHERE
    depot_pick_confirm_dt =  DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY) 
    AND from_bsns_unit_cd=(CASE WHEN @depot !="all" THEN @depot ELSE from_bsns_unit_cd END)
    AND dcs.vndr_nbr=@vendor_nbr
    AND vi.ACTIVE_DATE = depot_pick_confirm_dt 
  GROUP BY 1,2,3
  ),
  prevs_dc_issues AS(
  SELECT 
    sku_item_nbr,
    depot_pick_confirm_dt,
    @depot AS from_bsns_unit_cd,
    SUM(actual_pick) AS actual_pick
  FROM
    `DATA_INTEGRATION_PROJECT.dc_source_data_partitioned.load_sdh_dc_outbound_summary` dcs
  LEFT JOIN `GCLOUD_PROJECT.references.DWR_VNDR_ITEM` vi 
  on vi.vndr_nbr= dcs.vndr_nbr  
  and dcs.sku_item_nbr=vi.VNDR_ITEM_NBR
  WHERE
    depot_pick_confirm_dt = DATE_SUB(CURRENT_DATE(),INTERVAL 8 DAY) 
    AND from_bsns_unit_cd=(CASE WHEN @depot !="all" THEN @depot ELSE from_bsns_unit_cd END)
    AND dcs.vndr_nbr=@vendor_nbr
    AND vi.ACTIVE_DATE = depot_pick_confirm_dt 
  GROUP BY 1,2,3
    ),
  DRVNG AS(
  SELECT
    DISTINCT c.sku_item_nbr,
    t.tuc,
    dp.item_description,
    c.from_bsns_unit_cd AS R_DEPOTCODE,
    loc.location_long_name,
    c.depot_pick_confirm_dt AS R_DATE,
    CASE
      WHEN c.actual_pick IS NULL THEN "-"
    ELSE
    CAST(c.actual_pick AS STRING)
  END
    AS DCIssues,
    CASE
      WHEN p.actual_pick IS NULL THEN "-"
    ELSE
    CAST(p.actual_pick AS STRING)
  END
    AS prev_DCIssues,    
    CASE
      WHEN p.actual_pick=0 OR p.actual_pick IS NULL THEN "-"
    ELSE
    CONCAT(CAST(ROUND(((c.actual_pick - p.actual_pick)/ p.actual_pick)*100) AS STRING)," %")
  END
    AS prev_week_index,
    SUM(c.actual_pick) OVER (PARTITION BY c.depot_pick_confirm_dt)  AS totaldcissues,
    SUM(p.actual_pick) OVER (PARTITION BY c.depot_pick_confirm_dt) AS total_Prev_dcissues
  FROM
    curnt_dc_issues c
  LEFT JOIN
    prevs_dc_issues p
  ON
    c.sku_item_nbr=p.sku_item_nbr
    AND c.from_bsns_unit_cd=p.from_bsns_unit_cd    
  LEFT JOIN
    `DATA_INTEGRATION_PROJECT.data_marts_dimensions.DIM_PRODUCT_CURRENT` AS dp
  ON
    c.sku_item_nbr=dp.sku_item_number
  LEFT JOIN (select item, tuc, ebs_supplier_number from `DATA_INTEGRATION_PROJECT.supply_chain_tables.Item_Supplier_TUC` qualify row_number() over (partition by ebs_supplier_number, item order by insight_platform_load_timestamp desc) = 1 ) AS t
  ON dp.sku_item_number=t.item  
  AND t.ebs_supplier_number=CAST(@vendor_nbr AS INT64)
  left join `DATA_INTEGRATION_PROJECT.data_marts_dimensions.DIM_LOCATION_CURRENT` loc 
  on c.from_bsns_unit_cd=loc.location_id )
"""



dc_one_day_select_query="""
SELECT
  DISTINCT sku_item_nbr as product_name,
  tuc,
  item_description,
  CASE WHEN R_DEPOTCODE="all" Then "all" ELSE CONCAT(R_DEPOTCODE," - ",location_long_name) END as R_DEPOTCODE,
  R_DATE,
  DCIssues,
  prev_DCIssues,
  prev_week_index,
  CASE
    WHEN totaldcissues IS NULL THEN "-"
  ELSE
  CAST(totaldcissues AS STRING)
END
  AS totaldcissues,
  CASE
    WHEN total_Prev_dcissues IS NULL THEN "-"
  ELSE
  CAST(total_Prev_dcissues AS STRING)
END
  AS total_Prev_dcissues,
  CASE
    WHEN total_Prev_dcissues=0 OR total_Prev_dcissues IS NULL THEN "-"
  ELSE
  CONCAT(CAST(ROUND(((totaldcissues-total_Prev_dcissues)/ total_Prev_dcissues)*100) AS STRING)," %")
END
  AS total_pre_week_inx,
  CASE
    WHEN totaldcissues IS NULL THEN "-"
  ELSE
  CAST(ROUND(totaldcissues,1) AS STRING)
END AS avrg_totaldcoutbound,
  CASE
    WHEN total_Prev_dcissues=0 OR total_Prev_dcissues IS NULL THEN "-"
  ELSE
  CONCAT(CAST(ROUND(((totaldcissues-total_Prev_dcissues)/ total_Prev_dcissues)*100) AS STRING)," %")
END AS avrg_total_prev_dcoutbound
FROM
  DRVNG
"""

dc_week_select_query="""
SELECT
  DISTINCT sku_item_nbr as product_name,
  tuc,
  item_description,
  CASE WHEN R_DEPOTCODE="all" Then "all" ELSE CONCAT(R_DEPOTCODE," - ",location_long_name) END as R_DEPOTCODE,
  R_DATE,
  DCIssues,
  prev_DCIssues,
  prev_week_index,
  CASE
    WHEN totaldcissues IS NULL THEN "-"
  ELSE
  CAST(totaldcissues AS STRING)
END
  AS totaldcissues,
  CASE
    WHEN total_Prev_dcissues IS NULL THEN "-"
  ELSE
  CAST(total_Prev_dcissues AS STRING)
END
  AS total_Prev_dcissues,
  CASE
    WHEN total_Prev_dcissues=0 OR total_Prev_dcissues IS NULL THEN "-"
  ELSE
  CONCAT(CAST(ROUND(((totaldcissues-total_Prev_dcissues)/ total_Prev_dcissues)*100) AS STRING)," %")
END
  AS total_pre_week_inx,
  avrg_totaldcoutbound,
  avrg_total_prev_dcoutbound
FROM
  DRVNG,
  avg_total_dc_issues 
"""

add_filters_cases_query="""
UNION ALL
SELECT
  "Please Note - The SKUs for which there is no relevant data do not feature in this report." AS product_name,
  "" AS tuc,
  "" AS item_description,
  "" AS R_DEPOTCODE,
  "" AS R_DATE,
  "" AS DCIssues,
  "" AS prev_DCIssues,
  "" AS prev_week_index,
  "" AS totaldcissues,
  "" AS total_Prev_dcissues,
  "" AS total_pre_week_inx,
  "" AS avrg_totaldcoutbound,
  "" AS avrg_total_prev_dcoutbound
UNION ALL
SELECT
  "Supplier: SELECTED_SUPPLIER_NAME" AS product_name,
  "" AS tuc,
  "" AS item_description,
  "" AS R_DEPOTCODE,
  "" AS R_DATE,
  "" AS DCIssues,
  "" AS prev_DCIssues,
  "" AS prev_week_index,
  "" AS totaldcissues,
  "" AS total_Prev_dcissues,
  "" AS total_pre_week_inx,
  "" AS avrg_totaldcoutbound,
  "" AS avrg_total_prev_dcoutbound  
UNION ALL  
SELECT
  "ITEM_NUMBER" AS product_name,
  "TUC" AS tuc,
  "ITEM_DESCRIPTION" AS item_description,
  "DISTRIBUTION_CENTER" AS R_DEPOTCODE,
  "DATE" AS R_DATE,
  "DC_OUTBOUND_CASES " AS DCIssues,
  "PREVIOUS_WEEK_DC_OUTBOUND_CASES" AS prev_DCIssues,
  "PREVIOUS_WEEK_INDEX_PERCENT" AS prev_week_index,
  "TOTAL_DAILY_DC_OUTBOUND_CASES" AS totaldcissues,
  "TOTAL_DAILY_PREVIOUS_WEEK_DC_OUTBOUND_CASES" AS total_Prev_dcissues,
  "TOTAL_DAILY_PREVIOUS_WEEK_INDEX_PERCENT" AS total_pre_week_inx,
  "AVERAGE_REPORTING_PERIOD_DC_OUTBOUND_CASES" AS avrg_totaldcoutbound,
  "AVERAGE_REPORTING_PERIOD_PREVIOUS_WEEK_INDEX_PERCENT" AS avrg_total_prev_dcoutbound
ORDER BY
  CASE
    WHEN product_name LIKE "Please%" THEN 0
    WHEN product_name LIKE "Supplier:%" THEN 1
    WHEN product_name LIKE "ITEM%" THEN 2
  ELSE 3 END,
  product_name,
  R_DATE  
"""



add_filters_units_query="""
UNION ALL
SELECT
  "Please Note - The SKUs for which there is no relevant data do not feature in this report." AS product_name,
  "" AS tuc,
  "" AS item_description,
  "" AS R_DEPOTCODE,
  "" AS R_DATE,
  "" AS DCIssues,
  "" AS prev_DCIssues,
  "" AS prev_week_index,
  "" AS totaldcissues,
  "" AS total_Prev_dcissues,
  "" AS total_pre_week_inx,
  "" AS avrg_totaldcoutbound,
  "" AS avrg_total_prev_dcoutbound
UNION ALL  
SELECT
  "Supplier: SELECTED_SUPPLIER_NAME" AS product_name,
  "" AS tuc,
  "" AS item_description,
  "" AS R_DEPOTCODE,
  "" AS R_DATE,
  "" AS DCIssues,
  "" AS prev_DCIssues,
  "" AS prev_week_index,
  "" AS totaldcissues,
  "" AS total_Prev_dcissues,
  "" AS total_pre_week_inx,
  "" AS avrg_totaldcoutbound,
  "" AS avrg_total_prev_dcoutbound
UNION ALL  
SELECT
  "ITEM_NUMBER" AS product_name,
  "TUC" AS tuc,
  "ITEM_DESCRIPTION" AS item_description,
  "DISTRIBUTION_CENTER" AS R_DEPOTCODE,
  "DATE" AS R_DATE,
  "DC_OUTBOUND_UNITS " AS DCIssues,
  "PREVIOUS_WEEK_DC_OUTBOUND_UNITS" AS prev_DCIssues,
  "PREVIOUS_WEEK_INDEX_PERCENT" AS prev_week_index,
  "TOTAL_DAILY_DC_OUTBOUND_UNITS" AS totaldcissues,
  "TOTAL_DAILY_PREVIOUS_DC_OUTBOUND_UNITS" AS total_Prev_dcissues,
  "TOTAL_DAILY_PREVIOUS_WEEK_INDEX_PERCENT" AS total_pre_week_inx,
  "AVERAGE_REPORTING_PERIOD_DC_OUTBOUND_UNITS" AS avrg_totaldcoutbound,
  "AVERAGE_REPORTING_PERIOD_PREVIOUS_WEEK_INDEX_PERCENT" AS avrg_total_prev_dcoutbound
ORDER BY
  CASE
    WHEN product_name LIKE "Please%" THEN 0
    WHEN product_name LIKE "Supplier:%" THEN 1
    WHEN product_name LIKE "ITEM%" THEN 2
  ELSE 3 END,
  product_name,
  R_DATE 

"""

dc_count_query=""" 
SELECT
  count(DISTINCT sku_item_nbr) as total_results
FROM
  DRVNG
"""
def query_execute(query,query_total_records,req_data,export):
    print("query",query.replace("\n","\t"))    
    result = bigquery_dc_report(query,req_data,export)
    result_count = bigquery_dc_report(query_total_records,req_data,export)
    bq_result=convert_to_dict(result)
    #print("tr",convert_to_dict(result_count)[0]["total_results"])
    total_records=convert_to_dict(result_count)[0]["total_results"]
    final_result={"records":bq_result,"total_records":total_records}
    #print("in service controller",final_result)
    return final_result

def get_query(value,period_str): 
    result = {
        "case": {"day":{"query":dc_cases_one_day_query,"common_select_query":dc_one_day_select_query,"add_filters_query":add_filters_cases_query,"query_total":dc_count_query},
                 "week":{"query":dc_cases_query,"common_select_query":dc_week_select_query,"add_filters_query":add_filters_cases_query,"query_total":dc_count_query}
                },
        "units": {"day":{"query":dc_units_one_day_query,"common_select_query":dc_one_day_select_query,"add_filters_query":add_filters_units_query,"query_total":dc_count_query},
                  "week":{"query":dc_units_query,"common_select_query":dc_week_select_query,"add_filters_query":add_filters_units_query,"query_total":dc_count_query}
                 }
    }  
    return result[value][period_str] if value in result else constant.ERROR_NO_QUERY_FOUND
    
def append_query(pick_query,path,req_data):
    calc_query=pick_query["query"]    
    select_query=pick_query["common_select_query"]    
    calc_query=re.sub(r"\bDATA_INTEGRATION_PROJECT\b",get_environment_variable(constant.DATA_INTEGRATION_PROJECT),calc_query)
    calc_query=re.sub(r"\bGCLOUD_PROJECT\b",get_environment_variable(constant.GCLOUD_PROJECT),calc_query)
    if path == "/distributioncenteroutbound/download":
        query=pick_query["add_filters_query"]
        query=re.sub(r"\bSELECTED_SUPPLIER_NAME\b",req_data["supplier_name"],query)
        if req_data["period"] < 8:
            query=query.replace("AVERAGE_REPORTING_PERIOD","TOTAL_REPORTING_PERIOD",2)              
        select_query=select_query.replace(r"R_DATE","CAST(FORMAT_DATE('%d/%m/%Y', R_DATE) AS STRING) AS R_DATE",1)
        if req_data["period"]!=1:
            select_query=select_query.replace(r"avrg_totaldcoutbound","CAST(ROUND(avrg_totaldcoutbound,1) AS STRING) AS avrg_totaldcoutbound",1)
            select_query=select_query.replace(r"avrg_total_prev_dcoutbound","CAST(avrg_total_prev_dcoutbound AS STRING) AS avrg_total_prev_dcoutbound",1)
        #print("selectquery",select_query)
        select_query=select_query+query
    else:
        select_query=select_query+" where sku_item_nbr in (select distinct sku_item_nbr from DRVNG order by sku_item_nbr LIMIT @max_results OFFSET @offset) group by 1,2,3,4,5,6,7,8,9,10,11,12,13 order by product_name,R_DATE"
    query=calc_query+select_query 
    return query    

def get_handle(req_data):
        path = req_data[constant.PATH]
        value=req_data["value"]
        period_str="week"
        if req_data["period"]==1:period_str="day"    
        pick_query=get_query(value,period_str)
        if pick_query==constant.ERROR_NO_QUERY_FOUND:
            return {constant.MESSAGE : constant.ERROR_NO_QUERY_FOUND}
        query=append_query(pick_query,path,req_data)
        if path == "/dc":
            export="NA"
            query_total=re.sub(r"\bDATA_INTEGRATION_PROJECT\b",get_environment_variable(constant.DATA_INTEGRATION_PROJECT),pick_query["query"])+pick_query["query_total"]
            query_total=re.sub(r"\bGCLOUD_PROJECT\b",get_environment_variable(constant.GCLOUD_PROJECT),query_total)
            final_result = query_execute(query,query_total,req_data,export)            
            return final_result
        if path == "/distributioncenteroutbound/download":
            export="csv"
            result = bigquery_dc_report(query,req_data,export)
            final_result={"records":result,"total_records":1}
            return final_result
        return {constant.MESSAGE : constant.ERROR_INVALID_PATH}

