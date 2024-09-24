from commonutil import CommonUtil
import constant as constant

FILTER_CATEGORIES='filter_categories'
FILTER_CLASSES = 'filter_classes'
FILTER_BRANDS = 'filter_brands'
FILTER_SKUS='filter_skus'
FILTER_TUCS='filter_tucs'
FILTER_BARCODE='filter_barcode'
FILTER_FOR_DOWNLOAD='download_filter'





TUC_QUERY=u'WITH his_data as(SELECT PROMO_IND promo_h,SKU_ITEM_NBR,MIN_SKU_ITEM_NBR,DIRECT_PO_IND po_h,PRIMARY_VNDR_IND pr_h,SCHL_DLVRY_DT_KEY sc_h,ACT_DLVRY_DT_KEY actual_dt,VNDR_NBR,PCHSE_ORDR_NBR ' \
              u'FROM (SELECT DISTINCT ' \
              u'PROMO_IND,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND,PRIMARY_VNDR_IND,SCHL_DLVRY_DT_KEY,ACT_DLVRY_DT_KEY,VNDR_NBR,PCHSE_ORDR_NBR, ' \
              u'ROW_NUMBER() OVER (partition by MIN_SKU_ITEM_NBR  ORDER BY SCHL_DLVRY_DT_KEY DESC,PCHSE_ORDR_NBR DESC) rn ' \
              u'FROM ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_ITEM_STATE_GCP + '` g ' \
              u'WHERE PARSE_DATE("%Y%m%d",CAST(g.SCHL_DLVRY_DT_KEY AS STRING)) < CURRENT_DATE() and VNDR_NBR = "' + constant.VENDOR_NUMBER + '") ' \
              u'WHERE rn=1), ' \
              u'current_data as(SELECT ' \
              u'PROMO_IND promo_f,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND po_f,PRIMARY_VNDR_IND pr_f,SCHL_DLVRY_DT_KEY sc_f,VNDR_NBR,PCHSE_ORDR_NBR ' \
              u'FROM (SELECT DISTINCT '\
              u'PROMO_IND,SKU_ITEM_NBR,MIN_SKU_ITEM_NBR,DIRECT_PO_IND,PRIMARY_VNDR_IND,SCHL_DLVRY_DT_KEY,VNDR_NBR,PCHSE_ORDR_NBR, ' \
              u'ROW_NUMBER() OVER (partition by MIN_SKU_ITEM_NBR  ORDER BY SCHL_DLVRY_DT_KEY ASC,PCHSE_ORDR_NBR DESC) rn ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_ITEM_STATE_GCP + '` g ' \
              u'WHERE PARSE_DATE("%Y%m%d",CAST(g.SCHL_DLVRY_DT_KEY AS STRING)) >= CURRENT_DATE() and VNDR_NBR = "' + constant.VENDOR_NUMBER + '")  ' \
              u'WHERE rn=1), ' \
              u'gcp AS(SELECT ' \
              u'his_data.VNDR_NBR,his_data.SKU_ITEM_NBR,his_data.MIN_SKU_ITEM_NBR,his_data.po_h,his_data.pr_h,his_data.sc_h, ' \
              u'his_data.promo_h,current_data.po_f,current_data.pr_f,current_data.sc_f,current_data.promo_f, his_data.actual_dt, his_data.PCHSE_ORDR_NBR ' \
              u'FROM  his_data left outer join current_data ' \
              u'on his_data.VNDR_NBR= current_data.VNDR_NBR and his_data.SKU_ITEM_NBR=current_data.SKU_ITEM_NBR and his_data.MIN_SKU_ITEM_NBR=current_data.MIN_SKU_ITEM_NBR AND  his_data.PCHSE_ORDR_NBR=current_data.PCHSE_ORDR_NBR), ' \
              u'SKUFROMITEMSTATEGCP AS (SELECT DISTINCT CASE d.pack_indicator WHEN "Y" THEN SKU_ITEM_NBR ' \
              u'ELSE MIN_SKU_ITEM_NBR END AS MIN_SKU_ITEM_NBR FROM  `' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_SDH_VNDR_ITEM + '`,`'+ CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` d ' \
              u'WHERE vndr_nbr = ("' + constant.VENDOR_NUMBER + '")),DWR_VNDR_ITEM1  AS(SELECT DISTINCT MIN_SKU_ITEM_NBR ' \
              u'FROM SKUFROMITEMSTATEGCP UNION distinct (SELECT DISTINCT VNDR_ITEM_NBR MIN_SKU_ITEM_NBR ' \
              u'FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_SOURCE_VNDR_ITEM + '`  ' \
              u'WHERE VNDR_NBR = ("' + constant.VENDOR_NUMBER + '") AND XWMM_PRIMARY_VNDR_IND="Y")), ' \
              u'TUC_DATA as(SELECT DISTINCT ' \
              u'UPPER(tuc_barcode.TUC) tuc,UPPER(tuc_barcode.barcode) barcode ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_PRODUCT_CURRENT + '` d  ' \
              u'INNER JOIN ' \
              u' DWR_VNDR_ITEM1  t ' \
              u'ON (t.MIN_SKU_ITEM_NBR=d.sku_item_number) ' \
              u'LEFT OUTER JOIN  ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_BARCODE_TUC + '` tuc_barcode  ' \
              u'ON (tuc_barcode.sku_item_number=d.sku_item_number) ' \
              u'LEFT OUTER JOIN gcp ' \
              u'ON (CASE d.pack_indicator when "Y"  THEN gcp.SKU_ITEM_NBR else gcp.MIN_SKU_ITEM_NBR end)= d.sku_item_number '\
              u'AND gcp.VNDR_NBR = "' + constant.VENDOR_NUMBER + '"  ' \
              u'WHERE d.sku_item_number IN (' + FILTER_SKUS + ') ' \
              u'AND UPPER(IFNULL(d.commercial_category,"NULLVALUE")) IN (' + FILTER_CATEGORIES + ') ' \
              u'AND UPPER(IFNULL(d.commercial_class,"NULLVALUE")) IN (' + FILTER_CLASSES + ') ' \
              u'AND UPPER(IFNULL(d.brand_name,"NULLVALUE"))  IN (' + FILTER_BRANDS + ') ' \
              u'AND  case "' + constant.PROMOIND + '"'  \
              u'when "Y"  THEN gcp.promo_f="Y" ' \
              u'when "N" THEN gcp.promo_f="N" OR gcp.promo_f is null ' \
              u'when "" THEN gcp.promo_f is null or gcp.promo_f is not null ' \
              u'end ' \
              u'AND case "' + constant.DELIVERY + '"'  \
              u'when "Y" THEN UPPER(IFNULL(gcp.po_f, gcp.po_h))="Y" ' \
              u'when "N" THEN UPPER(IFNULL(gcp.po_f, gcp.po_h))="N" ' \
              u'when "U" THEN gcp.po_f is null and gcp.po_h is null ' \
              u'when "" THEN UPPER(IFNULL(gcp.po_f, gcp.po_h)) is null OR UPPER(IFNULL(gcp.po_f, gcp.po_h)) is not null '\
              u'end ' \
              u'AND case "' + constant.ITEM_STATUS + '"'  \
              u'WHEN "D" THEN (d.delete_flag="Y" AND d.item_status="A") '\
              u'WHEN "DC" THEN '\
              u'IF (gcp.PCHSE_ORDR_NBR IS NOT NULL,(PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))<= CURRENT_DATE()-181) AND gcp.sc_f IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N"),(d.date_discontinued IS NOT NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N"))) ' \
              u'WHEN "A" THEN IF (gcp.PCHSE_ORDR_NBR IS NOT NULL,(PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))>= CURRENT_DATE()-180 OR gcp.sc_f IS NOT NULL), (d.item_status="A" AND d.date_discontinued IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N"))) ' \
              u'WHEN "" THEN IF (gcp.PCHSE_ORDR_NBR IS NOT NULL,(((PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))<= CURRENT_DATE()-181) AND gcp.sc_f IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N")) OR (PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))>= CURRENT_DATE()-180 OR gcp.sc_f IS NOT NULL)or(gcp.sc_f is null and gcp.actual_dt is null)),(d.item_status <>"A" OR d.item_status IS NULL OR d.item_status="A") ) '\
              u'END )'\
              u'(SELECT TUC_DATA.tuc from TUC_DATA where TUC_DATA.tuc is not null) UNION DISTINCT (select TUC_DATA.barcode from TUC_DATA where TUC_DATA.barcode is not null) ' \
              u'ORDER BY UPPER(tuc) ASC'


RP_TUC_QUERY= u'WITH thisWeek AS (SELECT DATE(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY), WEEK(MONDAY)))  as monday), ' \
              u'storeStock AS (SELECT DISTINCT DATE(TRX_DATE) as stockDate FROM '\
              u'`' + constant.DB_STRING + '' + constant.TABLE_STORE_STOCK + '` ), ' \
              u'final_date AS (SELECT storeStock.stockDate FROM thisWeek,storeStock WHERE storeStock.stockDate >= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN thisWeek.monday WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 7 END) ' \
              u'AND storeStock.stockDate <= (CASE WHEN CURRENT_DATE() > thisWeek.monday THEN CURRENT_DATE() WHEN CURRENT_DATE() = thisWeek.monday THEN thisWeek.monday - 1 END) order by 1 limit 1) ' \
              u'SELECT distinct PIN_SKU_ITEM_NBR,CASE ( t.PIN_SKU_ITEM_NBR=t.SKU_ITEM_NBR) WHEN TRUE THEN tuc_barcode.barcode '\
              u'ELSE IFNULL(tuc_barcode.tuc,"-") END AS tuc '\
              u'FROM `'+ constant.DB_STRING +'' + constant.TABLE_STORE_STOCK + '`  t ' \
              u'INNER JOIN `'+ constant.DB_STRING +'' + constant.TABLE_PRODUCT_CURRENT + '`  d '  \
              u'ON (t.PIN_SKU_ITEM_NBR=d.sku_item_number) INNER JOIN '  \
              u'`'+ constant.DB_STRING +'' + constant.TABLE_BARCODE_TUC + '` tuc_barcode ' \
              u'ON (tuc_barcode.sku_item_number=t.PIN_SKU_ITEM_NBR) ' \
              u'WHERE t.VNDR_NBR = ("'+ constant.VENDOR_NUMBER +'") ' \
              u'and TRX_DATE >= (SELECT * FROM final_date) AND STORE_RANGE_IND = "Y" AND proc_plan_ind = "Y" ' \
              u'AND d.sku_item_number IN (' + FILTER_SKUS + ') ' \
              u'AND UPPER(IFNULL(d.commercial_category,"NULLVALUE")) IN (' + FILTER_CATEGORIES + ') ' \
              u'AND UPPER(IFNULL(d.commercial_class,"NULLVALUE")) IN (' + FILTER_CLASSES + ') ' \
              u'AND  UPPER(IFNULL(d.brand_name,"NULLVALUE")) IN (' + FILTER_BRANDS + ') ' \
              u'AND d.item_status ="A" ORDER BY UPPER(tuc) ASC'     

                                              


PRODUCT_DETAIL_QUERY=u'WITH his_data as(SELECT PROMO_IND promo_h,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND po_h,PRIMARY_VNDR_IND pr_h,SCHL_DLVRY_DT_KEY sc_h,ACT_DLVRY_DT_KEY actual_dt,VNDR_NBR,PCHSE_ORDR_NBR ' \
              u'FROM (SELECT  DISTINCT ' \
              u'PROMO_IND,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND,PRIMARY_VNDR_IND,SCHL_DLVRY_DT_KEY,ACT_DLVRY_DT_KEY,VNDR_NBR,PCHSE_ORDR_NBR, ' \
              u'ROW_NUMBER() OVER (partition by MIN_SKU_ITEM_NBR  ORDER BY SCHL_DLVRY_DT_KEY DESC,PCHSE_ORDR_NBR DESC) rn ' \
              u'FROM ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_ITEM_STATE_GCP + '` g ' \
              u'WHERE  PARSE_DATE("%Y%m%d", CAST(g.SCHL_DLVRY_DT_KEY AS STRING)) < CURRENT_DATE() and VNDR_NBR = "' + constant.VENDOR_NUMBER + '") ' \
              u'WHERE rn=1), ' \
              u'current_data as(SELECT ' \
              u'PROMO_IND promo_f,SKU_ITEM_NBR,MIN_SKU_ITEM_NBR,DIRECT_PO_IND po_f,PRIMARY_VNDR_IND pr_f,SCHL_DLVRY_DT_KEY sc_f,VNDR_NBR,PCHSE_ORDR_NBR ' \
              u'FROM (SELECT DISTINCT '\
              u'PROMO_IND,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND,PRIMARY_VNDR_IND,SCHL_DLVRY_DT_KEY,VNDR_NBR,PCHSE_ORDR_NBR, ' \
              u'ROW_NUMBER() OVER (partition by MIN_SKU_ITEM_NBR  ORDER BY SCHL_DLVRY_DT_KEY ASC,PCHSE_ORDR_NBR DESC) rn ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_ITEM_STATE_GCP + '` g ' \
              u'WHERE PARSE_DATE("%Y%m%d", CAST(g.SCHL_DLVRY_DT_KEY AS STRING)) >= CURRENT_DATE() and  VNDR_NBR = "' + constant.VENDOR_NUMBER + '")  ' \
              u'WHERE rn=1), ' \
              u'gcp AS(SELECT ' \
              u'his_data.VNDR_NBR,his_data.SKU_ITEM_NBR,his_data.MIN_SKU_ITEM_NBR,his_data.po_h,his_data.pr_h,his_data.sc_h, ' \
              u'his_data.promo_h,current_data.po_f,current_data.pr_f,current_data.sc_f,current_data.promo_f, his_data.actual_dt, his_data.PCHSE_ORDR_NBR ' \
              u'FROM  his_data left outer join current_data ' \
              u'on his_data.VNDR_NBR= current_data.VNDR_NBR and  his_data.SKU_ITEM_NBR=current_data.SKU_ITEM_NBR and his_data.MIN_SKU_ITEM_NBR=current_data.MIN_SKU_ITEM_NBR AND  his_data.PCHSE_ORDR_NBR=current_data.PCHSE_ORDR_NBR), ' \
              u'vendor AS( ' \
              u'SELECT TI,HI,VNDR_ITEM_NBR,VNDR_NBR,XWMM_PRIMARY_VNDR_IND FROM ' \
              u'(SELECT DISTINCT ' \
              u'v.TI,v.HI,v.VNDR_ITEM_NBR,v.VNDR_NBR,v.XWMM_PRIMARY_VNDR_IND, ' \
              u'ROW_NUMBER()OVER (PARTITION BY VNDR_ITEM_NBR ORDER BY  EFF_FROM_DT DESC) rn ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_SOURCE_VNDR_ITEM + '` v  ' \
              u'where v.CURR_IND="Y" and v.XWMM_PRIMARY_VNDR_IND="Y" and v.VNDR_NBR = "' + constant.VENDOR_NUMBER + '" ) where rn=1), '   \
              u'pack AS ( ' \
              u'SELECT ' \
              u'pin,MIN,pack_size ' \
              u'FROM ( SELECT ' \
              u'sku_item_nbr pin,mbr_sku_item_nbr MIN,perassembly_cnt pack_size, ' \
              u'ROW_NUMBER() OVER (PARTITION BY sku_item_nbr ORDER BY sku_item_collctn_key DESC) rn ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_ITEM_COLLCTN + '` ' \
              u'WHERE curr_ind = "Y" )' \
              u'WHERE rn = 1 ), ' \
              u'SKUFROMITEMSTATEGCP AS (SELECT DISTINCT CASE d.pack_indicator WHEN "Y" THEN SKU_ITEM_NBR ' \
              u'ELSE MIN_SKU_ITEM_NBR  END AS MIN_SKU_ITEM_NBR FROM  `' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_SDH_VNDR_ITEM + '`,`'+ CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` d ' \
              u'WHERE  vndr_nbr = ("' + constant.VENDOR_NUMBER + '")),DWR_VNDR_ITEM1 AS(SELECT DISTINCT MIN_SKU_ITEM_NBR ' \
              u'FROM SKUFROMITEMSTATEGCP UNION  distinct (SELECT DISTINCT VNDR_ITEM_NBR MIN_SKU_ITEM_NBR ' \
              u'FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_SOURCE_VNDR_ITEM + '` ' \
              u' WHERE  VNDR_NBR = ("' + constant.VENDOR_NUMBER + '") AND XWMM_PRIMARY_VNDR_IND="Y")) ' \
              u'SELECT DISTINCT ' \
              u'd.sku_item_number itemNumber,tuc_barcode.TUC tuc,d.item_description itemDescription,' \
              u'IF(gcp.PCHSE_ORDR_NBR IS NOT NULL, (IF(PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))>= CURRENT_DATE()-180 OR gcp.sc_f IS NOT NULL,"APPROVED", ' \
              u'IF ((PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))<= CURRENT_DATE()-181 ) AND gcp.sc_f IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N"),"DISCONTINUED", ' \
              u'IF ((d.delete_flag="Y" AND d.item_status="A"),"DELETED","-" )))),(CASE d.item_status ' \
              u' WHEN "A" THEN(CASE '\
              u'WHEN d.delete_flag="Y" THEN "DELETED" '\
              u'WHEN d.date_discontinued IS NOT NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N") THEN "DISCONTINUED" '\
              u'WHEN d.date_discontinued IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N") THEN "APPROVED" END ) ELSE  "-" END '\
              u' ))AS itemStatus, ' \
              u'CASE (IFNULL(gcp.promo_f,"N")) ' \
              u'WHEN "Y" THEN "YES" ' \
              u'WHEN "N" THEN "NO" ' \
              u'ELSE "-" ' \
              u'END onPromotion, ' \
              u'UPPER((IFNULL(d.commercial_division,"-"))) AS division, ' \
              u'UPPER((IFNULL(d.commercial_trading_group,"-"))) AS itemGroup, ' \
              u'UPPER((IFNULL(d.commercial_category,"-"))) AS category, ' \
              u'UPPER((IFNULL(d.commercial_department,"-"))) AS department, ' \
              u'UPPER((IFNULL(d.commercial_class,"-"))) AS subCategory, ' \
              u'UPPER((IFNULL(d.brand_name,"-"))) AS brand, ' \
              u'CAST(ROUND(IFNULL(p.pack_size,0)) AS INT64) AS packSize, '  \
              u'(IFNULL(d.min_acceptable_product_life,"-")) AS minimumAcceptableProductLife, '\
              u'(IFNULL(d.country_of_origin,"-")) AS origin, ' \
              u'CASE (IFNULL(gcp.po_f,gcp.po_h)) ' \
              u' WHEN "Y" THEN "DIRECT TO STORE" ' \
              u' WHEN "N" THEN "D.C." ' \
              u'ELSE "UNKNOWN" ' \
              u'END AS poInd, ' \
              u'CASE ((IFNULL(d.MEASUREMENT_UOM,"NULLVALUE"))) ' \
              u' WHEN "CM" THEN ROUND(CAST(d.hight AS FLOAT64),2) ' \
              u'WHEN "M" THEN (ROUND(CAST(d.hight AS FLOAT64)*100,2)) ' \
              u'ELSE  ((IFNULL(ROUND(CAST(d.hight AS FLOAT64),2),0.0))) ' \
              u'END AS height,' \
              u'CASE ((IFNULL(d.weight_uom,"NULLVALUE"))) ' \
              u' WHEN "KG" THEN ROUND(IFNULL(d.gross_weight,d.net_weight),2) ' \
              u'WHEN "G" THEN ROUND(IFNULL(d.gross_weight/100,d.net_weight/100),2) ' \
              u'ELSE  (IFNULL(ROUND(IFNULL(d.gross_weight,d.net_weight),2),0.0))  ' \
              u'END AS weight,' \
              u'CASE (IFNULL(gcp.pr_f,gcp.pr_h)) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.packs_per_pallet,0)) AS int64) ' \
              u' WHEN "N" THEN CAST(ROUND(IFNULL(vendor.TI*vendor.HI,0)) AS int64) ' \
              u'ELSE CASE (IFNULL(vendor.XWMM_PRIMARY_VNDR_IND,"NULLVALUE")) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.packs_per_pallet,0)) AS int64) ' \
              u'WHEN "N" THEN CAST(ROUND(IFNULL(vendor.TI*vendor.HI,0)) AS int64) ' \
              u'ELSE  0 '  \
              u'END ' \
              u'END AS packsPerPallet, ' \
              u'CASE (IFNULL(gcp.pr_f,gcp.pr_h)) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.ti_qty,0)) AS int64) ' \
              u' WHEN "N" THEN CAST(ROUND(IFNULL(vendor.TI,0)) AS int64) ' \
              u'ELSE CASE (IFNULL(vendor.XWMM_PRIMARY_VNDR_IND,"NULLVALUE")) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.ti_qty,0)) AS int64) ' \
              u'WHEN "N" THEN  CAST(ROUND(IFNULL(vendor.TI,0)) AS int64) ' \
              u'ELSE 0 '  \
              u'END ' \
              u'END AS packsPerLayer, ' \
              u'tuc_barcode.barcode AS barCode, ' \
              u'"-" as minimumOrder '\
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_PRODUCT_CURRENT + '` d  ' \
              u'INNER JOIN ' \
              u'DWR_VNDR_ITEM1  t ' \
              u'ON (t.MIN_SKU_ITEM_NBR=d.sku_item_number) ' \
              u'LEFT OUTER JOIN gcp ' \
              u'ON  (CASE d.pack_indicator WHEN "Y" THEN gcp.SKU_ITEM_NBR ELSE gcp.MIN_SKU_ITEM_NBR END )= d.sku_item_number ' \
              u'AND gcp.VNDR_NBR = "' + constant.VENDOR_NUMBER + '" ' \
              u'LEFT OUTER JOIN vendor ' \
              u'ON vendor.VNDR_ITEM_NBR=d.sku_item_number ' \
              u'AND vendor.VNDR_NBR = "' + constant.VENDOR_NUMBER + '" ' \
              u'LEFT OUTER JOIN pack p ' \
              u'ON (p.pin=d.sku_item_number) ' \
              u'LEFT OUTER JOIN  ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_BARCODE_TUC + '` tuc_barcode  ' \
              u'ON (tuc_barcode.sku_item_number=d.sku_item_number) ' \
              u'WHERE  d.sku_item_number IN (' + FILTER_SKUS + ') ' \
              u'AND (UPPER(IFNULL(tuc_barcode.TUC,"NULLVALUE")) IN (' + FILTER_TUCS + ') ' \
              u'OR UPPER(IFNULL(tuc_barcode.barcode,"NULLVALUE")) IN (' + FILTER_BARCODE + ')) ' \
              u'AND UPPER(IFNULL(d.commercial_category,"NULLVALUE")) IN (' + FILTER_CATEGORIES + ') ' \
              u'AND  UPPER(IFNULL(d.commercial_class,"NULLVALUE")) IN (' + FILTER_CLASSES + ') ' \
              u'AND UPPER(IFNULL(d.brand_name,"NULLVALUE")) IN  (' + FILTER_BRANDS + ') ' \
              u'AND case "' + constant.PROMOIND + '" '  \
              u'when "Y"  THEN gcp.promo_f="Y" ' \
              u'when "N" THEN gcp.promo_f="N" OR gcp.promo_f is null ' \
              u'when "" THEN gcp.promo_f is null or gcp.promo_f is not null ' \
              u'end ' \
               u'AND case "' + constant.DELIVERY + '" '  \
              u'when "Y" THEN UPPER(IFNULL(gcp.po_f, gcp.po_h))="Y" ' \
              u'when "N" THEN UPPER(IFNULL(gcp.po_f, gcp.po_h))="N" ' \
              u'when "U" THEN gcp.po_f is null and gcp.po_h is null ' \
              u'when "" THEN UPPER(IFNULL(gcp.po_f, gcp.po_h)) is null OR UPPER(IFNULL(gcp.po_f, gcp.po_h)) is not null ' \
              u'end ' \
              u'AND case "' + constant.ITEM_STATUS + '" '  \
              u'WHEN "D" THEN (d.delete_flag="Y" AND d.item_status="A") '\
              u'WHEN "DC" THEN '\
              u'IF (gcp.PCHSE_ORDR_NBR IS NOT NULL,(PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))<= CURRENT_DATE()-181) AND gcp.sc_f IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N"),(d.date_discontinued IS NOT NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N"))) ' \
              u'WHEN "A" THEN IF (gcp.PCHSE_ORDR_NBR IS NOT NULL,(PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))>= CURRENT_DATE()-180 OR gcp.sc_f IS NOT NULL), (d.item_status="A" AND d.date_discontinued IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N"))) ' \
              u'WHEN "" THEN IF (gcp.PCHSE_ORDR_NBR IS NOT NULL,(((PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))<= CURRENT_DATE()-181) AND gcp.sc_f IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N")) OR (PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))>= CURRENT_DATE()-180 OR gcp.sc_f IS NOT NULL)or(gcp.sc_f is null and gcp.actual_dt is null)),(d.item_status <>"A" OR d.item_status IS NULL OR d.item_status="A") ) '\
              u'END '\
              u'ORDER BY '+ constant.SORT_DETAIL + ''


PRODUCT_DETAIL_DOWNLOAD_QUERY=u'WITH his_data as(SELECT PROMO_IND promo_h,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND po_h,PRIMARY_VNDR_IND pr_h,SCHL_DLVRY_DT_KEY sc_h,ACT_DLVRY_DT_KEY actual_dt,VNDR_NBR,PCHSE_ORDR_NBR ' \
              u'FROM (SELECT DISTINCT ' \
              u'PROMO_IND,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND,PRIMARY_VNDR_IND,SCHL_DLVRY_DT_KEY,ACT_DLVRY_DT_KEY,VNDR_NBR,PCHSE_ORDR_NBR, ' \
              u'ROW_NUMBER() OVER (partition by MIN_SKU_ITEM_NBR ORDER BY SCHL_DLVRY_DT_KEY DESC,PCHSE_ORDR_NBR DESC) rn ' \
              u'FROM ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_ITEM_STATE_GCP + '` g ' \
              u'WHERE PARSE_DATE("%Y%m%d", CAST(g.SCHL_DLVRY_DT_KEY AS STRING)) < CURRENT_DATE() and  VNDR_NBR = "' + constant.VENDOR_NUMBER + '") ' \
              u'WHERE rn=1), ' \
              u'current_data as(SELECT ' \
              u'PROMO_IND promo_f,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND po_f,PRIMARY_VNDR_IND pr_f,SCHL_DLVRY_DT_KEY sc_f,VNDR_NBR,PCHSE_ORDR_NBR ' \
              u'FROM (SELECT DISTINCT '\
              u'PROMO_IND,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND,PRIMARY_VNDR_IND,SCHL_DLVRY_DT_KEY,VNDR_NBR,PCHSE_ORDR_NBR, ' \
              u'ROW_NUMBER() OVER (partition by MIN_SKU_ITEM_NBR ORDER BY SCHL_DLVRY_DT_KEY ASC,PCHSE_ORDR_NBR DESC) rn ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_ITEM_STATE_GCP + '` g ' \
              u'WHERE PARSE_DATE("%Y%m%d", CAST(g.SCHL_DLVRY_DT_KEY AS STRING)) >= CURRENT_DATE() and VNDR_NBR = "' + constant.VENDOR_NUMBER + '")  ' \
              u'WHERE rn=1), ' \
              u'gcp AS(SELECT ' \
              u'his_data.VNDR_NBR,his_data.SKU_ITEM_NBR,his_data.MIN_SKU_ITEM_NBR,his_data.po_h,his_data.pr_h,his_data.sc_h, ' \
              u'his_data.promo_h,current_data.po_f,current_data.pr_f,current_data.sc_f,current_data.promo_f, his_data.actual_dt, his_data.PCHSE_ORDR_NBR ' \
              u'FROM  his_data left outer join current_data ' \
              u'on  his_data.VNDR_NBR= current_data.VNDR_NBR and his_data.SKU_ITEM_NBR=current_data.SKU_ITEM_NBR and his_data.MIN_SKU_ITEM_NBR=current_data.MIN_SKU_ITEM_NBR AND  his_data.PCHSE_ORDR_NBR=current_data.PCHSE_ORDR_NBR), ' \
              u'vendor AS( ' \
              u'SELECT TI,HI,VNDR_ITEM_NBR,VNDR_NBR,XWMM_PRIMARY_VNDR_IND FROM ' \
              u'(SELECT DISTINCT ' \
              u'v.TI,v.HI,v.VNDR_ITEM_NBR,v.VNDR_NBR,v.XWMM_PRIMARY_VNDR_IND, ' \
              u'ROW_NUMBER()OVER (PARTITION BY VNDR_ITEM_NBR ORDER BY  EFF_FROM_DT DESC) rn ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_SOURCE_VNDR_ITEM + '` v  ' \
              u'where v.CURR_IND="Y" and v.XWMM_PRIMARY_VNDR_IND="Y" and  v.VNDR_NBR = "' + constant.VENDOR_NUMBER + '"  ) where rn=1), '   \
              u'pack AS ( ' \
              u'SELECT ' \
              u'pin,MIN,pack_size ' \
              u'FROM ( SELECT ' \
              u'sku_item_nbr pin,mbr_sku_item_nbr MIN, perassembly_cnt pack_size, ' \
              u'ROW_NUMBER() OVER (PARTITION BY sku_item_nbr ORDER BY sku_item_collctn_key DESC) rn ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_ITEM_COLLCTN + '` ' \
              u'WHERE curr_ind = "Y" ) ' \
              u'WHERE rn = 1 ), ' \
              u'SKUFROMITEMSTATEGCP AS (SELECT DISTINCT CASE d.pack_indicator WHEN "Y" THEN SKU_ITEM_NBR ' \
              u'ELSE MIN_SKU_ITEM_NBR END AS MIN_SKU_ITEM_NBR FROM  `' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_SDH_VNDR_ITEM + '`,`'+ CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` d ' \
              u' WHERE vndr_nbr = ("' + constant.VENDOR_NUMBER + '")),DWR_VNDR_ITEM1 AS(SELECT DISTINCT MIN_SKU_ITEM_NBR ' \
              u'FROM SKUFROMITEMSTATEGCP UNION distinct (SELECT  DISTINCT VNDR_ITEM_NBR MIN_SKU_ITEM_NBR ' \
              u'FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_SOURCE_VNDR_ITEM + '` ' \
              u' WHERE VNDR_NBR = ("' + constant.VENDOR_NUMBER + '") AND XWMM_PRIMARY_VNDR_IND="Y")) ' \
              u'SELECT DISTINCT ' \
              u'd.sku_item_number ITEMNUMBER,temp.tuc as TUC,d.item_description ITEMDESCRIPTION,' \
              u'IF(gcp.PCHSE_ORDR_NBR IS NOT NULL, (IF(PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))>= CURRENT_DATE()-180 OR gcp.sc_f IS NOT NULL,"APPROVED", ' \
              u'IF ((PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))<= CURRENT_DATE()-181 ) AND gcp.sc_f IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N"),"DISCONTINUED", ' \
              u'IF ((d.delete_flag="Y" AND d.item_status="A"),"DELETED","-" )))),(CASE d.item_status ' \
              u' WHEN "A" THEN(CASE '\
              u'WHEN d.delete_flag="Y" THEN "DELETED" '\
              u'WHEN d.date_discontinued IS NOT NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N") THEN "DISCONTINUED" '\
              u'WHEN d.date_discontinued IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N") THEN "APPROVED" END ) ELSE  "-" END '\
              u')) AS ITEMSTATUS, ' \
              u'CASE (IFNULL(gcp.promo_f,"N")) ' \
              u'WHEN "Y" THEN "YES" ' \
              u'WHEN "N" THEN "NO" ' \
              u'ELSE "-" ' \
              u'END ONPROMOTION, ' \
              u'UPPER((IFNULL(d.commercial_division,"-"))) AS DIVISION, ' \
              u'UPPER((IFNULL(d.commercial_trading_group,"-"))) AS ITEMGROUP, ' \
              u'UPPER((IFNULL(d.commercial_category,"-"))) AS CATEGORY, ' \
              u'UPPER((IFNULL(d.commercial_department,"-"))) AS DEPARTMENT, ' \
              u'UPPER((IFNULL(d.commercial_class,"-"))) AS SUBCATEGORY, ' \
              u'UPPER((IFNULL(d.brand_name,"-"))) AS BRAND, ' \
              u'CAST(ROUND(IFNULL(p.pack_size,0)) AS INT64) AS PACKSIZE, '  \
              u'(IFNULL(d.min_acceptable_product_life,"-")) AS MINIMUMACCEPTABLEPRODUCTLIFE, '\
              u'(IFNULL(d.country_of_origin,"-")) AS ORIGIN, ' \
              u'CASE (IFNULL(gcp.po_f,gcp.po_h)) ' \
              u' WHEN "Y" THEN "DIRECT TO STORE" ' \
              u' WHEN "N" THEN "D.C." ' \
              u'ELSE "UNKNOWN" ' \
              u'END AS POIND, ' \
              u'CASE (IFNULL(gcp.pr_f,gcp.pr_h)) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.packs_per_pallet,0)) AS int64) ' \
              u' WHEN "N" THEN CAST(ROUND(IFNULL(vendor.TI*vendor.HI,0)) AS int64) ' \
              u'ELSE CASE (IFNULL(vendor.XWMM_PRIMARY_VNDR_IND,"NULLVALUE")) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.packs_per_pallet,0)) AS int64) ' \
              u'WHEN "N" THEN CAST(ROUND(IFNULL(vendor.TI*vendor.HI,0)) AS int64) ' \
              u'ELSE 0 '  \
              u'END ' \
              u'END AS PACKSPERPALLET, ' \
              u'temp.barcode AS BARCODE, ' \
              u'CASE ((IFNULL(d.MEASUREMENT_UOM,"NULLVALUE"))) ' \
              u' WHEN "CM" THEN ROUND(CAST(d.hight AS FLOAT64),2) ' \
              u'WHEN "M" THEN (ROUND(CAST(d.hight AS FLOAT64)*100,2)) ' \
              u'ELSE  ((IFNULL(ROUND(CAST(d.hight AS FLOAT64),2),0.0))) ' \
              u'END AS HEIGHT,' \
              u'CASE ((IFNULL(d.weight_uom,"NULLVALUE"))) ' \
              u' WHEN "KG" THEN ROUND(IFNULL(d.gross_weight,d.net_weight),2) ' \
              u'WHEN "G" THEN ROUND(IFNULL(d.gross_weight/100,d.net_weight/100),2) ' \
              u'ELSE  (IFNULL(ROUND(IFNULL(d.gross_weight,d.net_weight),2),0.0))  ' \
              u'END AS WEIGHT,' \
              u'CASE (IFNULL(gcp.pr_f,gcp.pr_h)) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.ti_qty,0)) AS int64) ' \
              u' WHEN "N" THEN CAST(ROUND(IFNULL(vendor.TI,0)) AS int64) ' \
              u'ELSE CASE (IFNULL(vendor.XWMM_PRIMARY_VNDR_IND,"NULLVALUE")) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.ti_qty,0)) AS int64) ' \
              u'WHEN "N" THEN  CAST(ROUND(IFNULL(vendor.TI,0)) AS int64) ' \
              u'ELSE 0 '  \
              u'END ' \
              u'END AS PACKSPERLAYER, ' \
              u'"-" as MINIMUMORDER '\
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_PRODUCT_CURRENT + '` d  ' \
              u'INNER JOIN ' \
              u'DWR_VNDR_ITEM1  t ' \
              u'ON (t.MIN_SKU_ITEM_NBR=d.sku_item_number) ' \
              u'LEFT OUTER JOIN gcp ' \
              u'ON  (CASE d.pack_indicator WHEN "Y" THEN gcp.SKU_ITEM_NBR ELSE gcp.MIN_SKU_ITEM_NBR END )= d.sku_item_number ' \
              u' and gcp.VNDR_NBR = "' + constant.VENDOR_NUMBER + '" ' \
              u'LEFT OUTER JOIN vendor ' \
              u'ON vendor.VNDR_ITEM_NBR=d.sku_item_number ' \
              u'AND vendor.VNDR_NBR ="' + constant.VENDOR_NUMBER + '" ' \
              u'LEFT OUTER JOIN pack p ' \
              u'ON (p.pin=d.sku_item_number) ' \
              u'LEFT OUTER JOIN '  \
			  u'(select barcode,tuc,sku_item_number as sku_nbr from `' +  constant.DB_STRING + '' + constant.TABLE_BARCODE_TUC + '`) temp  ON (d.sku_item_number=temp.sku_nbr) '  \
              u'WHERE  d.sku_item_number IN (' + FILTER_SKUS + ') ' \
              u'AND (UPPER(IFNULL(temp.TUC,"NULLVALUE")) IN (' + FILTER_TUCS + ') ' \
              u'OR UPPER(IFNULL(temp.barcode,"NULLVALUE")) IN (' + FILTER_BARCODE + ')) ' \
              u'AND UPPER(IFNULL(d.commercial_category,"NULLVALUE")) IN (' + FILTER_CATEGORIES + ') ' \
              u'AND  UPPER(IFNULL(d.commercial_class,"NULLVALUE")) IN (' + FILTER_CLASSES + ') ' \
              u'AND UPPER(IFNULL(d.brand_name,"NULLVALUE")) IN (' + FILTER_BRANDS + ') ' \
               u'AND case "' + constant.PROMOIND + '" '  \
              u'when "Y"  THEN gcp.promo_f="Y" ' \
              u'when "N" THEN gcp.promo_f="N" OR gcp.promo_f is null ' \
              u'when "" THEN gcp.promo_f is null or gcp.promo_f is not null ' \
              u'end ' \
              u'AND case "' + constant.DELIVERY + '" '  \
              u'when "Y" THEN UPPER(IFNULL(gcp.po_f, gcp.po_h))="Y" ' \
              u'when "N" THEN UPPER(IFNULL(gcp.po_f, gcp.po_h))="N" ' \
              u'when "U" THEN gcp.po_f is null and gcp.po_h is null ' \
              u'when "" THEN UPPER(IFNULL(gcp.po_f, gcp.po_h)) is null OR UPPER(IFNULL(gcp.po_f, gcp.po_h)) is not null '\
              u'end ' \
              u'AND case "' + constant.ITEM_STATUS + '" '  \
              u'WHEN "D" THEN (d.delete_flag="Y" AND d.item_status="A") '\
              u'WHEN "DC" THEN '\
              u'IF (gcp.PCHSE_ORDR_NBR IS NOT NULL,(PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))<= CURRENT_DATE()-181) AND gcp.sc_f IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N"),(d.date_discontinued IS NOT NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N"))) ' \
              u'WHEN "A" THEN IF (gcp.PCHSE_ORDR_NBR IS NOT NULL,(PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))>= CURRENT_DATE()-180 OR gcp.sc_f IS NOT NULL), (d.item_status="A" AND d.date_discontinued IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N"))) ' \
              u'WHEN "" THEN IF (gcp.PCHSE_ORDR_NBR IS NOT NULL,(((PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))<= CURRENT_DATE()-181) AND gcp.sc_f IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N")) OR (PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))>= CURRENT_DATE()-180 OR gcp.sc_f IS NOT NULL)or(gcp.sc_f is null and gcp.actual_dt is null)),(d.item_status <>"A" OR d.item_status IS NULL OR d.item_status="A") ) '\
              u'END '\
              u'ORDER BY UPPER(itemNumber) ASC'

PRODUCT_DETAIL_DOWNLOAD_QUERY_ALL=u'WITH his_data as(SELECT PROMO_IND promo_h,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND po_h,PRIMARY_VNDR_IND pr_h,SCHL_DLVRY_DT_KEY sc_h,ACT_DLVRY_DT_KEY actual_dt,VNDR_NBR,PCHSE_ORDR_NBR ' \
              u'FROM (SELECT DISTINCT ' \
              u'PROMO_IND,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND,PRIMARY_VNDR_IND,SCHL_DLVRY_DT_KEY,ACT_DLVRY_DT_KEY,VNDR_NBR,PCHSE_ORDR_NBR, ' \
              u'ROW_NUMBER() OVER (partition by MIN_SKU_ITEM_NBR  ORDER BY  SCHL_DLVRY_DT_KEY DESC,PCHSE_ORDR_NBR DESC) rn ' \
              u'FROM ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_ITEM_STATE_GCP + '` g ' \
              u'WHERE  PARSE_DATE("%Y%m%d", CAST(g.SCHL_DLVRY_DT_KEY AS STRING)) < CURRENT_DATE() and VNDR_NBR = "' + constant.VENDOR_NUMBER + '") ' \
              u'WHERE rn=1), ' \
              u'current_data as(SELECT ' \
              u'PROMO_IND promo_f,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND po_f,PRIMARY_VNDR_IND pr_f,SCHL_DLVRY_DT_KEY sc_f,VNDR_NBR,PCHSE_ORDR_NBR ' \
              u'FROM (SELECT  DISTINCT '\
              u'PROMO_IND,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND,PRIMARY_VNDR_IND,SCHL_DLVRY_DT_KEY,VNDR_NBR,PCHSE_ORDR_NBR, ' \
              u'ROW_NUMBER() OVER (partition by MIN_SKU_ITEM_NBR  ORDER BY SCHL_DLVRY_DT_KEY ASC,PCHSE_ORDR_NBR DESC) rn ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_ITEM_STATE_GCP + '` g ' \
              u'WHERE PARSE_DATE("%Y%m%d", CAST(g.SCHL_DLVRY_DT_KEY AS STRING)) >= CURRENT_DATE()  and VNDR_NBR = "' + constant.VENDOR_NUMBER + '")  ' \
              u'WHERE rn=1), ' \
              u'gcp AS(SELECT ' \
              u'his_data.VNDR_NBR,his_data.SKU_ITEM_NBR,his_data.MIN_SKU_ITEM_NBR,his_data.po_h,his_data.pr_h,his_data.sc_h, ' \
              u'his_data.promo_h,current_data.po_f,current_data.pr_f,current_data.sc_f,current_data.promo_f, his_data.actual_dt, his_data.PCHSE_ORDR_NBR ' \
              u'FROM  his_data left outer join current_data ' \
              u'on his_data.VNDR_NBR= current_data.VNDR_NBR and his_data.SKU_ITEM_NBR=current_data.SKU_ITEM_NBR and his_data.MIN_SKU_ITEM_NBR=current_data.MIN_SKU_ITEM_NBR AND  his_data.PCHSE_ORDR_NBR=current_data.PCHSE_ORDR_NBR), ' \
              u'vendor  AS( ' \
              u'SELECT TI,HI,VNDR_ITEM_NBR,VNDR_NBR,XWMM_PRIMARY_VNDR_IND FROM ' \
              u'(SELECT DISTINCT ' \
              u'v.TI,v.HI,v.VNDR_ITEM_NBR,v.VNDR_NBR,v.XWMM_PRIMARY_VNDR_IND, ' \
              u'ROW_NUMBER()OVER (PARTITION BY VNDR_ITEM_NBR ORDER BY  EFF_FROM_DT DESC) rn ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_SOURCE_VNDR_ITEM + '` v  ' \
              u'where v.CURR_IND="Y" and v.XWMM_PRIMARY_VNDR_IND="Y"  and v.VNDR_NBR = "' + constant.VENDOR_NUMBER + '"  ) where rn=1), '   \
              u'pack AS ( ' \
              u'SELECT ' \
              u'pin,MIN,pack_size ' \
              u'FROM ( SELECT ' \
              u'sku_item_nbr pin,mbr_sku_item_nbr MIN,perassembly_cnt pack_size, ' \
              u'ROW_NUMBER() OVER (PARTITION BY sku_item_nbr ORDER BY sku_item_collctn_key DESC) rn ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_ITEM_COLLCTN + '` ' \
              u'WHERE curr_ind = "Y" )' \
              u'WHERE rn = 1 ), ' \
              u'SKUFROMITEMSTATEGCP AS (SELECT DISTINCT CASE d.pack_indicator WHEN "Y" THEN SKU_ITEM_NBR ' \
              u'ELSE MIN_SKU_ITEM_NBR END AS MIN_SKU_ITEM_NBR FROM  `' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_SDH_VNDR_ITEM + '`,`'+ CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` d ' \
              u'WHERE vndr_nbr =  ("' + constant.VENDOR_NUMBER + '")),DWR_VNDR_ITEM1 AS(SELECT DISTINCT  MIN_SKU_ITEM_NBR ' \
              u'FROM SKUFROMITEMSTATEGCP UNION distinct (SELECT DISTINCT VNDR_ITEM_NBR MIN_SKU_ITEM_NBR ' \
              u'FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_SOURCE_VNDR_ITEM + '` ' \
              u'WHERE VNDR_NBR = ("' + constant.VENDOR_NUMBER + '") AND XWMM_PRIMARY_VNDR_IND="Y")) ' \
              u'SELECT DISTINCT ' \
              u'd.sku_item_number ITEMNUMBER,temp.tuc as TUC,d.item_description ITEMDESCRIPTION,' \
              u'IF(gcp.PCHSE_ORDR_NBR IS NOT NULL, (IF(PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))>= CURRENT_DATE()-180 OR gcp.sc_f IS NOT NULL,"APPROVED", ' \
              u'IF ((PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))<= CURRENT_DATE()-181 ) AND gcp.sc_f IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N"),"DISCONTINUED", ' \
              u'IF ((d.delete_flag="Y" AND d.item_status="A"),"DELETED","-" )))),(CASE d.item_status ' \
              u' WHEN "A" THEN(CASE '\
              u'WHEN d.delete_flag="Y" THEN "DELETED" '\
              u'WHEN d.date_discontinued IS NOT NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N") THEN "DISCONTINUED" '\
              u'WHEN d.date_discontinued IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N") THEN "APPROVED" END ) ELSE  "-" END '\
              u' )) AS ITEMSTATUS, ' \
              u'CASE (IFNULL(gcp.promo_f,"N")) ' \
              u'WHEN "Y" THEN "YES" ' \
              u'WHEN "N" THEN "NO" ' \
              u'ELSE "-" ' \
              u'END ONPROMOTION, ' \
              u'UPPER((IFNULL(d.commercial_division,"-"))) AS DIVISION, ' \
              u'UPPER((IFNULL(d.commercial_trading_group,"-"))) AS ITEMGROUP, ' \
              u'UPPER((IFNULL(d.commercial_category,"-"))) AS CATEGORY, ' \
              u'UPPER((IFNULL(d.commercial_department,"-"))) AS DEPARTMENT, ' \
              u'UPPER((IFNULL(d.commercial_class,"-"))) AS SUBCATEGORY, ' \
              u'UPPER((IFNULL(d.brand_name,"-"))) AS BRAND, ' \
              u'CAST(ROUND(IFNULL(p.pack_size,0)) AS INT64) AS PACKSIZE, '  \
              u'(IFNULL(d.min_acceptable_product_life,"-")) AS MINIMUMACCEPTABLEPRODUCTLIFE, '\
              u'(IFNULL(d.country_of_origin,"-")) AS ORIGIN, ' \
              u'CASE (IFNULL(gcp.po_f,gcp.po_h)) ' \
              u' WHEN "Y" THEN "DIRECT TO STORE" ' \
              u' WHEN "N" THEN "D.C." ' \
              u'ELSE "UNKNOWN" ' \
              u'END AS POIND, ' \
              u'CASE (IFNULL(gcp.pr_f,gcp.pr_h)) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.packs_per_pallet,0)) AS int64) ' \
              u' WHEN "N" THEN CAST(ROUND(IFNULL(vendor.TI*vendor.HI,0)) AS int64) ' \
              u'ELSE CASE (IFNULL(vendor.XWMM_PRIMARY_VNDR_IND,"NULLVALUE")) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.packs_per_pallet,0)) AS int64) ' \
              u'WHEN "N" THEN CAST(ROUND(IFNULL(vendor.TI*vendor.HI,0)) AS int64) ' \
              u'ELSE 0 '  \
              u'END ' \
              u'END AS PACKSPERPALLET, ' \
              u'temp.barcode AS BARCODE, ' \
              u'CASE ((IFNULL(d.MEASUREMENT_UOM,"NULLVALUE"))) ' \
              u' WHEN "CM" THEN ROUND(CAST(d.hight AS FLOAT64),2) ' \
              u'WHEN "M" THEN (ROUND(CAST(d.hight AS FLOAT64)*100,2)) ' \
              u'ELSE  ((IFNULL(ROUND(CAST(d.hight AS FLOAT64),2),0.0))) ' \
              u'END AS HEIGHT,' \
              u'CASE ((IFNULL(d.weight_uom,"NULLVALUE"))) ' \
              u' WHEN "KG" THEN ROUND(IFNULL(d.gross_weight,d.net_weight),2) ' \
              u'WHEN "G" THEN ROUND(IFNULL(d.gross_weight/100,d.net_weight/100),2) ' \
              u'ELSE  (IFNULL(ROUND(IFNULL(d.gross_weight,d.net_weight),2),0.0))  ' \
              u'END AS WEIGHT,' \
              u'CASE (IFNULL(gcp.pr_f,gcp.pr_h)) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.ti_qty,0)) AS int64) ' \
              u' WHEN "N" THEN CAST(ROUND(IFNULL(vendor.TI,0)) AS int64) ' \
              u'ELSE CASE (IFNULL(vendor.XWMM_PRIMARY_VNDR_IND,"NULLVALUE")) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.ti_qty,0)) AS int64) ' \
              u'WHEN "N" THEN  CAST(ROUND(IFNULL(vendor.TI,0)) AS int64) ' \
              u'ELSE 0 '  \
              u'END ' \
              u'END AS PACKSPERLAYER, ' \
              u'"-" as MINIMUMORDER '\
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_PRODUCT_CURRENT + '` d  ' \
              u'INNER JOIN ' \
              u'DWR_VNDR_ITEM1  t ' \
              u'ON (t.MIN_SKU_ITEM_NBR=d.sku_item_number) ' \
              u'LEFT OUTER JOIN gcp ' \
              u'ON  (CASE d.pack_indicator WHEN "Y" THEN gcp.SKU_ITEM_NBR ELSE gcp.MIN_SKU_ITEM_NBR END )= d.sku_item_number ' \
              u' and gcp.VNDR_NBR= "' + constant.VENDOR_NUMBER + '" ' \
              u'LEFT OUTER JOIN vendor ' \
              u'ON vendor.VNDR_ITEM_NBR=d.sku_item_number ' \
              u'AND vendor.VNDR_NBR= "' + constant.VENDOR_NUMBER + '" ' \
              u'LEFT OUTER JOIN pack p ' \
              u'ON (p.pin=d.sku_item_number) ' \
              u'LEFT OUTER JOIN '  \
			  u'`' +  constant.DB_STRING + '' + constant.TABLE_BARCODE_TUC + '` temp  ON (d.sku_item_number=temp.sku_item_number) '  \
              u'WHERE 1=1 ' \
              u'ORDER BY UPPER(itemNumber) ASC'              
                 

SEARCHBY_PRODUCT_QUERY=u'WITH his_data as(SELECT PROMO_IND promo_h,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND po_h,PRIMARY_VNDR_IND pr_h,SCHL_DLVRY_DT_KEY sc_h,ACT_DLVRY_DT_KEY actual_dt,VNDR_NBR,PCHSE_ORDR_NBR ' \
              u'FROM (SELECT DISTINCT ' \
              u'PROMO_IND,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND,PRIMARY_VNDR_IND,SCHL_DLVRY_DT_KEY,ACT_DLVRY_DT_KEY,VNDR_NBR,PCHSE_ORDR_NBR, ' \
              u'ROW_NUMBER() OVER (partition by MIN_SKU_ITEM_NBR  ORDER BY SCHL_DLVRY_DT_KEY DESC,PCHSE_ORDR_NBR DESC) rn ' \
              u'FROM ' \
              u'`' + constant.DB_STRING + '' + constant.TABLE_ITEM_STATE_GCP + '` g ' \
              u'WHERE PARSE_DATE("%Y%m%d", CAST(g.SCHL_DLVRY_DT_KEY AS STRING)) < CURRENT_DATE() and VNDR_NBR = "' + constant.VENDOR_NUMBER + '") ' \
              u'WHERE rn=1), ' \
              u'current_data as(SELECT ' \
              u'PROMO_IND promo_f,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND po_f,PRIMARY_VNDR_IND pr_f,SCHL_DLVRY_DT_KEY sc_f,VNDR_NBR,PCHSE_ORDR_NBR ' \
              u'FROM (SELECT DISTINCT '\
              u'PROMO_IND,SKU_ITEM_NBR, MIN_SKU_ITEM_NBR,DIRECT_PO_IND,PRIMARY_VNDR_IND,SCHL_DLVRY_DT_KEY,VNDR_NBR,PCHSE_ORDR_NBR, ' \
              u'ROW_NUMBER() OVER (partition by MIN_SKU_ITEM_NBR  ORDER BY SCHL_DLVRY_DT_KEY ASC,PCHSE_ORDR_NBR DESC) rn ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_ITEM_STATE_GCP + '` g ' \
              u'WHERE  PARSE_DATE("%Y%m%d", CAST(g.SCHL_DLVRY_DT_KEY AS STRING)) >= CURRENT_DATE() and VNDR_NBR = "' + constant.VENDOR_NUMBER + '")  ' \
              u'WHERE rn=1), ' \
              u'gcp AS(SELECT ' \
              u'his_data.VNDR_NBR,his_data.SKU_ITEM_NBR,his_data.MIN_SKU_ITEM_NBR,his_data.po_h,his_data.pr_h,his_data.sc_h, ' \
              u'his_data.promo_h,current_data.po_f,current_data.pr_f,current_data.sc_f,current_data.promo_f, his_data.actual_dt, his_data.PCHSE_ORDR_NBR ' \
              u'FROM  his_data left outer join current_data ' \
              u'on his_data.VNDR_NBR= current_data.VNDR_NBR and his_data.SKU_ITEM_NBR=current_data.SKU_ITEM_NBR and his_data.MIN_SKU_ITEM_NBR=current_data.MIN_SKU_ITEM_NBR AND  his_data.PCHSE_ORDR_NBR=current_data.PCHSE_ORDR_NBR), ' \
              u'vendor AS( ' \
              u'SELECT TI,HI,VNDR_ITEM_NBR,VNDR_NBR,XWMM_PRIMARY_VNDR_IND FROM ' \
              u'(SELECT DISTINCT ' \
              u'v.TI,v.HI,v.VNDR_ITEM_NBR,v.VNDR_NBR,v.XWMM_PRIMARY_VNDR_IND, ' \
              u'ROW_NUMBER()OVER (PARTITION BY VNDR_ITEM_NBR ORDER BY  EFF_FROM_DT DESC) rn ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_SOURCE_VNDR_ITEM + '` v  ' \
              u'where  v.CURR_IND="Y" and v.VNDR_NBR = "' + constant.VENDOR_NUMBER + '"  ) where rn=1), '   \
              u'pack AS ( ' \
              u'SELECT ' \
              u'pin,MIN,pack_size ' \
              u'FROM (SELECT ' \
              u'sku_item_nbr pin,mbr_sku_item_nbr MIN,perassembly_cnt pack_size, ' \
              u'ROW_NUMBER() OVER (PARTITION BY sku_item_nbr ORDER BY sku_item_collctn_key DESC) rn ' \
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_ITEM_COLLCTN + '` ' \
              u'WHERE curr_ind = "Y" )' \
              u'WHERE rn = 1 ), ' \
              u'SKUFROMITEMSTATEGCP AS  (SELECT DISTINCT CASE d.pack_indicator WHEN "Y" THEN SKU_ITEM_NBR ' \
              u'ELSE MIN_SKU_ITEM_NBR END AS MIN_SKU_ITEM_NBR FROM  `' + CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT) + '' + constant.TABLE_SDH_VNDR_ITEM + '`,`'+ CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_PRODUCT_CURRENT + '` d ' \
              u'WHERE   vndr_nbr = ("' + constant.VENDOR_NUMBER + '")),DWR_VNDR_ITEM1 AS(SELECT DISTINCT MIN_SKU_ITEM_NBR ' \
              u'FROM SKUFROMITEMSTATEGCP UNION distinct (SELECT DISTINCT VNDR_ITEM_NBR MIN_SKU_ITEM_NBR ' \
              u'FROM `' + CommonUtil.get_environment_variable(constant.DATA_INTEGRATION_PROJECT) + '' + constant.TABLE_SOURCE_VNDR_ITEM + '` ' \
              u'WHERE  VNDR_NBR = ("' + constant.VENDOR_NUMBER + '") AND XWMM_PRIMARY_VNDR_IND="Y")) ' \
              u'SELECT DISTINCT ' \
              u'd.sku_item_number itemNumber,temp.tuc as tuc,d.item_description itemDescription,' \
              u'IF(gcp.PCHSE_ORDR_NBR IS NOT NULL, (IF(PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))>= CURRENT_DATE()-180 OR gcp.sc_f IS NOT NULL,"APPROVED", ' \
              u'IF ((PARSE_DATE("%Y%m%d", CAST(gcp.actual_dt AS STRING))<= CURRENT_DATE()-181 ) AND gcp.sc_f IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N"),"DISCONTINUED", ' \
              u'IF ((d.delete_flag="Y" AND d.item_status="A"),"DELETED","-" )))),(CASE d.item_status ' \
              u' WHEN "A" THEN(CASE '\
              u'WHEN d.delete_flag="Y" THEN "DELETED" '\
              u'WHEN d.date_discontinued IS NOT NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N") THEN "DISCONTINUED" '\
              u'WHEN d.date_discontinued IS NULL AND (d.delete_flag IS NULL OR d.delete_flag ="N") THEN "APPROVED" END ) ELSE  "-" END '\
              u')) AS itemStatus, ' \
              u'CASE (IFNULL(gcp.promo_f,"N")) ' \
              u'WHEN "Y" THEN "YES" ' \
              u'WHEN "N" THEN "NO" ' \
              u'ELSE "-" ' \
              u'END onPromotion, ' \
              u'UPPER((IFNULL(d.commercial_division,"-"))) AS division, ' \
              u'UPPER((IFNULL(d.commercial_trading_group,"-"))) AS itemGroup, ' \
              u'UPPER((IFNULL(d.commercial_category,"-"))) AS category, ' \
              u'UPPER((IFNULL(d.commercial_department,"-"))) AS department, ' \
              u'UPPER((IFNULL(d.commercial_class,"-"))) AS subCategory, ' \
              u'UPPER((IFNULL(d.brand_name,"-"))) AS brand, ' \
              u'CAST(ROUND(IFNULL(p.pack_size,0)) AS INT64) AS packSize, '  \
              u'(IFNULL(d.min_acceptable_product_life,"-")) AS minAcceptableProductLifeDays, '\
              u'(IFNULL(d.country_of_origin,"-")) AS origin, ' \
              u'CASE (IFNULL(gcp.po_f,gcp.po_h)) ' \
              u' WHEN "Y" THEN "DIRECT TO STORE" ' \
              u' WHEN "N" THEN "D.C." ' \
              u'ELSE "UNKNOWN" ' \
              u'END AS deliveryInd, ' \
              u'CASE ((IFNULL(d.MEASUREMENT_UOM,"NULLVALUE"))) ' \
              u' WHEN "CM" THEN ROUND(CAST(d.hight AS FLOAT64),2) ' \
              u'WHEN "M" THEN (ROUND(CAST(d.hight AS FLOAT64)*100,2)) ' \
              u'ELSE  ((IFNULL(ROUND(CAST(d.hight AS FLOAT64),2),0.0))) ' \
              u'END AS height,' \
              u'CASE ((IFNULL(d.weight_uom,"NULLVALUE"))) ' \
              u' WHEN "KG" THEN ROUND(IFNULL(d.gross_weight,d.net_weight),2) ' \
              u'WHEN "G" THEN ROUND(IFNULL(d.gross_weight/100,d.net_weight/100),2) ' \
              u'ELSE  (IFNULL(ROUND(IFNULL(d.gross_weight,d.net_weight),2),0.0))  ' \
              u'END AS weight,' \
              u'CASE (IFNULL(gcp.pr_f,gcp.pr_h)) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.packs_per_pallet,0)) AS int64) ' \
              u' WHEN "N" THEN CAST(ROUND(IFNULL(vendor.TI*vendor.HI,0)) AS int64) ' \
              u'ELSE CASE (IFNULL(vendor.XWMM_PRIMARY_VNDR_IND,"NULLVALUE")) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.packs_per_pallet,0)) AS int64) ' \
              u'WHEN "N" THEN CAST(ROUND(IFNULL(vendor.TI*vendor.HI,0)) AS int64) ' \
              u'ELSE 0 '  \
              u'END ' \
              u'END AS packPerPallet, ' \
              u'CASE (IFNULL(gcp.pr_f,gcp.pr_h)) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.ti_qty,0)) AS int64) ' \
              u' WHEN "N" THEN CAST(ROUND(IFNULL(vendor.TI,0)) AS int64) ' \
              u'ELSE CASE (IFNULL(vendor.XWMM_PRIMARY_VNDR_IND,"NULLVALUE")) ' \
              u' WHEN "Y" THEN CAST(ROUND(IFNULL(d.ti_qty,0)) AS int64) ' \
              u'WHEN "N" THEN  CAST(ROUND(IFNULL(vendor.TI,0)) AS int64) ' \
              u'ELSE 0 '  \
              u'END ' \
              u'END AS packPerLayer, ' \
              u'(temp.barcode) AS barcode, ' \
              u'"-" as minimumOrder '\
              u'FROM ' \
              u'`' +  constant.DB_STRING + '' + constant.TABLE_PRODUCT_CURRENT + '` d  ' \
              u'INNER JOIN ' \
              u'DWR_VNDR_ITEM1  t ' \
              u'ON (t.MIN_SKU_ITEM_NBR=d.sku_item_number) ' \
              u'LEFT OUTER JOIN gcp ' \
              u'ON  (CASE d.pack_indicator WHEN "Y" THEN gcp.SKU_ITEM_NBR ELSE gcp.MIN_SKU_ITEM_NBR END )= d.sku_item_number ' \
              u' and gcp.VNDR_NBR= "' + constant.VENDOR_NUMBER + '" ' \
              u'LEFT OUTER JOIN vendor ' \
              u'ON vendor.VNDR_ITEM_NBR=d.sku_item_number ' \
              u'AND vendor.VNDR_NBR= "' + constant.VENDOR_NUMBER + '" ' \
              u'LEFT OUTER JOIN pack p ' \
              u'ON (p.pin=d.sku_item_number) ' \
              u'LEFT OUTER JOIN '  \
			  u'`' +  constant.DB_STRING + '' + constant.TABLE_BARCODE_TUC + '` temp  ON (d.sku_item_number=temp.sku_item_number) '  \
              u' WHERE ' \
              u' d.sku_item_number = ("'+ constant.ITEM_NUMBER + '") AND ' \
              u'(UPPER(IFNULL(temp.TUC,"NULLVALUE")) IN (' + FILTER_TUCS + ') ' \
              u'OR UPPER(IFNULL(temp.barcode,"NULLVALUE")) IN (' + FILTER_BARCODE + ')) ' \
              u'ORDER BY UPPER(itemNumber) ASC'

PRODUCT_DETAIL_DOWNLOAD_QUERY_CSV= "SELECT CATEGORY,SUBCATEGORY,BRAND,ITEMNUMBER, TUC,ITEMDESCRIPTION,ITEMSTATUS,PACKSIZE,MINIMUMACCEPTABLEPRODUCTLIFE,HEIGHT,WEIGHT,PACKSPERPALLET,PACKSPERLAYER,BARCODE,ONPROMOTION FROM ("+ PRODUCT_DETAIL_DOWNLOAD_QUERY + ")"

PRODUCT_DETAIL_DOWNLOAD_QUERY_CSVALL_DF= "SELECT DISTINCT ITEMNUMBER,TUC,ITEMDESCRIPTION,"+FILTER_FOR_DOWNLOAD +" FROM ("+ PRODUCT_DETAIL_DOWNLOAD_QUERY_ALL + ")"

PRODUCT_DETAIL_DOWNLOAD_QUERY_CSVALL= "SELECT DISTINCT "+FILTER_FOR_DOWNLOAD +" FROM ("+ PRODUCT_DETAIL_DOWNLOAD_QUERY_ALL + ")"



PRODUCT_DETAIL_DOWNLOAD_QUERY_CSVALL_SELECTED_FIELDS_DF ="""
SELECT "Please Note - The SKUs for which there is no relevant data do not feature in this report." AS ITEMNUMBER, "" AS TUC,
"" AS ITEMDESCRIPTION,
FIELD_ARAAY_SELECTED
UNION ALL 
SELECT "Supplier: SELECTED_SUPPLIER_NAME" AS ITEMNUMBER, "" AS TUC,
"" AS ITEMDESCRIPTION,
FIELD_ARAAY_SELECTED
UNION ALL
SELECT "ITEM_NUMBER" AS ITEMNUMBER, "TUC" AS TUC,
"ITEM_DESCRIPTION" AS ITEMDESCRIPTION,
FIELD_ARRAY
"""
PRODUCT_DETAIL_DOWNLOAD_QUERY_CSVALL_SELECTED_FIELDS = """
SELECT "Please Note - The SKUs for which there is no relevant data do not feature in this report." FIELD_ARAAY_SELECTED 
UNION ALL 
SELECT "Supplier: SELECTED_SUPPLIER_NAME" FIELD_ARAAY_SELECTED
UNION ALL
SELECT FIELD_ARRAY
"""

PRODUCT_DETAIL_DOWNLOAD_QUERY_CSV__SELECTED_FIELDS="""
SELECT "Please Note - The SKUs for which there is no relevant data do not feature in this report." AS CATEGORY, "" AS SUBCATEGORY,
"" AS BRAND,"" AS ITEMNUMBER,
"" AS TUC,"" AS ITEMDESCRIPTION,"" AS ITEMSTATUS,
"" AS PACKSIZE, "" AS MINIMUMACCEPTABLEPRODUCTLIFE,
"" AS HEIGHT, "" AS WEIGHT,"" AS PACKSPERPALLET, "" AS PACKSPERLAYER,
"" AS BARCODE, "" AS ONPROMOTION
UNION ALL 
SELECT "Supplier: SELECTED_SUPPLIER_NAME"  AS CATEGORY, "" AS SUBCATEGORY,
"" AS BRAND,"" AS ITEMNUMBER,
"" AS TUC,"" AS ITEMDESCRIPTION,"" AS ITEMSTATUS,
"" AS PACKSIZE, "" AS MINIMUMACCEPTABLEPRODUCTLIFE,
"" AS HEIGHT, "" AS WEIGHT,"" AS PACKSPERPALLET, "" AS PACKSPERLAYER,
"" AS BARCODE, "" AS ONPROMOTION 
UNION ALL
SELECT  "CATEGORY" AS CATEGORY, "SUBCATEGORY" AS SUBCATEGORY,
"BRAND" AS BRAND,"ITEM_NUMBER" AS ITEMNUMBER,
"TUC" AS TUC,"ITEM_DESCRIPTION" AS ITEMDESCRIPTION,"ITEM_STATUS" AS ITEMSTATUS,
"PACK_SIZE" AS PACKSIZE, "MIN_ACCEPTABLE_PRODUCT_LIFE_DAYS" AS MINIMUMACCEPTABLEPRODUCTLIFE,
"HEIGHT_CM" AS HEIGHT, "WEIGHT_KG" AS WEIGHT,"PACKS_PER_PALLET" AS PACKSPERPALLET, "PACKS_PER_LAYER" AS PACKSPERLAYER,
"BARCODE_SELLING" AS BARCODE, "ON_PROMOTION" AS ONPROMOTION
"""


SEARCHBY_PRODUCT_QUERY_SELECTED_FIELDS="""
SELECT "Please Note - The SKUs for which there is no relevant data do not feature in this report." AS itemNumber, "" AS tuc,
"" AS itemDescription,
"" AS itemStatus, "" AS onPromotion, "" AS division,
"" AS itemGroup, "" AS category,
"" AS department, 
"" AS subCategory, "" AS brand,
"" AS packSize, "" AS minAcceptableProductLifeDays, 
"" AS origin, "" AS deliveryInd,
"" AS height, "" AS weight,
"" AS packPerPallet, "" AS packPerLayer,
"" AS barcode, "" AS minimumOrder 
UNION ALL 
SELECT "Supplier: SELECTED_SUPPLIER_NAME" AS itemNumber, "" AS tuc,
"" AS itemDescription,
"" AS itemStatus, "" AS onPromotion, "" AS division,
"" AS itemGroup, "" AS category,
"" AS department, 
"" AS subCategory, "" AS brand,
"" AS packSize, "" AS minAcceptableProductLifeDays, 
"" AS origin, "" AS deliveryInd,
"" AS height, "" AS weight,
"" AS packPerPallet, "" AS packPerLayer,
"" AS barcode, "" AS minimumOrder 
UNION ALL
SELECT "ITEM_NUMBER" AS itemNumber, "TUC" AS tuc,
"ITEM_DESCRIPTION" AS itemDescription,
"ITEM_STATUS" AS itemStatus, "ON_PROMOTION" AS onPromotion, "DIVISION" AS division,
"ITEM_GROUP" AS itemGroup, "CATEGORY" AS category,
"DEPARTMENT" AS department, 
"SUBCATEGORY" AS subCategory, "BRAND" AS brand,
"PACK_SIZE" AS packSize, "MIN_ACCEPTABLE_PRODUCT_LIFE_DAYS" AS minAcceptableProductLifeDays, 
"ORIGIN" AS origin, "DELIVERY" AS deliveryInd,
"HEIGHT_CM" AS height, "WEIGHT_KG" AS weight,
"PACKS_PER_PALLET" AS packPerPallet, "PACKS_PER_LAYER" AS packPerLayer,
"BARCODE_SELLING" AS barcode, "MINIMUM_ORDER" AS minimumOrder 
"""


