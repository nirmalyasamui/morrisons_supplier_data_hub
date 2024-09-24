
import os
import uuid
from urllib.parse import unquote
from datetime import date
from datetime import datetime
from datetime import timedelta
from dateutil import tz
from dateutil.relativedelta import relativedelta
import constant as constant
from google.cloud import bigquery  # Cloud BigQuery library...
import google.cloud.logging

client = google.cloud.logging.Client()
client.setup_logging()
import logging

BQ = bigquery.Client()



class FYUtil:

    YTD_DATES_QUERY = 'WITH CURRENT_YTD as (SELECT  BSNS_YR_STRT_DT,DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) - 1 END_DT FROM `' + 'db_value' + '' + constant.TABLE_TIME_DAY + '` where current_date() >=  DATE(BSNS_YR_STRT_DT) and current_date()<= DATE(BSNS_YR_END_DT) limit 1) SELECT CAST(CURRENT_YTD.BSNS_YR_STRT_DT as DATE) as YTD_START_DATE,CAST(CURRENT_YTD.END_DT as DATE) as YTD_END_DATE from CURRENT_YTD '

    @staticmethod
    def _get_business_year():

        db=FYUtil._get_environment_var(constant.DATA_INTEGRATION_PROJECT)
        query= FYUtil.YTD_DATES_QUERY
        query=query.replace('db_value',db)
        result=FYUtil._search_data(query,1)
        logging.info('fy_data')
        logging.info(result)
        return result

    @staticmethod
    def _search_data(query,limit) -> object:
        params = []

        # Limit is provided that means the output of the service call needs to be displayed on the UI...
        if limit is not None:
            query = query + ' LIMIT @limit  '

            params = [
                bigquery.ScalarQueryParameter('limit', 'INT64', int(limit)),
               
            ]

        job_config = bigquery.QueryJobConfig()
        job_config.query_parameters = params

        query_job = BQ.query(query=query, location=constant.LOCATION, job_config=job_config,
                             job_id_prefix=FYUtil._get_environment_var(constant.JOB_ID_PREFIX))
        records = [dict(row) for row in query_job]

        return records   

    @staticmethod
    def _get_environment_var(token):
        return os.environ.get(token)     