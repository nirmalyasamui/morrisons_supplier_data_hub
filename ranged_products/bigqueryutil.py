'''
***********************************************************************************************************************
Purpose: Common BigQuery utility file for implementing searches.
Developer: Somnath De (somnath.de@morrisonsplc.co.uk)
***********************************************************************************************************************
'''

import json
from google.cloud import bigquery  # Cloud BigQuery library...
from google.cloud import storage  # Cloud storage library...
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import Forbidden,BadRequest
from datetime import datetime
from commonutil import CommonUtil
import constant as constant
import gcsfs
from dateutil import tz



import google.cloud.logging
import logging

client = google.cloud.logging.Client()
client.setup_logging()

bst_tz = tz.gettz('Europe/London')


BQ = bigquery.Client()
GCS = storage.Client()
GCP_STORAGE_URL = 'gs://' + CommonUtil.get_environment_variable(constant.CSV_FILE_STORAGE_BUCKET) + '/'
FULL_COUNT_PREFIX = 'SELECT *, count(*) OVER() as full_count FROM ('

class BigQueryUtil:
    @staticmethod
    def _get_extract_job_config(format):
        job_config = bigquery.job.ExtractJobConfig(print_header=False)
        if format.upper() == 'JSON':
            job_config.destination_format = (bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON)
        elif format.upper() == 'AVRO':
            job_config.destination_format = (bigquery.DestinationFormat.AVRO)
        elif format.upper() == 'PARQUET':
            job_config.destination_format = (bigquery.DestinationFormat.PARQUET)
        elif format.upper() == 'CSV':
            job_config.destination_format = (bigquery.DestinationFormat.CSV)

        return job_config

    @staticmethod
    def create_download_table(query, destination_blob, file_and_table_name, vendor, file_format) -> object:
        pre_time = datetime.now(bst_tz)  # start time...
        final_result = {}
        logging.info("-----final query in bigquery util------")
        logging.info(query)
        table_ref=BigQueryUtil._create_download_table(query)
        if table_ref==constant.ERROR_RESPONSE_TOO_LARGE:return {constant.MESSAGE : constant.ERROR_RESPONSE_TOO_LARGE}
        if destination_blob==file_and_table_name:
            file_name = vendor + '/' + file_and_table_name + '.csv'
        else:file_name = vendor + '/' + file_and_table_name + '*.csv'    
        destination_uri = GCP_STORAGE_URL + file_name

        job_config = BigQueryUtil._get_extract_job_config(file_format)

        extract_job = BQ.extract_table(
            source = table_ref,
            destination_uris = destination_uri,
            location = constant.LOCATION,
            job_id = file_and_table_name,
            job_config = job_config,
            job_id_prefix = CommonUtil.get_environment_variable(constant.JOB_ID_PREFIX)
        )
        # Do not wait...        
        if destination_blob!=file_and_table_name:
            extract_job.result()  # Waits for job to complete.
            file_name = vendor + '/' +  destination_blob + '.csv'            
            compose_file(file_name,destination_uri)
            destination_uri = GCP_STORAGE_URL + file_name
        post_time = datetime.now(bst_tz)  # end time...

        pre_date_time = pre_time.strftime(constant.DATE_FORMAT_YMDHMS)
        post_date_time = post_time.strftime(constant.DATE_FORMAT_YMDHMS)

        final_result['message'] = 'success'
        final_result['startTime'] = pre_date_time
        final_result['endTime'] = post_date_time
        final_result['filename'] = destination_uri

        return final_result

    @staticmethod
    def search_and_display(query, limit, offset=0) -> object:
        query = query.rstrip()
        final_result = {}
        if query.upper().endswith('ASC') or query.upper().endswith('DESC'):
            idx = query.upper().rindex(' ORDER BY')
            query = FULL_COUNT_PREFIX + query[:idx] + ')' + query[idx:]
        else:
            query = FULL_COUNT_PREFIX + query + ')'
        logging.info("-----final query in bigquery util------")
        logging.info(query)
        #print('final query in bigquery util - {}'.format(query))
        pre_time = datetime.now(bst_tz)  # start time...
        final_result = BigQueryUtil._get_final_result(query, limit, offset)
        post_time = datetime.now(bst_tz)  # end time...

        pre_date_time = pre_time.strftime(constant.DATE_FORMAT_YMDHMS)
        post_date_time = post_time.strftime(constant.DATE_FORMAT_YMDHMS)

        final_result['startTime'] = pre_date_time
        final_result['endTime'] = post_date_time

        return final_result

    

    @staticmethod
    def search_and_display_sort(query, limit, offset,sort_by,sort_direction) -> object:
        query = query.rstrip()
        final_result = {}
        if query.upper().endswith('ASC') or query.upper().endswith('DESC'):
            idx = query.upper().rindex(' ORDER BY')
            query = FULL_COUNT_PREFIX + query[:idx] + ')' + query[idx:]
        else:
            query = FULL_COUNT_PREFIX + query + ')'
        print('final query in bigquery util - {}'.format(query))
        pre_time = datetime.now(bst_tz)  # start time...
        final_result = BigQueryUtil._get_final_result_sort(query, limit, offset,sort_by,sort_direction)
        post_time = datetime.now(bst_tz)  # end time...

        pre_date_time = pre_time.strftime(constant.DATE_FORMAT_YMDHMS)
        post_date_time = post_time.strftime(constant.DATE_FORMAT_YMDHMS)

        final_result['startTime'] = pre_date_time
        final_result['endTime'] = post_date_time

        return final_result    

    @staticmethod
    def search_save_and_display(query, limit, page, user) -> object:
        pre_time = datetime.now(bst_tz)  # start time...
        final_result = {}
        final_result = BigQueryUtil._get_final_result(query, limit, page)
        data = final_result['data']

        file_name = BigQueryUtil._save_to_gcs(data, user)

        final_result['filePath'] = file_name
        post_time = datetime.now(bst_tz)  # end time...

        pre_date_time = pre_time.strftime(constant.DATE_FORMAT_YMDHMS)
        post_date_time = post_time.strftime(constant.DATE_FORMAT_YMDHMS)

        final_result['startTime'] = pre_date_time
        final_result['endTime'] = post_date_time

        return final_result

    @staticmethod
    def _get_final_result_sort(query, limit, offset,sort_by,sort_direction) -> dict:
        records = BigQueryUtil._search_big_query(query, limit, offset)
        final_result = BigQueryUtil._get_output_as_dict_sort(records, limit, offset,sort_by,sort_direction)


        return final_result    

    @staticmethod
    def search_and_save(query, limit, page, user) -> object:
        pre_time = datetime.now(bst_tz)  # start time...
        final_result = {}
        final_result = BigQueryUtil._get_final_result(query, limit, page)
        data = final_result['data']

        file_name = BigQueryUtil._save_to_gcs(data, user)

        final_result.pop('data')
        final_result['filePath'] = file_name
        post_time = datetime.now(bst_tz)  # end time...

        pre_date_time = pre_time.strftime(constant.DATE_FORMAT_YMDHMS)
        post_date_time = post_time.strftime(constant.DATE_FORMAT_YMDHMS)

        final_result['startTime'] = pre_date_time
        final_result['endTime'] = post_date_time

        return final_result

    @staticmethod
    def _search_big_query(query, limit, offset) -> object:
        params = []

        # Limit is provided that means the output of the service call needs to be displayed on the UI...
        if limit is not None:
            query = query + ' LIMIT @limit OFFSET @offset'

            params = [
                bigquery.ScalarQueryParameter('limit', 'INT64', int(limit)),
                bigquery.ScalarQueryParameter('offset', 'INT64', int(offset))
            ]

        job_config = bigquery.QueryJobConfig()
        job_config.query_parameters = params

        query_job = BQ.query(query=query, location=constant.LOCATION, job_config=job_config,
                             job_id_prefix=CommonUtil.get_environment_variable(constant.JOB_ID_PREFIX))
        records = [dict(row) for row in query_job]

        return records

    @staticmethod
    def _create_download_table(query) -> object:
        try:
            job_config = bigquery.QueryJobConfig()
            query_job = BQ.query(query=query, location=constant.LOCATION, job_config=job_config,
                             job_id_prefix=CommonUtil.get_environment_variable(constant.JOB_ID_PREFIX))
            query_job.result()
            destination = query_job.destination
            return destination
        except Forbidden as e:
            print('forbidden error - ', e)
            return constant.ERROR_RESPONSE_TOO_LARGE
        except BadRequest as err: 
            print('bad request err - ', err)
            return constant.ERROR_RESPONSE_TOO_LARGE                

    @staticmethod
    def _get_final_result(query, limit, offset) -> dict:
        records = BigQueryUtil._search_big_query(query, limit, offset)
        final_result = BigQueryUtil._get_output_as_dict(records, limit, offset)
        return final_result
    
   

    @staticmethod
    def _save_to_gcs(data, user) -> object:
        query_date = datetime.now(bst_tz).strftime("%Y%m%d")
        random_name = datetime.now(bst_tz).strftime("%Y%m%d%H%M%S%f")
        file_name = query_date + '/' + user + '/' + random_name + '.json'

        bucket = GCS.get_bucket(CommonUtil.get_environment_variable(constant.CSV_FILE_STORAGE_BUCKET))
        blob = bucket.blob(file_name)
        blob.upload_from_string(json.dumps(data))

        return GCP_STORAGE_URL + file_name

    @staticmethod
    def _get_output_as_dict(records, limit, offset) -> dict:
        final_result = {}

        if not records:
            final_result['message'] = constant.ERROR_NO_DATA_FOUND
            final_result['data'] = []
        else:
            final_result['message'] = constant.SUCCESS_MESSAGE
            final_result['data'] = records
            final_result['metadata'] = {}
            final_result['metadata']['limit'] = limit
            final_result['metadata']['offset'] = offset
            final_result['metadata']['totalCount'] = int(records[0]['full_count'])

        return final_result

    

    @staticmethod
    def _get_output_as_dict_sort(records, limit, offset,sort_by,sort_direction) -> object:
        final_result = {}

        if not records:
            final_result['message'] = constant.ERROR_NO_DATA_FOUND
            final_result['data'] = []
        else:
            final_result['message'] = constant.SUCCESS_MESSAGE
            final_result['data'] = records
            final_result['metadata'] = {}
            final_result['metadata']['limit'] = limit
            final_result['metadata']['offset'] = offset
            final_result['metadata']['sortBy'] = sort_by
            final_result['metadata']['sortDirection'] = sort_direction
            final_result['metadata']['totalCount'] = int(records[0]['full_count'])
            

        return final_result   


def compose_file(destination_blob_name,destination_uri):
    """Concatenate source blobs into destination blob."""
    sources=[] # sources is a list of Blob instances, up to the max of 32 instances per request
    x=0
    gcs=gcsfs.GCSFileSystem(project=CommonUtil.get_environment_variable(constant.SUPPLIER_DATA_HUB_PROJECT))
    files=gcs.glob(destination_uri)
    n=len(files)
    bucket_name = CommonUtil.get_environment_variable(constant.CSV_FILE_STORAGE_BUCKET)
    bucket = GCS.bucket(bucket_name)
    destination = bucket.blob(destination_blob_name)
    destination.content_type = "application/octet-stream"
    for f in files:
        f=f.replace(bucket_name+"/",'')
        x=x+1
        if x==1:sources.append(bucket.get_blob(destination_blob_name))
        sources.append(bucket.get_blob(f))
        if x ==31 or x==n:
            destination.compose(sources)
            sources=[]
            x=0

    for f in files:
        f=f.replace(bucket_name+"/",'')
        try:
            bucket.delete_blob(f)
        except NotFound:
            pass
    

