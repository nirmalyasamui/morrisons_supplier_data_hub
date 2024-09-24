"""
This file does the operation of retrieving biqquery results
"""

from datetime import datetime

from google.cloud import bigquery  # Cloud BigQuery library...
from google.cloud import storage  # Cloud storage library...


import constant as constant
from req_res_controller import get_environment_variable
from dateutil import tz

bst_tz = tz.gettz('Europe/London')

BQ_Client = bigquery.Client()
GCS_Client = storage.Client()


def bigquery_dc_report(query, req_data,export):            
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("week_commence", "STRING", req_data["week_commencing"]),
            bigquery.ScalarQueryParameter("depot", "STRING", req_data["dc"]),
            bigquery.ScalarQueryParameter("period","STRING",int(req_data["period"])-1),
            bigquery.ScalarQueryParameter("max_results", "INT64", (int(req_data["max_results"]) + int(req_data["offset"])) * int(req_data["period"])),
            bigquery.ScalarQueryParameter("offset", "INT64", int(req_data["offset"])),
            bigquery.ScalarQueryParameter("vendor_nbr", "STRING", req_data["vendor_no"])
        ]
    )
    print(query)
    query_job = BQ_Client.query(query,location=constant.LOCATION, job_config=job_config,
                job_id_prefix=get_environment_variable(constant.JOB_ID_PREFIX))  
    result=query_job.result()  
    destination = query_job.destination  
    if export=="csv":
        user_name=req_data["user_name"]
        vendor_no=req_data["vendor_no"]
        result=export_to_storage_csv(vendor_no,user_name,destination)
    return result


# Exports a table to a CSV file in a Cloud Storage bucket


def export_to_storage_csv(vendor_no,user_name,table_ref):
    try:
        query_date = "{:_%d%m%Y_%H_%M_%S}".format(datetime.now(bst_tz))
        file_name = vendor_no + "/" + "dcoutbound_"+user_name + query_date + '.csv'
        bucket_name = get_environment_variable(constant.CSV_FILE_STORAGE_BUCKET)
        destination_uri = "gs://{}/{}".format(bucket_name, file_name)
        
        job_config = bigquery.job.ExtractJobConfig(print_header=False)
        BQ_Client.extract_table(table_ref, destination_uri, location=constant.LOCATION,job_config=job_config)
        
        result=destination_uri
        return result  
    except Exception as exp:
        return exp
