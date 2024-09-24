from google.cloud import storage
import constants as constant
import json
import os


def get_bucket_data(key):
    try:
        # Read the data from Google Cloud Storage
        storage_client = storage.Client()

        # Set buckets and filenames
        bucket_name = os.environ.get(constant.GCLOUD_PROJECT) + "-" + os.environ.get(constant.BUCKET)
        filename = os.environ.get(constant.CONFIG_FILE_NAME)

        # get bucket with name
        bucket = storage_client.bucket(bucket_name)

        # get bucket data as blob
        blob = bucket.get_blob(filename)
        print('blob - {}'.format(blob))
        # convert to string
        json_data = blob.download_as_string()

        print('json_data - {}'.format(json_data))
        data = json.loads(json_data)
        print('data type - {}'.format(type(data)))
        for d in data:
            if key in d:
                return d
    except ValueError as e:
        print('Exception in reading the config json file from storage', e)
        raise ValueError(e)