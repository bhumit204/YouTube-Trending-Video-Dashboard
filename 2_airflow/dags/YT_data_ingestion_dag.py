import os
from os import listdir
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from google.cloud import storage

from kaggle.api.kaggle_api_extended import KaggleApi
from airflow.models.dag import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
# from airflow.operators.bash import BashOperator

GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'YT_Trending_data_all')
LOCAL_PATH = '/home/airflow/'
PARQUET_FILE = 'All_YT_Data.parquet'

args_dags = {'kaggle_username':'xxxxx', 'kaggle_key':'xxxxx', 
            'link':'rsrishav/youtube-trending-video-dataset', 'path':LOCAL_PATH}
default_args = {
    'depends_on_past': False
}

def extract_data_from_kaggle(kaggle_username, kaggle_key, link, path):
    os.environ['KAGGLE_USERNAME'] = kaggle_username
    os.environ['KAGGLE_KEY'] = kaggle_key

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(link, path=path, unzip=True)

def process_csv_convert_to_parquet(path_of_csv):
    csv_data = pd.DataFrame()
    json_data = pd.DataFrame()
    for file_name in listdir(path_of_csv):
        if file_name.endswith('.csv'):
            print(f"Processing {file_name}...")
            csv_file_path = os.path.join(path_of_csv,file_name)
            yt_data = pd.read_csv(csv_file_path, encoding='latin-1')
            csv_data = pd.concat([csv_data, yt_data])
        elif file_name.endswith('.json'):
            print(f"Processing {file_name}...")
            json_file_path = os.path.join(path_of_csv,file_name)
            yt_data = pd.read_json(json_file_path, encoding='latin-1')
            json_data = pd.concat([json_data, yt_data])

    csv_data = csv_data.drop(columns=['description'])
    json_data = json_data.filter(items=['items']).drop_duplicates().to_dict('records')
    # print(f"json_data is {json_data}")
    json_unq_data = []
    unq_id = []
    for itr in json_data:
        if itr['items']['id'] not in unq_id:
            json_unq_data.append({'categoryId': int(itr['items']['id']), 'category':itr['items']['snippet']['title']})
            unq_id.append(itr['items']['id'])

    json_unq_data = pd.DataFrame(json_unq_data)
    csv_data = csv_data.merge(json_unq_data,on='categoryId', how='left')
    print(f"csv_data.shape[0] is {csv_data.shape[0]}")
    csv_data = csv_data.astype({'categoryId':'int','view_count':'int','likes':'int','dislikes':'int','comment_count':'int'})
    csv_data['publishedAt'] = pd.DatetimeIndex(csv_data['publishedAt']).tz_localize(None)
    csv_data['trending_date'] = pd.to_datetime(csv_data['trending_date'], yearfirst=True, format='ISO8601').dt.date
    table = pa.Table.from_pandas(csv_data)
    pq.write_table(table, os.path.join(LOCAL_PATH, PARQUET_FILE), coerce_timestamps='us')

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name + local_file)
    blob.upload_from_filename(os.path.join(LOCAL_PATH,local_file))


with DAG(
    'YT_data_ingestion',
    default_args=default_args,
    description='YouTube data extract and load to Data Lake and Warehouse',
    schedule_interval=None,
    start_date=days_ago(2),
    # tags=['example'],
) as dag:

    extract_data_task = PythonOperator(
        task_id="YT_Extract_Data",
        python_callable=extract_data_from_kaggle,
        op_kwargs=args_dags,
    )

    convert_csv_to_parquet_task = PythonOperator(
        task_id="Conv_csv_to_parquet",
        python_callable=process_csv_convert_to_parquet,
        op_kwargs={'path_of_csv':LOCAL_PATH},
    )

    load_parquet_to_gcs = PythonOperator(
        task_id="load_parquet_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": GCS_BUCKET,
            "object_name": "YT_Data/",
            "local_file": PARQUET_FILE,
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bq_YT_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "YT_external_table_data",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{GCS_BUCKET}/YT_Data/{PARQUET_FILE}"],
            },
        },
    )

    extract_data_task >> convert_csv_to_parquet_task >> load_parquet_to_gcs >> bigquery_external_table_task