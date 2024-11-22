# -*- coding: utf-8 -*-


from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator

args = {
    'owner': 'airflow'
}


hiveSQL_create_bike_share_trips_table = '''
CREATE EXTERNAL TABLE IF NOT EXISTS bike_share_trips (
    tripduration INT,
    starttime TIMESTAMP,
    stoptime TIMESTAMP,
    `start station id` INT,
    `start station name` STRING,
    `start station latitude` DOUBLE,
    `start station longitude` DOUBLE,
    `end station id` INT,
    `end station name` STRING,
    `end station latitude` DOUBLE,
    `end station longitude` DOUBLE,
    bikeid INT,
    usertype STRING,
    `birth year` INT,
    gender INT
)
COMMENT 'Bike data' 
PARTITIONED BY (partition_year INT, partition_month INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' -- Assuming CSV file
STORED AS TEXTFILE LOCATION '/user/hadoop/bike_share/bike_share_trips'
TBLPROPERTIES ('skip.header.line.count'='1');
'''

hiveSQL_add_partition_bike_share_trips = '''
ALTER TABLE bike_share_trips ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}})
LOCATION '/user/hadoop/bike_share/bike_share_trips/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/'; 

LOAD DATA INPATH '/user/hadoop/bike_share/bike_share_trips/2024/11/bike_share_{{ ds }}.csv' INTO TABLE bike_share_trips;
'''


dag = DAG('DAG for hubway', default_args=args, description='KPI Pipeline',
          schedule_interval='56 18 * * *',
          start_date=datetime(2024, 11, 13), catchup=False, max_active_runs=1)

create_local_import_directory = CreateDirectoryOperator(
    task_id='create_import_directory',
    path='/home/airflow',
    directory='bike_share',
    dag=dag,
)

remove_old_csv = BashOperator(
    task_id='remove_old_csv_files',
    bash_command='rm -r /home/airflow/bike_share/csv',
    dag=dag,
)

clear_local_import_directory = ClearDirectoryOperator(
    task_id='clear_import_directory',
    directory='/home/airflow/bike_share',
    pattern='*',
    dag=dag,
)

download_bike_share_data = HttpDownloadOperator(
    task_id='download_bike_share_dataset',
    download_uri='https://www.kaggle.com/api/v1/datasets/download/acmeyer/bike-share-data',
    save_to='/home/airflow/bike_share/bike_share_{{ ds }}.zip',
    dag=dag,
)

unzip_bike_share_data = BashOperator(
    task_id='unzip_bike_share_dataset',
    bash_command='unzip /home/airflow/bike_share/bike_share_{{ ds }}.zip -d /home/airflow/bike_share/',
    dag=dag,
)

# Create CSV directory for bike share data
create_bike_share_csv_directory = CreateDirectoryOperator(
    task_id='create_bike_share_csv_directory',
    path='/home/airflow/bike_share',
    directory='csv',
    dag=dag,
)

filter_and_copy_bike_share_csv = BashOperator(
    task_id='filter_and_copy_csv_files',
    bash_command='find /home/airflow/bike_share/ -type f -name \"*bike-share-tripdata.csv\" -exec cp {} /home/airflow/bike_share/csv/ \;',
    dag=dag,
)

merge_bike_share_csv_files = BashOperator(
    task_id='merge_bike_share_csv_files',
    bash_command='cd /home/airflow/bike_share/csv && csvstack *.csv > bike_share_{{ ds }}.csv',
    dag=dag,
)

# HDFS Operations
create_hdfs_bike_share_partition_directory = HdfsMkdirFileOperator(
    task_id='create_hdfs_bike_share_partition_directory',
    directory='/user/hadoop/bike_share/bike_share_trips/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/',
    hdfs_conn_id='hdfs',
    dag=dag,
)


hdfs_upload_bike_share_data = HdfsPutFileOperator(
    task_id='upload_bike_share_data_to_hdfs',
    local_file='/home/airflow/bike_share/csv/bike_share_{{ ds }}.csv',
    remote_file='/user/hadoop/bike_share/bike_share_trips/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/bike_share_{{ ds }}.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)


create_HiveTable_bike_share_trips = HiveOperator(
    task_id='create_bike_share_trips_table',
    hql=hiveSQL_create_bike_share_trips_table,
    hive_cli_conn_id='beeline',
    dag=dag
)

addPartition_HiveTable_bike_share_trips = HiveOperator(
    task_id='add_partition_to_bike_share_trips_table',
    hql=hiveSQL_add_partition_bike_share_trips,
    hive_cli_conn_id='beeline',
    dag=dag
)

kpi_gen = SparkSubmitOperator(
    task_id='Gen_kpi',
    conn_id='spark',
    application='/home/airflow/airflow/python/kpi_generation.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='3g',
    num_executors='2',
    name='spark_gen_kpi',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}', '--hdfs_source_dir', '/user/hadoop/hubway'],
    dag=dag
)


create_local_import_directory >> remove_old_csv >> clear_local_import_directory >> download_bike_share_data >> unzip_bike_share_data >> create_bike_share_csv_directory >> filter_and_copy_bike_share_csv >> merge_bike_share_csv_files >> create_hdfs_bike_share_partition_directory >> hdfs_upload_bike_share_data >> kpi_gen
