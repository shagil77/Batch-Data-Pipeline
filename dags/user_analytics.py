from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
  LocalFilesystemToS3Operator
)
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

with DAG(
  "user_analytics_dag",
  description="A DAG to Pull user data and movie review data to analyze their behavior.",
  schedule_interval=timedelta(days=1),
  start_date=datetime(2024, 8, 20),
  catchup=False
) as dag:
  user_analytics_bucket = "user-analytics"

  create_s3_bucket = S3CreateBucketOperator(
    task_id="create_s3_bucket",
    bucket_name=user_analytics_bucket
  )

  movie_review_to_s3 = LocalFilesystemToS3Operator(
    task_id="movie_review_to_s3",
    filename="/opt/airflow/data/model_review.csv",
    dest_key="raw/movie_review.csv",
    dest_bucket=user_analytics_bucket,
    replace=True
  )

  user_purchase_to_s3 = SqlToS3Operator(
    task_id="user_purchase_to_s3",
    sql_conn_id="postgres_default",
    query="select * from retail.user_purchase",
    s3_bucket=user_analytics_bucket,
    s3_key="raw/user_purchase/user_purchase.csv",
    replace=True
  )