import s3fs
import polars as pl
import pyarrow.fs as fs

from loguru import logger
from polars import LazyFrame
from pyarrow.dataset import dataset
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket

class NBABucket(object):
    
    def __init__(self):
        self.aws_creds = AwsCredentials.load("aws-nba-etl-user-credentials")
        self.bucket = S3Bucket.load("nba-bucket")
        self.bucket_name = self.bucket.bucket_name
        self.region_name = self.aws_creds.region_name
        self.storage_options = {"key": self.aws_creds.aws_access_key_id, "secret": self.aws_creds.aws_secret_access_key.get_secret_value()}
        self.fs = s3fs.S3FileSystem(**self.storage_options)


    def scan_parquet(self, filepath: str) -> LazyFrame:
        """
        Scan for Parquet files in the specified S3 bucket and prefix using PyArrow.
        """

        logger.info(f"Scanning Parquet dataset: s3://{self.bucket_name}/{filepath}")

        s3_fs = fs.S3FileSystem(
            access_key=self.storage_options["key"],
            secret_key=self.storage_options["secret"],
            region=self.region_name,
        )

        ds = dataset(
            source=f"{self.bucket_name}/{filepath}", 
            filesystem=s3_fs, 
            format="parquet"
        )

        return pl.scan_pyarrow_dataset(ds)

    def sink_parquet(
        self, lf: LazyFrame, output_key: str, folder: str = "processed"
    ) -> str:
        """
        Write a Polars LazyFrame to S3 in Parquet format.
        """

        logger.info(f"Writing data to s3://{self.bucket_name}/{folder}/{output_key}")

        output_path = f"s3://{self.bucket_name}/{folder}/{output_key}"

        with self.fs.open(output_path, "wb") as f:
            lf.collect().write_parquet(
                f, compression="snappy", storage_options=self.storage_options
            )

        logger.info(f"Data written to: {output_path}")

        return output_path


nba_bucket = NBABucket()