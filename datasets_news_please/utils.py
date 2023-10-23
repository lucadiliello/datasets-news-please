# import hashlib
import logging
import os
import time
import urllib
import urllib.parse
from typing import Dict
import boto3
import botocore
import gzip

import requests
from dateutil import parser
from newsplease.crawler.commoncrawl_crawler import (
    __iterate_by_month, __date_within_period, __extract_date_from_warc_filename
)
from newsplease.crawler.commoncrawl_extractor import NewsPlease

from tqdm import tqdm


# set own logger
logger = logging.getLogger('datasets_news_please')

# commoncrawl.org
CC_BASE_URL = 'https://data.commoncrawl.org'
CC_BASE_BUCKET = 'commoncrawl'

# what to keep from downloaded articles
KEYS_TO_KEEP = (
    "date_download",
    "date_publish",
    "date_modify",
    "description",
    "language",
    "title",
    "title_page",
    "source_domain",
    "maintext",
    "authors",
    "image_url",
)


class DownloadProgress(object):

    def __init__(self, total: int = None, name: str = None, position: int = None, disable: bool = False):
        super().__init__()
        if name is not None:
            name = urllib.parse.unquote(name)
        name = "Downloading" if name is None else f"Downloading {os.path.split(name)[-1]}"
        self.progress = tqdm(desc=name, total=total, unit="B", unit_scale=True, position=position, disable=disable)

    def __enter__(self):
        return self.callback

    def callback(self, increment: int):
        self.progress.update(increment)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress.close()


def get_publishing_language(warc_record, article):
    r""" Extracts the publishing language from the record. """
    if hasattr(article, 'language'):
        return str(article.language)
    else:
        return None


def get_publishing_date(warc_record, article):
    r""" Extracts the publishing date from the record. """
    if hasattr(article, 'date_publish'):
        return parser.parse(article.date_publish) if isinstance(article.date_publish, str) else article.date_publish
    else:
        return None


def get_remote_index(warc_files_start_date=None, warc_files_end_date=None, bucket_name: str = CC_BASE_BUCKET):
    r""" Gets the index of news crawl files and returns an array of names. """

    s3_client = boto3.client('s3')
    # Verify access to commoncrawl bucket
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except (botocore.exceptions.ClientError, botocore.exceptions.NoCredentialsError):
        logger.info(f'Failed to read {bucket_name} bucket, using monthly WARC file listings')
        s3_client = None

    objects = []

    if s3_client:
        def s3_list_objects(bucket, prefix):
            response = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
            if 'Contents' not in response:
                return []
            return [x['Key'] for x in response['Contents']]

        if warc_files_start_date or warc_files_end_date:
            # The news files are grouped per year and month in separate folders
            warc_dates = __iterate_by_month(start_date=warc_files_start_date, end_date=warc_files_end_date)
            for date in warc_dates:
                year = date.strftime('%Y')
                month = date.strftime('%m')
                prefix = f'crawl-data/CC-NEWS/{year}/{month}/'
                logger.debug(f'Listing objects on S3 bucket {bucket_name} and prefix {prefix}')
                objects += s3_list_objects(bucket_name, prefix)
        else:
            objects = s3_list_objects(bucket_name, 'crawl-data/CC-NEWS/')

    else:
        # The news files are grouped per year and month in separate folders
        warc_dates = __iterate_by_month(start_date=warc_files_start_date, end_date=warc_files_end_date)
        for date in warc_dates:
            year = date.strftime('%Y')
            month = date.strftime('%m')
            url = f'{CC_BASE_URL}/crawl-data/CC-NEWS/{year}/{month}/warc.paths.gz'
            logger.debug(f'Fetching WARC paths listing {url}')
            response = requests.get(url)
            if response:
                objects += gzip.decompress(response.content).decode('ascii').strip().split('\n')
            else:
                logger.info(f'Failed to fetch WARC file list {url}: {response}')

    if warc_files_start_date or warc_files_end_date:
        # Now filter further on day of month, hour, minute
        objects = [
            p for p in objects if __date_within_period(
                __extract_date_from_warc_filename(p),
                start_date=warc_files_start_date,
                end_date=warc_files_end_date,
            )
        ]

    logger.info(f'Found {len(objects)} WARC files')

    return objects

def from_warc(record, fetch_images: bool = False):
    return NewsPlease.from_warc(record, decode_errors="strict", fetch_images=fetch_images)


def on_valid_article_extracted(article: Dict) -> Dict:
    r""" This function will be invoked for each article that was extracted successfully
    from the archived data and that satisfies the filter criteria.
    """
    # UUID = hashlib.sha256(article.filename.encode()).hexdigest()[:32]

    # keep only interesting fields
    article = {k: v for k, v in article.__dict__.items() if k in KEYS_TO_KEEP}
    # article_dict['uuid'] = UUID
    return article


def download(
    path: str,
    temporary_directory: str,
    position: int = None,
    retry_time: int = 120,
    s3_client=None,
    bucket_name: str = CC_BASE_BUCKET,
):
    r""" Download and save a file locally. """
    local_filename = urllib.parse.quote_plus(path)
    local_filepath = os.path.join(temporary_directory, local_filename)

    # cleanup
    try:
        os.remove(local_filepath)
    except OSError:
        pass

    while True:

        try:

            if s3_client is not None:
                logger.info(f"Downloading file {path} to {local_filepath} with S3")
                with open(local_filepath, 'wb') as file_obj:
                    s3_client.download_fileobj(bucket_name, path, file_obj)

            else:
                # download
                url = f"{CC_BASE_URL}/{path}"
                logger.info(f'Downloading {path} to {local_filepath} with HTTPS')

                response = requests.get(url, stream=True)
                total_size_in_bytes = response.headers.get('content-length', None)
                total_size_in_bytes = int(total_size_in_bytes) if total_size_in_bytes is not None else total_size_in_bytes

                with DownloadProgress(total_size_in_bytes, local_filepath, position=position) as prog_bar:
                    with open(local_filepath, 'wb') as fo:
                        for data in response.iter_content(16 * 1024 * 1024):
                            prog_bar(len(data))
                            fo.write(data)

                if response.status_code != 200:
                    raise Exception(f'Not OK status code received: {response.status_code}')

        except Exception as e:
            logger.warning(e)
            logger.warning(f"A connection error occurred for URL {url}, retrying in {retry_time} seconds...")
            time.sleep(retry_time)
        else:
            break

    logger.info(f'Download completed, local file: {local_filepath}')
    return local_filepath
