# import hashlib
import logging
import os
import time
import urllib
import urllib.parse
from typing import Dict

import requests
from requests.exceptions import ConnectionError
from dateutil import parser
from newsplease.crawler.commoncrawl_extractor import NewsPlease, commoncrawl_crawler
from tqdm import tqdm


# set own logger
logger = logging.getLogger('datasets_news_please')

# commoncrawl.org
CC_BASE_URL = 'https://data.commoncrawl.org/'

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


def get_remote_index():
    r""" Gets the index of news crawl files from commoncrawl.org and returns an array of names. """
    return commoncrawl_crawler.__get_remote_index()


def from_warc(record):
    return NewsPlease.from_warc(record, decode_errors="strict", fetch_images=False)


def on_valid_article_extracted(article: Dict) -> Dict:
    r""" This function will be invoked for each article that was extracted successfully
    from the archived data and that satisfies the filter criteria.
    """
    # UUID = hashlib.sha256(article.filename.encode()).hexdigest()[:32]

    # keep only interesting fields
    article_dict = {k: v for k, v in article.__dict__.items() if k in KEYS_TO_KEEP}
    # article_dict['uuid'] = UUID
    return article_dict


def download(path: str, temporary_directory: str, position: int = None, retry_time: int = 30):
    r""" Download and save a file locally. """
    local_filename = urllib.parse.quote_plus(path)
    local_filepath = os.path.join(temporary_directory, local_filename)

    # download
    url = CC_BASE_URL + path
    logger.debug(f'Downloading {url} to {local_filepath}')

    while True:
    
        # cleanup
        try:
            os.remove(local_filepath)
        except OSError:
            pass

        try:
            response = requests.get(url, stream=True)
            total_size_in_bytes = response.headers.get('content-length', None)
            total_size_in_bytes = int(total_size_in_bytes) if total_size_in_bytes is not None else total_size_in_bytes

            with DownloadProgress(
                total_size_in_bytes, local_filepath, position=position, disable=True,
            ) as progress_bar:
                with open(local_filepath, 'wb') as file:
                    for data in response.iter_content(1024 * 1024):
                        progress_bar(len(data))
                        file.write(data)

        except ConnectionError:
            logger.warning(f"A connection error occurred for URL {url}, retrying in {retry_time} seconds...")
            time.sleep(retry_time)
        else:
            break

    logger.debug(f'Download completed, local file: {local_filepath}')
    return local_filepath
