import datetime
import logging
import os
import sys
from typing import Dict, Generator, List
import jieba
import boto3
from newsplease.crawler.commoncrawl_extractor import EmptyResponseError, configure_logging
from tqdm import tqdm
from warcio.archiveiterator import ArchiveIterator
import botocore

from datasets_news_please.utils import (
    CC_BASE_BUCKET,
    download,
    from_warc,
    get_publishing_date,
    get_publishing_language,
    on_valid_article_extracted,
)


# set own logger
logger = logging.getLogger('datasets_news_please')

# make other loggers quiet
configure_logging({"LOG_LEVEL": "ERROR"})
logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('readability').setLevel(logging.CRITICAL)
logging.getLogger('PIL').setLevel(logging.CRITICAL)
logging.getLogger('newspaper').setLevel(logging.CRITICAL)
logging.getLogger('newsplease').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('fsspec.local').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
jieba.setLogLevel(logging.CRITICAL)
boto3.set_stream_logger('botocore', logging.CRITICAL)
boto3.set_stream_logger('boto3', logging.CRITICAL)


class IterableCommonCrawlExtractor:

    # remote url where we can download the warc file
    warc_path = None

    # hosts (if None or empty list, any host is OK)
    filter_include_hosts = None  # example: ['elrancaguino.cl']
    filter_exclude_hosts = None  # example: ['elrancaguino.cl']

    # start and end date (if None, any date is OK), as datetime
    # if date filtering is string, e.g., if we could not detect the date of an article, we will discard the article
    filter_start_date = None
    filter_end_date = None
    filter_strict_date = True

    # fetch images (may take a lot of space)
    fetch_images = False

    # limit extracted articles for debugging
    limit = None

    def __init__(self, temporary_directory: str = None, process_id: int = None, bucket_name: str = CC_BASE_BUCKET):
        r""" Crawl and extract articles form the news crawl provided by commoncrawl.org. """

        self.temporary_directory = temporary_directory
        os.makedirs(self.temporary_directory, exist_ok=True)
        self.process_id = process_id
        self.bucket_name = bucket_name

        s3_client = boto3.client('s3')
        # Verify access to commoncrawl bucket
        try:
            s3_client.head_bucket(Bucket=self.bucket_name)
            self.s3_client = s3_client
        except (botocore.exceptions.ClientError, botocore.exceptions.NoCredentialsError):
            logger.info(f'Failed to read {self.bucket_name} bucket, using monthly WARC file listings')
            self.s3_client = None

    def filter_record(self, warc_record, article=None):
        r"""
        Returns true if a record passes all tests: hosts, publishing date
        :param warc_record:
        :return: A tuple of (True or False) and an article (might be None)
        """

        # filter by host
        if self.filter_include_hosts:
            url = warc_record.rec_headers.get_header('WARC-Target-URI')

            # very simple check, check if one of the required host names is contained in the url of the WARC transaction
            # better would be to extract the host name from the WARC transaction Target URI and then check for equality
            # because currently something like g.co?forward_url=facebook.com would yield a positive filter test for
            # facebook.com even though the actual host is g.co
            for valid_host in self.filter_include_hosts:
                if valid_host in url:
                    break
            else:
                return False, article

        if self.filter_exclude_hosts:
            url = warc_record.rec_headers.get_header('WARC-Target-URI')

            # very simple check, check if one of the required host names is contained in the url of the WARC transaction
            # better would be to extract the host name from the WARC transaction Target URI and then check for equality
            # because currently something like g.co?forward_url=facebook.com would yield a positive filter test for
            # facebook.com even though the actual host is g.co
            for invalid_host in self.filter_exclude_hosts:
                if invalid_host in url:
                    return False, article

        # filter by date
        if self.filter_start_date or self.filter_end_date:
            if not article:
                article = from_warc(warc_record, fetch_images=self.fetch_images)

            publishing_date = get_publishing_date(warc_record, article)
            if not publishing_date:
                if self.filter_strict_date:
                    return False, article
            else:  # here we for sure have a date
                # is article published too early?
                if self.filter_start_date and publishing_date < self.filter_start_date:
                    return False, article
                if self.filter_end_date and publishing_date > self.filter_end_date:
                    return False, article

        # filter on language
        if self.filter_on_language is not None:
            if not article:
                article = from_warc(warc_record, fetch_images=self.fetch_images)
            original_language = get_publishing_language(warc_record, article)

            if not original_language:
                return False, article

            else:  # here we for sure have a language
                # is article published in another language
                if self.filter_on_language and original_language != self.filter_on_language:
                    return False, article

        return True, article

    def process_warc_gz_file(self, path_name: str) -> Generator[Dict, None, None]:
        r""" Iterates all transactions in one WARC file and for each transaction tries to extract an article object.
        Returns a generator of newly extracted documents. """
    
        position = self.process_id + 1

        with open(path_name, 'rb') as stream:
            for index, record in tqdm(
                enumerate(ArchiveIterator(stream)),
                desc=f"Extraction {self.process_id}",
                unit="articles",
                position=position,
                disable=True,
            ):
                try:
                    if record.rec_type == 'response':
                        # if the article passes filter tests, we notify the user
                        try:
                            filter_pass, article = self.filter_record(record)
                        except (UnicodeDecodeError, EmptyResponseError):
                            filter_pass = False
                            article = None

                        if filter_pass:
                            try:
                                if not article:
                                    article = from_warc(record, fetch_images=self.fetch_images)
                            except (UnicodeDecodeError, EmptyResponseError):
                                filter_pass = False

                        if filter_pass:
                            logger.debug(
                                f'article pass ({article.source_domain}; {article.date_publish}; {article.title})'
                            )
                            article = on_valid_article_extracted(article)
                            yield article
                        else:
                            if article:
                                logger.debug(
                                    f'article discard ({article.source_domain}; '
                                    f'{article.date_publish}; {article.title})'
                                )
                            else:
                                logger.debug(f'article discard ({record.rec_headers.get_header("WARC-Target-URI")})')

                except:  # noqa E722
                    logger.warning(f'Unexpected error extracting article: {sys.exc_info()[0]} ({sys.exc_info()[1]})')
                    logger.warning(sys.exc_info()[2], exc_info=True)

                finally:
                    if self.limit is not None and index >= self.limit:
                        break

        # cleanup
        logger.debug(f'removing fully extracted warc {path_name}')
        os.remove(path_name)

    def extract_from_commoncrawl(
        self,
        warc_path: str,
        include_hosts: List[str] = None,
        exclude_hosts: List[str] = None,
        start_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
        language: str = 'en',
        strict_date: bool = False,
        fetch_images: bool = False,
        limit: int = None,
    ) -> Generator[Dict, None, None]:
        r""" Crawl and extract articles form the news crawl provided by commoncrawl.org. """
        self.warc_path = warc_path
        self.filter_include_hosts = include_hosts
        self.filter_exclude_hosts = exclude_hosts

        self.filter_start_date = start_date
        self.filter_end_date = end_date
        self.filter_on_language = language
        self.filter_strict_date = strict_date

        self.fetch_images = fetch_images

        self.limit = limit

        local_path_name = download(
            self.warc_path,
            self.temporary_directory,
            position=self.process_id + 1,
            s3_client=self.s3_client,
            bucket_name=self.bucket_name,
        )
        yield from self.process_warc_gz_file(local_path_name)
