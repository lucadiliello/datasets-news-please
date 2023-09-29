import datetime
import gzip
import logging
import os
from typing import Generator, List
from urllib.parse import urlparse

import requests
from dateutil import parser


logger = logging.getLogger('datasets_news_please')

# commoncrawl.org
CC_BASE_URL = 'https://data.commoncrawl.org/'
CC_S3_BUCKET = 'commoncrawl'

__common_crawl_start_date = datetime.datetime(2016, 8, 26)


def get_publishing_date(article):
    r""" Extracts the publishing date from the article. """
    if article.publish_date:
        return parser.parse(article.publish_date)
    else:
        return None


def get_download_url(name: str):
    r""" Creates a download url given the name. """
    return os.path.join(CC_BASE_URL, name)


def iterate_by_month(
    start_date: datetime.datetime = None, end_date: datetime.datetime = None, month_step: int = 1
) -> Generator[datetime.datetime, None, None]:
    if start_date is None:
        # The starting month of Common Crawl.
        start_date = __common_crawl_start_date

    if end_date is None:
        # Until now.
        end_date = datetime.datetime.today()

    current_date = start_date
    yield current_date

    while True:
        carry, new_month = divmod(current_date.month - 1 + month_step, 12)
        new_month += 1
        current_date = current_date.replace(year=current_date.year + carry, month=new_month)
        yield current_date

        if current_date > end_date:
            break


def extract_date_from_warc_filename(path: str) -> datetime.datetime:
    fn = os.path.basename(path)
    # Assume the filename pattern is CC-NEWS-20160911145202-00018.warc.gz
    fn = fn.replace('CC-NEWS-', '')
    dt = fn.split('-')[0]

    try:
        return datetime.datetime.strptime(dt, '%Y%m%d%H%M%S')
    except:  # noqa E722
        # return date clearly outside the range
        return datetime.datetime(1900, 1, 1)


def date_within_period(
    date: datetime.datetime, start_date: datetime.datetime = None, end_date: datetime.datetime = None
) -> bool:
    if start_date is None:
        # The starting month of Common Crawl.
        start_date = __common_crawl_start_date
    if end_date is None:
        # Until now.
        end_date = datetime.datetime.today()
    return start_date <= date < end_date


def get_remote_index(
    warc_files_start_date: datetime.datetime = None, warc_files_end_date: datetime.datetime = None
) -> List[str]:
    r""" Gets the index of news crawl files from commoncrawl.org and returns an array of names. """

    objects = []

    # The news files are grouped per year and month in separate folders
    warc_dates = iterate_by_month(start_date=warc_files_start_date, end_date=warc_files_end_date)
    for date in warc_dates:
        year = date.strftime('%Y')
        month = date.strftime('%m')
        url = f'{CC_BASE_URL}crawl-data/CC-NEWS/{year}/{month}/warc.paths.gz'

        logger.debug(f'Fetching WARC paths listing {url}')
        response = requests.get(url)

        if response:
            objects += gzip.decompress(response.content).decode('ascii').strip().split('\n')
        else:
            logger.info('Failed to fetch WARC file list %s: %s', url, response)

    if warc_files_start_date or warc_files_end_date:
        # Now filter further on day of month, hour, minute
        objects = [
            p for p in objects if date_within_period(
                extract_date_from_warc_filename(p),
                start_date=warc_files_start_date,
                end_date=warc_files_end_date,
            )
        ]

    return objects


def get_url_path(url_or_path):
    if url_or_path.startswith('http:') or url_or_path.startswith('https:'):
        try:
            url = urlparse(url_or_path)
            return url.path.lstrip('/')  # trim leading slash
        except:  # noqa: E722
            pass
    return url_or_path
