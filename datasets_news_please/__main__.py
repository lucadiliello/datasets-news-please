import datetime
import logging
import os
import warnings
from argparse import ArgumentParser, Namespace
from typing import Dict, Generator, List

from datasets import Dataset, disable_caching
from multiprocess import current_process
from newsplease.crawler.commoncrawl_crawler import __get_remote_index
from tqdm import tqdm

from datasets_news_please.extractor import IterableCommonCrawlExtractor


os.environ['REUSE_DATASET_IF_EXISTS'] = '0'

# logging
logger = logging.getLogger('datasets_news_please')

# suppress all warning from BeautifoulSoup and others
warnings.filterwarnings("ignore")

DEFAULT_TEMP_DIR = '/tmp/datasets_news_please/'
LOGGING_STR_TO_ID = dict(
    debug=logging.DEBUG,
    info=logging.INFO,
    warning=logging.WARNING,
    error=logging.ERROR,
)


def extraction_function(
    warc_path: str = None,
    include_hosts: List[str] = None,
    exclude_hosts: List[str] = None,
    start_date: datetime.datetime = None,
    end_date: datetime.datetime = None,
    language: str = 'en',
    strict_date: bool = False,
    temporary_directory: str = DEFAULT_TEMP_DIR,
    process_id: int = 0,
) -> Generator[Dict, None, None]:
    r""" Extract a single warc files and return results as a list of dictionaries. """

    commoncrawl_extractor = IterableCommonCrawlExtractor(temporary_directory, process_id)
    yield from commoncrawl_extractor.extract_from_commoncrawl(
        warc_path,
        include_hosts=include_hosts,
        exclude_hosts=exclude_hosts,
        start_date=start_date,
        end_date=end_date,
        language=language,
        strict_date=strict_date,
    )


def processor(warc_paths: List[str] = [], **kwargs) -> Generator[Dict, None, None]:
    r""" Takes a list of warc files. Start multiprocessing pool, update a progress bar
    and returns an iterable of dictionaries containing the new articles examples. """
    # run the crawler in the current, single process if number of extraction processes is set to 1
    process = current_process()._identity
    process_id = process[0] if len(process) > 0 else 0

    # position progress bar on top of all extraction processes
    position = process_id + 1

    for warc_path in tqdm(
        warc_paths,
        desc=f'Progress {process_id}',
        unit='warcs',
        smoothing=0.2,
        position=position,
    ):
        yield from extraction_function(warc_path, **kwargs, process_id=process_id)

    logger.info(f'Processor {process_id} finished successfully...')


def main(args: Namespace):

    # setting log level
    logger.setLevel(LOGGING_STR_TO_ID[args.logging_level])

    logger.info('Starting Datasets CC-News Extractor...')
    logger.info(f'Temporary download directory for warc files: {args.temp_warc_dir}')

    os.makedirs(args.temp_warc_dir, exist_ok=True)
    assert not os.path.exists(args.output_folder)

    article_start_date = datetime.datetime.strptime(args.article_start_date, '%Y-%m-%d') if args.article_start_date else None  # noqa: E501
    article_end_date = datetime.datetime.strptime(args.article_end_date, '%Y-%m-%d') if args.article_end_date else None

    warc_start_date = datetime.datetime.strptime(args.warc_start_date, '%Y-%m-%d') if args.warc_start_date else None
    warc_end_date = datetime.datetime.strptime(args.warc_end_date, '%Y-%m-%d') if args.warc_end_date else None

    logger.info('Getting listing of WARC files.')
    cc_news_crawl_names = __get_remote_index(warc_files_start_date=warc_start_date, warc_files_end_date=warc_end_date)[:2]
    number_of_warc_files_on_cc = len(cc_news_crawl_names)
    logger.info(f'Found {number_of_warc_files_on_cc} WARC files.')

    logger.info(f'Creating extraction process pool with {args.num_workers} processes...')
    logger.info('Starting dataset generation...')

    # need tuple to avoid datasets generator from splitting it over processes
    if args.include_hosts is not None:
        args.include_hosts = tuple(args.include_hosts)
    if args.exclude_hosts is not None:
        args.exclude_hosts = tuple(args.exclude_hosts)

    disable_caching()
    dataset = Dataset.from_generator(
        processor,
        keep_in_memory=False,
        gen_kwargs=dict(
            warc_paths=cc_news_crawl_names,
            include_hosts=args.include_hosts,
            exclude_hosts=args.exclude_hosts,
            start_date=article_start_date,
            end_date=article_end_date,
            language=args.language,
            strict_date=args.article_strict_date,
            temporary_directory=args.temp_warc_dir,
        ),
        num_proc=args.num_workers,
    )

    logger.info(f'Finished generating dataset containing {len(dataset)} examples.')
    logger.info('Saving to disk...')
    dataset.save_to_disk(args.output_folder)


if __name__ == "__main__":

    parser = ArgumentParser("Datasets CC-News Extractor")
    parser.add_argument('--temp_warc_dir', type=str, required=False, default=DEFAULT_TEMP_DIR)
    parser.add_argument('--output_folder', type=str, required=True)

    # filter hosts
    parser.add_argument('--include_hosts', type=str, nargs='+', required=False, default=None)
    parser.add_argument('--exclude_hosts', type=str, nargs='+', required=False, default=None)

    # filter article date
    parser.add_argument('--article_start_date', type=str, required=False, default=None, help="Date as YYYY-MM-DD")
    parser.add_argument('--article_end_date', type=str, required=False, default=None, help="Date as YYYY-MM-DD")
    # if date filtering is strict and news-please could not detect the date of an article, the article will be discarded
    parser.add_argument('--article_strict_date', action="store_true")

    # filter WARC file date
    parser.add_argument('--warc_start_date', type=str, required=False, default=None, help="Date as YYYY-MM-DD")
    parser.add_argument('--warc_end_date', type=str, required=False, default=None, help="Date as YYYY-MM-DD")

    # filter language
    parser.add_argument('--language', type=str, required=False, default=None)

    # mixed arguments
    parser.add_argument('--num_workers', type=int, required=False, default=None)
    parser.add_argument('--logging_level', type=str, default='info', choices=('info', 'debug', 'warning', 'error'))
    args = parser.parse_args()
    main(args)
