import datetime
import logging
import os
from argparse import ArgumentParser, Namespace
from functools import partial
from multiprocessing import Pool, cpu_count
from typing import Dict, Generator, List

from datasets import Dataset
from tqdm import tqdm

from datasets_news_please.extractor import IterableCommonCrawlExtractor
from datasets_news_please.utils import get_remote_index


# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('datasets_news_please')

# makes other loggers quiet
logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('readability').setLevel(logging.CRITICAL)
logging.getLogger('PIL').setLevel(logging.CRITICAL)
logging.getLogger('newspaper').setLevel(logging.CRITICAL)
logging.getLogger('newsplease').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('jieba').setLevel(logging.CRITICAL)


DEFAULT_TEMP_DIR = '/tmp/datasets_news_please/'


def extraction_function(
    warc_path: str = None,
    include_hosts: List[str] = None,
    exclude_hosts: List[str] = None,
    start_date: datetime.datetime = None,
    end_date: datetime.datetime = None,
    language: str = 'en',
    strict_date: bool = False,
    temporary_directory: str = DEFAULT_TEMP_DIR,
) -> List[Dict]:
    r""" Extract a single warc files and return results as a list of dictionaries. """

    commoncrawl_extractor = IterableCommonCrawlExtractor()
    commoncrawl_extractor.extract_from_commoncrawl(
        warc_path,
        include_hosts=include_hosts,
        exclude_hosts=exclude_hosts,
        start_date=start_date,
        end_date=end_date,
        language=language,
        strict_date=strict_date,
        temporary_directory=temporary_directory,
    )


def processor(warc_paths: List[str] = [], **kwargs) -> Generator[Dict, None, None]:
    r""" Takes a list of warc files. Start multiprocessing pool, update a progress bar
    and returns an iterable of dictionaries containing the new articles examples. """
    # run the crawler in the current, single process if number of extraction processes is set to 1
    extraction_fn = partial(extraction_function, **kwargs)

    stats = dict(
        counter_article_passed=0,
        counter_article_discarded=0,
        counter_article_error=0,
        counter_article_total=0,
        counter_warc_skipped=0,
        counter_warc_processed=0,
    )

    general_progress_bar = tqdm(desc='Total progress', total=len(warc_paths), unit='warcs', smoothing=0.2, position=1)
    with Pool(args.num_workers) as extraction_process_pool:
        for results, new_stats in extraction_process_pool.imap(extraction_fn, warc_paths, chunksize=None):
            general_progress_bar.update(1)

            # update general statistics
            for k, v in new_stats.items():
                stats[k] += v

            # yield results
            yield from results

    logger.info('Processing finished...')
    logger.info('Statistics:')
    for k, v in stats.items():
        logger.info(f'- {k}: {v}')


def main(args: Namespace):

    logger.info('Starting Datasets CC-News Extractor...')
    logger.info(f'Temporary download directory for warc files: {args.temp_warc_dir}')

    os.makedirs(args.temp_warc_dir, exist_ok=True)
    assert not os.path.exists(args.output_folder)

    article_start_date = datetime.datetime.strptime(args.article_start_date, '%Y-%m-%d') if args.article_start_date else None  # noqa: E501
    article_end_date = datetime.datetime.strptime(args.article_end_date, '%Y-%m-%d') if args.article_end_date else None

    warc_start_date = datetime.datetime.strptime(args.warc_start_date, '%Y-%m-%d') if args.warc_start_date else None
    warc_end_date = datetime.datetime.strptime(args.warc_end_date, '%Y-%m-%d') if args.warc_end_date else None

    logger.info('Getting listing of WARC files.')
    cc_news_crawl_names = get_remote_index(warc_start_date, warc_end_date)
    number_of_warc_files_on_cc = len(cc_news_crawl_names)
    logger.info(f'Found {number_of_warc_files_on_cc} WARC files.')

    logger.info(f'Creating extraction process pool with {args.num_workers} processes...')
    logger.info('Starting dataset generation...')

    dataset = Dataset.from_generator(
        cc_news_crawl_names,
        keep_in_memory=False,
        gen_kwargs=dict(
            warc_paths=cc_news_crawl_names,
            include_hosts=args.include_hosts,
            exclude_hosts=args.exlude_hosts,
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
    parser.add_argument('--num_workers', type=int, required=False, default=cpu_count())
    args = parser.parse_args()
    main(args)
