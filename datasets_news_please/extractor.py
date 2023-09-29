import datetime
import hashlib
import logging
import os
import sys
import urllib
from typing import Dict, List, Tuple

from newsplease.crawler.commoncrawl_extractor import CommonCrawlExtractor, EmptyResponseError
# from six.moves import urllib
from warcio.archiveiterator import ArchiveIterator


logger = logging.getLogger('datasets_news_please')


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


class IterableCommonCrawlExtractor(CommonCrawlExtractor):

    # remote url where we can download the warc file
    __warc_path = None

    # hosts (if None or empty list, any host is OK)
    __filter_include_hosts = None  # example: ['elrancaguino.cl']
    __filter_exclude_hosts = None  # example: ['elrancaguino.cl']

    # start and end date (if None, any date is OK), as datetime
    # if date filtering is string, e.g., if we could not detect the date of an article, we will discard the article
    __filter_start_date = None
    __filter_end_date = None
    __filter_strict_date = True

    # commoncrawl.org
    __cc_base_url = 'https://data.commoncrawl.org/'

    # ignore unicode errors and whether to gather images from source
    __ignore_unicode_errors = True
    __fetch_images = False

    # if the download progress is shown
    __show_download_progress = False

    def on_valid_article_extracted(article: Dict) -> Dict:
        r""" This function will be invoked for each article that was extracted successfully
        from the archived data and that satisfies the filter criteria.
        """
        UUID = hashlib.sha256(article.filename.encode()).hexdigest()[:32]

        # keep only interesting fields
        article_dict = {k: v for k, v in article.__dict__.items() if k in KEYS_TO_KEEP}
        article_dict['uuid'] = UUID
        return article_dict

    def __setup(self):
        r""" Just create needed folders. """
        os.makedirs(self.__temporary_directory, exist_ok=True)

    def filter_record(self, warc_record, article=None):
        r"""
        Returns true if a record passes all tests: hosts, publishing date
        :param warc_record:
        :return: A tuple of (True or False) and an article (might be None)
        """
        # filter by host
        if self.__filter_include_hosts:
            url = warc_record.rec_headers.get_header('WARC-Target-URI')

            # very simple check, check if one of the required host names is contained in the url of the WARC transaction
            # better would be to extract the host name from the WARC transaction Target URI and then check for equality
            # because currently something like g.co?forward_url=facebook.com would yield a positive filter test for
            # facebook.com even though the actual host is g.co
            for valid_host in self.__filter_include_hosts:
                if valid_host in url:
                    break
            else:
                return False, article
        
        if self.__filter_exclude_hosts:
            url = warc_record.rec_headers.get_header('WARC-Target-URI')

            # very simple check, check if one of the required host names is contained in the url of the WARC transaction
            # better would be to extract the host name from the WARC transaction Target URI and then check for equality
            # because currently something like g.co?forward_url=facebook.com would yield a positive filter test for
            # facebook.com even though the actual host is g.co
            for invalid_host in self.__filter_exclude_hosts:
                if invalid_host in url:
                    return False, article

        # filter by date
        if self.__filter_start_date or self.__filter_end_date:
            if not article:
                article = self._from_warc(warc_record)

            publishing_date = self.__get_publishing_date(warc_record, article)
            if not publishing_date:
                if self.__filter_strict_date:
                    return False, article
            else:  # here we for sure have a date
                # is article published too early?
                if self.__filter_start_date and publishing_date < self.__filter_start_date:
                    return False, article
                if self.__filter_end_date and publishing_date > self.__filter_end_date:
                    return False, article

        # filter on language
        if self.__filter_on_language is not None:
            if not article:
                article = self._from_warc(warc_record)

            original_language = self.__get_publishing_language(warc_record, article)
            if not original_language:
                return False, article

            else:  # here we for sure have a language
                # is article published in another language
                if self.__filter_on_language and original_language != self.__filter_on_language:
                    return False, article

        return True, article

    def __download(self, path):
        """
        Download and save a file locally.
        :param url: Where to download from
        :return: File path name of the downloaded file
        """
        local_filename = urllib.parse.quote_plus(path)
        local_filepath = os.path.join(self.__temporary_directory, local_filename)

        # cleanup
        try:
            os.remove(local_filepath)
        except OSError:
            pass

        # download
        url = self.__cc_base_url + path
        self.__logger.info('downloading %s (local: %s)', url, local_filepath)
        urllib.request.urlretrieve(url, local_filepath, reporthook=self.__on_download_progress_update)
        self.__logger.info('download completed, local file: %s', local_filepath)
        return local_filepath

    def __process_warc_gz_file(self, path_name) -> Tuple[List[Dict], Dict]:
        r"""
        Iterates all transactions in one WARC file and for each transaction tries to extract an article object.
        Returns the list of extracted and filtered articles along with a dictionary of the statistics. """
    
        counter_article_total = 0
        counter_article_passed = 0
        counter_article_discarded = 0
        counter_article_error = 0

        # final list of articles to be returned
        res = []

        with open(path_name, 'rb') as stream:
            for record in ArchiveIterator(stream):
                try:
                    if record.rec_type == 'response':
                        counter_article_total += 1

                        # if the article passes filter tests, we notify the user
                        try:
                            filter_pass, article = self.filter_record(record)
                        except (UnicodeDecodeError, EmptyResponseError):
                            filter_pass = False
                            article = None

                        if filter_pass:
                            try:
                                if not article:
                                    article = self._from_warc(record)
                            except (UnicodeDecodeError, EmptyResponseError):
                                filter_pass = False

                        if filter_pass:
                            counter_article_passed += 1
                            logger.debug(
                                f'article pass ({article.source_domain}; {article.date_publish}; {article.title})'
                            )
                            article = self.on_valid_article_extracted(article)
                            res.append(article)
                        else:
                            counter_article_discarded += 1
                            if article:
                                logger.debug(
                                    f'article discard ({article.source_domain}; '
                                    f'{article.date_publish}; {article.title})'
                                )
                            else:
                                logger.info(f'article discard ({record.rec_headers.get_header("WARC-Target-URI")})')

                except:  # noqa E722
                    logger.debug(f'Unexpected error extracting article: {sys.exc_info()[0]} ({sys.exc_info()[1]})')
                    logger.debug(sys.exc_info()[2], exc_info=True)
                    counter_article_error += 1

        # cleanup
        logging.debug(f'removing fully extracted warc {path_name}')
        os.remove(path_name)

        statistics = dict(
            counter_article_total=counter_article_total,
            counter_article_passed=counter_article_passed,
            counter_article_discarded=counter_article_discarded,
            counter_article_error=counter_article_error,
        )

        return res, statistics

    def __run(self) -> Tuple[List[Dict], Dict]:
        r"""
        Main execution method, which consists of: get an up-to-date list of WARC files, and for each of them: download
        and extract articles. Each article is checked against a filter. Finally, for each valid article is
        reduced to a set of key-value pairs that are relevant. A dict with the relevant statistics is returned along
        with the produced list of articles.
        """
        self.__setup()

        local_path_name = self.__download(self.__warc_path)
        return self.__process_warc_gz_file(local_path_name)

    def extract_from_commoncrawl(
        self,
        warc_path: str,
        include_hosts: List[str] = None,
        exclude_hosts: List[str] = None,
        start_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
        language: str = 'en',
        strict_date: bool = False,
        temporary_directory: str = None,
    ) -> List[Dict]:
        r""" Crawl and extract articles form the news crawl provided by commoncrawl.org. """

        self.__warc_path = warc_path
        self.__filter_include_hosts = include_hosts
        self.__filter_exclude_hosts = exclude_hosts

        self.__filter_start_date = start_date
        self.__filter_end_date = end_date
        self.__filter_on_language = language
        self.__filter_strict_date = strict_date
        self.__temporary_directory = temporary_directory

        return self.__run()
