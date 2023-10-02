# datasets-news-please
Download Common Crawl data directly into an HuggingFace dataset.

## Usage

```bash
python -m datasets_news_please \
    --output_folder </path/to/output/folder> \
    --warc_start_date <YYYY-MM-DD> \
    --warc_end_date <YYYY_MM_DD> \
    --num_workers <number of CPUs> 
```

The above script crawls and downloads articles collected between `warc_start_date` and `warc_end_date` from CC-News. Notice that `warc_start_date` and `warc_end_date` filter the data based on the date they were collected, and not the date the articles were published. To filter on the articles publish date, use `article_start_date` and `article_end_date`.

List of all arguments:
- `--output_folder </path/to/directory>` (required): where the final dataset will be saved;
- `--temp_warc_dir </path/to/directory>`: directory where WARC files are downloaded for processing;
- `--include_hosts <host1> <host2> ...`: include only articles from these hosts;
- `--exclude_hosts <host1> <host2> ...`: exclude articles from these hosts;
- `--article_start_date <YYYY-MM-DD>`: keep articles published after this date;
- `--article_end_date <YYYY-MM-DD>`: keep articles published before this date;
- `--article_strict_date`: if used, remove articles without a publishing date;
- `--warc_start_date <YYYY-MM-DD>`: process WARC files published after this date;
- `--warc_end_date <YYYY-MM-DD>`: process WARC files published before this date;
- `--language <language code>`: keep only articles in this language;
- `--num_workers <number of CPUs>`: the number of CPUs to use, the larger the better;

## Credits

This project is an adaptation of the original [news-please](https://github.com/fhamborg/news-please) framework by [fhamborg](https://github.com/fhamborg).
