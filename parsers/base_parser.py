"""Base parser that is parent to parsers for particular websites"""
import asyncio
import csv
from datetime import datetime, timedelta
from io import StringIO
from typing import List, Dict

from parsers.saving_to_csv import save_to_csv, prepare_csv, does_file_exist, get_filename
from parsers.url_fetcher import fetch


class BaseParser:
    def __init__(self, base_url: str, parser_name: str, fieldnames: list, verbose: bool = False):
        """Set main fields"""
        self.verbose = verbose
        self.base_url = base_url
        self.fieldnames = fieldnames
        self.parser_name = parser_name

    async def run(self, start_date: str, end_date: str, day_limit: int, output_filename: str = "",
                  concurrency_rate: int = 10, continue_database: bool = False) -> None:
        """Scrapes and parses the website with given parameters"""
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        current_date = start_date
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        if not output_filename:
            output_filename = get_filename(self.parser_name, start_date, end_date)

        self._validate_run(start_date, end_date, output_filename, continue_database)

        if continue_database:
            current_date = self._read_last_dataset_date(output_filename) + timedelta(days=1)

        if not continue_database:
            prepare_csv(output_filename, self.fieldnames)

        while current_date <= end_date:
            if self.verbose:
                print(f"Processing {current_date.strftime('%Y-%m-%d')}")

            archive_pages_urls = self._get_archive_pages_urls(current_date, day_limit)
            archive_pages_htmls = []

            for i in range(0, len(archive_pages_urls), concurrency_rate):
                archive_pages_futures = [fetch(archive_page) for archive_page in archive_pages_urls[i:i + concurrency_rate]]
                archive_pages_htmls += await asyncio.gather(*archive_pages_futures)

            articles_urls = []

            for archive_page_html in archive_pages_htmls:
                articles_urls += self._find_articles(archive_page_html)

            articles_urls = list(set(articles_urls))

            if self.verbose:
                print(f"Found {len(articles_urls)} articles")

            articles_htmls = []

            for i in range(0, len(articles_urls), concurrency_rate):
                articles_futures = [fetch(article_url) for article_url in articles_urls[i:i + concurrency_rate]]
                articles_htmls += await asyncio.gather(*articles_futures)

            articles_info = []

            for article_html in articles_htmls:
                article_dict = self._parse_article_html(article_html)

                if not article_dict:
                    continue

                article_dict["date"] = current_date.strftime("%Y/%m/%d")
                articles_info.append(article_dict)

            for article_dict in articles_info:
                save_to_csv([article_dict], output_filename)

            current_date += timedelta(days=1)

    def _get_archive_pages_urls(self, date: datetime, page_count: int) -> list:
        """Find or generate archive pages for a given date."""
        pass

    def _find_articles(self, html: str) -> List[str]:
        """Find all links to articles on the page and return list of links to them."""
        pass

    def _parse_article_html(self, html: str) -> Dict[str, str]:
        """Parse article html and return dict with article title, text, topic and subtopic."""
        pass

    def _read_last_dataset_date(self, filename: str) -> datetime:
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                pass
            last_line = line

            csv_input = csv.reader(StringIO(last_line, newline=''), delimiter=',')
            rows = list(csv_input)

            date = rows[0][self.fieldnames.index('date')]
            return datetime.strptime(date, "%Y/%m/%d")

    def _validate_run(self, start_date: datetime, end_date: datetime, output_filename: str = "",
                      continue_database: bool = False):
        """Validate run configuration."""
        if end_date > datetime.now():
            raise ValueError("End date is in the future.")

        if end_date < start_date:
            raise ValueError("End date is earlier than start date.")

        if continue_database and not does_file_exist(output_filename):
            raise ValueError("Nowhere to append data.")

        if not continue_database and does_file_exist(output_filename):
            answer = input(f"Filename {output_filename} already exists. Do you want to rewrite the file? Write yN: ")
            if answer.lower() == 'y':
                print("Rewriting the file...")
            else:
                raise FileExistsError("Set continue_database to True to continue parsing the website.")

        if 'date' not in self.fieldnames:
            raise ValueError("Fieldnames must have 'date' column.")

        if 'text' not in self.fieldnames:
            raise ValueError("Fieldnames must have 'text' column.")