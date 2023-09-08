import asyncio
from datetime import datetime
from typing import List, Dict

from base_parser import BaseParser

from bs4 import BeautifulSoup


class KommersantParser(BaseParser):
    def __init__(self, verbose: bool):
        super().__init__(base_url="https://www.kommersant.ru", parser_name='kommersant',
                         fieldnames=['date', 'title', 'text', 'topic'], verbose=verbose)

    def _get_archive_pages_urls(self, date: datetime, page_count: int) -> list:
        archive_urls = []

        for i in range(2, 10):
            archive_urls.append(f"{self.base_url}/archive/rubric/{i}/day/{date.strftime('%Y-%m-%d')}?page=1")

        return archive_urls

    def _find_articles(self, html: str) -> List[str]:
        doc_tree = BeautifulSoup(html, 'html.parser')
        news_list = doc_tree.find("div", {"class": "rubric_lenta"})
        if not news_list:
            return []
        return [f"{self.base_url}{link['href']}" for link in news_list.find_all('a') if 'doc/' in link.get('href', '')]

    def _parse_article_html(self, html: str) -> Dict[str, str]:
        doc_tree = BeautifulSoup(html, 'html.parser')

        title = doc_tree.find("h1", {"class": "doc_header__name js-search-mark"})
        title = title.get_text().strip() if title else None

        body = doc_tree.find_all("p", {"class": "doc__text"})
        text = " ".join([p.get_text().strip() for p in body])

        if not text:
            return {}

        topic = doc_tree.find("a", {"class": "decor"})
        topic = topic.get_text().strip() if topic else None

        return {'title': title, 'text': text, 'topic': topic}


if __name__ == "__main__":
    parser = KommersantParser(verbose=True)
    asyncio.run(parser.run("2021-01-01", "2021-12-31", 10, concurrency_rate=15, continue_database=True))
