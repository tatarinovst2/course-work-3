"""Script to download articles from https://lenta.ru/news/ using aiohttp and asyncio"""

import asyncio
from datetime import datetime
from typing import List, Dict

from base_parser import BaseParser

from bs4 import BeautifulSoup


class LentaParser(BaseParser):
    def __init__(self, verbose: bool = False):
        super().__init__(base_url="https://lenta.ru/news/", parser_name='lenta',
                       fieldnames=["date", "title", "text", "topic", "subtopic"], verbose=verbose)

    def _get_archive_pages_urls(self, date: datetime, page_count: int) -> list:
        """Find archive pages for a given date."""
        archive_pages_urls = []
        for page_number in range(1, page_count + 1):
            archive_pages_urls.append(f"{self.base_url}{date.strftime('%Y/%m/%d')}/page/{page_number}/")
        return archive_pages_urls

    def _find_articles(self, html: str) -> List[str]:
        """Find all links to https://lenta.ru articles on the page and return list of links to them."""
        doc_tree = BeautifulSoup(html, 'html.parser')
        news_list = doc_tree.find_all("a", "card-full-news _archive")
        return list(set(f"https://lenta.ru{news['href']}" for news in news_list))

    def _parse_article_html(self, html: str) -> Dict[str, str]:
        """Parse article html and return dict with article title, text, topic and subtopic."""
        doc_tree = BeautifulSoup(html, 'html.parser')

        body = doc_tree.find_all("p", {"class": "topic-body__content-text"})

        if not body and self.verbose:
            print(f"Article body is not found")
            return {}

        text = " ".join([p.get_text().strip() for p in body])

        topic = doc_tree.find("div", {"class": "rubric-header__title"})
        topic = topic.get_text().strip() if topic else None

        subtopic = doc_tree.find("a", {"class": "rubric-header__link _active"})
        subtopic = subtopic.get_text().strip() if subtopic and subtopic.get_text() != 'Все' else None

        title = doc_tree.find("span", {"class": "topic-body__title"})
        title = title.get_text().strip() if title else None

        return {"title": title, "text": text, "topic": topic, "subtopic": subtopic}


if __name__ == "__main__":
    parser = LentaParser(verbose=True)
    asyncio.run(parser.run("2022-01-01", "2022-12-31", 10, concurrency_rate=2, continue_database=True))
