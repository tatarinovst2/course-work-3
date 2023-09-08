"""Script to divide dataset into chunks by year"""
import csv
from datetime import datetime
from pathlib import Path
import sys
from typing import List

from parsers.saving_to_csv import save_to_csv, prepare_csv

from constants import ROOT_PATH

csv.field_size_limit(sys.maxsize)


def divide_dataset_by_year(filename: str | Path, fieldnames: List[str], dataset_name: str, output_directory_path: str | Path) -> None:
    """Divides dataset into chunks by year"""
    with open(filename, "r", encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=',', quotechar='"')
        articles_info = []
        for row in reader:
            if not row["date"]:
                continue
            try:
                datetime.strptime(row["date"], "%Y-%m-%d")
            except ValueError:
                continue
            articles_info.append(row)

    articles_info.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d"))

    years = set([article_info["date"].split("-")[0] for article_info in articles_info])

    for year in years:
        output_filename = Path(output_directory_path) / f"{dataset_name}_{year}.csv"
        prepare_csv(output_filename, fieldnames)
        for article in articles_info:
            if year == article["date"].split("-")[0]:
                save_to_csv([article], output_filename)


if __name__ == "__main__":
    divide_dataset_by_year(ROOT_PATH / "parsers" / "kommersant_dataset" / "kommersant-news.csv",
                           ["date", "title", "text", "topic"],
                           ROOT_PATH / "parsers" / "kommersant_dataset")
