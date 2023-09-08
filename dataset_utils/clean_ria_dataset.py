"""Script that removes duplicates from dataset"""
import csv
from typing import List
from pathlib import Path
import sys


csv.field_size_limit(sys.maxsize)

from parsers.saving_to_csv import save_to_csv, prepare_csv
from constants import ROOT_PATH


def remove_duplicate_rows(filename: str | Path, fieldnames: List[str], output_filename: str | Path) -> None:
    """Removes duplicate rows by checking date and title fields"""
    with open(filename, "r", encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=',', quotechar='"')
        articles_info = []
        for row in reader:
            articles_info.append({fieldname: row[fieldname] for fieldname in fieldnames})

    articles_info = list({article["date"] + article["title"]: article for article in articles_info}.values())

    prepare_csv(output_filename, fieldnames)
    save_to_csv(articles_info, output_filename)


if __name__ == "__main__":
    for filename in Path(ROOT_PATH / 'parsers' / 'ria_dataset' / 'v2').iterdir():
        remove_duplicate_rows(filename, ["date", "title", "text", "source", "topics"], ROOT_PATH / 'parsers' / 'ria_dataset' / 'v2_clean' / filename.name)
