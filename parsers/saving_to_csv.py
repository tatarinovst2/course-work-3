"""Saving to csv file."""
import csv
from datetime import datetime
import os
from typing import Dict, List
from pathlib import Path


def get_filename(parser_name: str, start_date: datetime, end_date: datetime) -> str:
    """Create a filename to save data"""
    return f"{parser_name}_dataset/{parser_name}-{start_date.strftime('%Y-%m-%d')}-to-" \
           f"{end_date.strftime('%Y-%m-%d')}.csv"


def does_file_exist(filename: str):
    """Checks if the file exists"""
    return os.path.exists(filename)


def prepare_csv(filename: str | Path, fieldnames: list) -> None:
    """Prepare csv file."""
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    with open(filename, "w", encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()


def save_to_csv(articles_dict: List[Dict[str, str]], filename: str | Path, fieldnames: list = None,
                create_new_file: bool = False) -> None:
    """Save articles to csv file."""
    if not fieldnames:
        if create_new_file:
            fieldnames = articles_dict[0].keys()
        else:
            with open(filename, "r", encoding='utf-8') as file:
                reader = csv.DictReader(file)
                fieldnames = reader.fieldnames

    mode = "a" if not create_new_file else "w"

    with open(filename, mode, encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if create_new_file:
            writer.writeheader()
        for article_dict in articles_dict:
            writer.writerow(article_dict)
