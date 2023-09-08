"""Script to create a .txt file from .csv dataset"""
import csv
from pathlib import Path
import sys


csv.field_size_limit(sys.maxsize)


def create_text_file_from_dataset(filename: str | Path, output_filename: str | Path) -> None:
    """Creates a .txt file from .csv dataset"""
    with open(filename, "r", encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=',', quotechar='"')
        with open(output_filename, "w", encoding='utf-8') as output_file:
            for row in reader:
                output_file.write(row["text"])


def read_text_file_from_dataset(filename: str | Path) -> str:
    """Reads a .txt file from .csv dataset"""
    text = []

    with open(filename, "r", encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=',', quotechar='"')
        for row in reader:
            text.append(row["text"])

    return ' '.join(text)
