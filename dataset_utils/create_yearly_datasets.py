"""Script to create datasets for each year from datasets with various dates"""
import csv
from datetime import datetime
from typing import List
from pathlib import Path
import sys

from parsers.saving_to_csv import save_to_csv, prepare_csv

from constants import ROOT_PATH


csv.field_size_limit(sys.maxsize)


def validate_dataset_name(dataset_name: str) -> bool:
    """Checks if dataset name is valid"""
    if not dataset_name.endswith(".csv"):
        return False

    try:
        datetime.strptime(dataset_name.split("_")[1], "%Y-%m-%d")
        datetime.strptime(dataset_name.split("_")[3].replace(".csv", ""), "%Y-%m-%d")
    except ValueError:
        print(f"Invalid date in dataset name: {dataset_name}")
        return False

    return True


def get_dataset_names_for_year(directory_path: str | Path, year: int, is_sorted: bool = True) -> List[str]:
    """Returns list of dataset names for given year"""
    dataset_names = []
    for filename in Path(directory_path).iterdir():
        if not filename.name.endswith(".csv"):
            continue
        if not str(year) in filename.name:
            continue
        if not validate_dataset_name(filename.name):
            continue

        if datetime.strptime(str(filename.name).split("_")[1], "%Y-%m-%d").year == year or \
                datetime.strptime(str(filename.name).split("_")[3].replace(".csv", ""), "%Y-%m-%d").year == year:
            dataset_names.append(filename.name)

    if is_sorted:
        dataset_names.sort(key=lambda x: datetime.strptime(x.split("_")[1], "%Y-%m-%d"))

    return dataset_names


def create_yearly_dataset(directory_path: str | Path, year: int, fieldnames: List[str],
                          output_directory_path: str | Path) -> None:
    """Creates dataset for given year from datasets with various dates"""
    dataset_names = get_dataset_names_for_year(directory_path, year)
    output_filename = Path(output_directory_path) / f"ria_dataset_{year}.csv"
    prepare_csv(output_filename, fieldnames)

    for dataset_name in dataset_names:
        with open(Path(directory_path) / dataset_name, "r", encoding='utf-8') as file:
            reader = csv.DictReader(file)
            articles = [article for article in reader if article['date'] and
                        datetime.strptime(article['date'], "%Y-%m-%d").year == year]
            save_to_csv(articles, output_filename, fieldnames, create_new_file=False)


if __name__ == "__main__":
    for year in range(2004, 2023):
        create_yearly_dataset(ROOT_PATH / "parsers" / "ria_dataset", year, ["date", "title", "text", "source", "topics"],
                              ROOT_PATH / "parsers" / "ria_yearly_dataset")
