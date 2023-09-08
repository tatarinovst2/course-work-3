"""Script that merges datasets from different sources into one dataset"""
import csv
from datetime import datetime
import sys

from dataset_utils.parse_date import parse_date, convert_date
from parsers.saving_to_csv import save_to_csv, prepare_csv
from constants import ROOT_PATH


csv.field_size_limit(sys.maxsize)


def merge_datasets(dataset_filenames: list, start_date: str, end_date: str, fieldnames: list,
                   output_filename: str = "", skip_not_existing_datasets: bool = False) -> None:
    """Merges datasets from different sources into one dataset"""
    if output_filename:
        prepare_csv(output_filename, fieldnames)
    else:
        output_filename = "merged_dataset.csv"
        prepare_csv(output_filename, fieldnames)

    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    if end_date > datetime.now():
        print("End date is in the future.")

    if end_date < start_date:
        print("End date is earlier than start date.")

    articles_info = []

    for dataset_filename in dataset_filenames:
        try:
            with open(dataset_filename, "r", encoding='utf-8') as file:
                reader = csv.DictReader(file)

                for row in reader:
                    row_dict = {fieldname: row[fieldname] for fieldname in fieldnames}
                    parsed_date = parse_date(row_dict['date'])

                    if '/' in row_dict['date']:
                        row_dict['date'] = convert_date(row_dict['date'], "%Y-%m-%d")

                    if parsed_date < start_date:
                        continue

                    if parsed_date > end_date:
                        continue

                    articles_info.append(row_dict)

        except (FileNotFoundError, FileExistsError):
            if skip_not_existing_datasets:
                print(f"Skipping {dataset_filename} because could not find it.")
                pass
            else:
                raise FileNotFoundError(f"File with path {dataset_filename} does not exist. "
                                        f" skip_not_existing_datasets if you want to skip it.")

    articles_info.sort(key=lambda x: parse_date(x['date']))
    save_to_csv(articles_info, output_filename)


if __name__ == "__main__":
    for year in range(2000, 2023):
        merge_datasets([ROOT_PATH / "parsers" / "kommersant_dataset" / f"kommersant_{year}.csv",
                        ROOT_PATH / "parsers" / "lenta_dataset" / f"lenta_{year}.csv",
                        ROOT_PATH / "parsers" / "ria_dataset" / f"ria_{year}.csv"],
                       f"{year}-01-01", f"{year}-12-31", ["date", "title", "text"],
                       ROOT_PATH / "parsers" / "merged_dataset" / f"merged_{year}.csv", skip_not_existing_datasets=True)
