from datetime import datetime
from typing import Optional


def parse_date(date_string: str) -> Optional[datetime]:
    date = None

    for pattern in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            date = datetime.strptime(date_string, pattern)
        except:
            pass

    if not date:
        raise ValueError(f"String {date_string} could not be parsed.")

    return date


def convert_date(date_string: str, new_pattern: str) -> str:
    date = parse_date(date_string)
    return datetime.strftime(date, new_pattern)
