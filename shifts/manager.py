"""Script that is used for working with the database."""

import csv
from pathlib import Path
import json

import seaborn as sns
import matplotlib.pyplot as plt

import json
from constants import ROOT_PATH


def load_shifts() -> dict:
    """Load shifts from the database."""
    shift_years = {}

    for year in range(2000, 2022):
        with open(ROOT_PATH / 'shifts' / f'shifts_{year}_{year + 1}.json', 'r',
                  encoding='utf-8') as file:
            shift_years[f"{year}_{year + 1}"] = json.load(file)
    return shift_years


SHIFTS = load_shifts()
YEARS = list(range(2000, 2022))
VOCABS = {
    '2000_2001': 11081,
    '2001_2002': 15569,
    '2002_2003': 15660,
    '2003_2004': 17170,
    '2004_2005': 31508,
    '2005_2006': 36229,
    '2006_2007': 40989,
    '2007_2008': 44133,
    '2008_2009': 49189,
    '2009_2010': 50775,
    '2010_2011': 51629,
    '2011_2012': 53609,
    '2012_2013': 52528,
    '2013_2014': 47604,
    '2014_2015': 47028,
    '2015_2016': 49423,
    '2016_2017': 51035,
    '2017_2018': 49527,
    '2018_2019': 48568,
    '2019_2020': 47877,
    '2020_2021': 50105,
    '2021_2022': 50874,
    'overall': 102475,
    'overall_with_duplicates': 875275
}

POS_VOCABS = {
    '2000_2001': {'NOUN': 4229, 'PROPN': 2062, 'ADJ': 2139, 'VERB': 1866, 'ADV': 435},
    '2001_2002': {'NOUN': 5457, 'PROPN': 3599, 'ADJ': 2941, 'VERB': 2552, 'ADV': 543},
    '2002_2003': {'NOUN': 5451, 'PROPN': 3591, 'ADJ': 3005, 'VERB': 2562, 'ADV': 566},
    '2003_2004': {'NOUN': 5872, 'PROPN': 4071, 'ADJ': 3305, 'VERB': 2783, 'ADV': 622},
    '2004_2005': {'NOUN': 9841, 'PROPN': 8704, 'ADJ': 6065, 'VERB': 4731, 'ADV': 1062},
    '2005_2006': {'NOUN': 11186, 'PROPN': 10164, 'ADJ': 6983, 'VERB': 5472, 'ADV': 1227},
    '2006_2007': {'NOUN': 12476, 'PROPN': 11843, 'ADJ': 7706, 'VERB': 6121, 'ADV': 1373},
    '2007_2008': {'NOUN': 13097, 'PROPN': 13497, 'ADJ': 8065, 'VERB': 6309, 'ADV': 1425},
    '2008_2009': {'NOUN': 14113, 'PROPN': 16262, 'ADJ': 8575, 'VERB': 6515, 'ADV': 1501},
    '2009_2010': {'NOUN': 14513, 'PROPN': 16953, 'ADJ': 8791, 'VERB': 6747, 'ADV': 1543},
    '2010_2011': {'NOUN': 14822, 'PROPN': 17251, 'ADJ': 8852, 'VERB': 6889, 'ADV': 1574},
    '2011_2012': {'NOUN': 15204, 'PROPN': 18476, 'ADJ': 8921, 'VERB': 7033, 'ADV': 1634},
    '2012_2013': {'NOUN': 14988, 'PROPN': 18034, 'ADJ': 8779, 'VERB': 6942, 'ADV': 1596},
    '2013_2014': {'NOUN': 13683, 'PROPN': 15906, 'ADJ': 8040, 'VERB': 6513, 'ADV': 1511},
    '2014_2015': {'NOUN': 13428, 'PROPN': 15828, 'ADJ': 7878, 'VERB': 6443, 'ADV': 1514},
    '2015_2016': {'NOUN': 13994, 'PROPN': 16721, 'ADJ': 8238, 'VERB': 6668, 'ADV': 1551},
    '2016_2017': {'NOUN': 14562, 'PROPN': 17107, 'ADJ': 8572, 'VERB': 6965, 'ADV': 1605},
    '2017_2018': {'NOUN': 14337, 'PROPN': 16165, 'ADJ': 8325, 'VERB': 6995, 'ADV': 1608},
    '2018_2019': {'NOUN': 14357, 'PROPN': 15479, 'ADJ': 8270, 'VERB': 7008, 'ADV': 1588},
    '2019_2020': {'NOUN': 14256, 'PROPN': 14980, 'ADJ': 8189, 'VERB': 6957, 'ADV': 1544},
    '2020_2021': {'NOUN': 15172, 'PROPN': 15391, 'ADJ': 8582, 'VERB': 7309, 'ADV': 1620},
    '2021_2022': {'NOUN': 15637, 'PROPN': 15032, 'ADJ': 8974, 'VERB': 7548, 'ADV': 1719},
    'overall': {'NOUN': 25075, 'ADJ': 14674, 'PROPN': 49676, 'ADV': 2572, 'VERB': 10478},
    'overall_with_duplicates': {'ADJ': 159195, 'VERB': 128928, 'NOUN': 270675, 'PROPN': 287116, 'ADV': 29361}
}


def find_shifts(word: str) -> dict:
    """Find shift for the word."""
    shifts = {}
    for year_shifts in SHIFTS:
        for shift in SHIFTS[year_shifts]:
            if '_' not in word and '_' in shift:
                if word.split('_')[0].lower() == shift.split('_')[0].lower():
                    shifts[year_shifts] = {shift: SHIFTS[year_shifts][shift]}
            else:
                if word.lower() == shift.lower():
                    shifts[year_shifts] = {shift: SHIFTS[year_shifts][shift]}
    return shifts


def get_pos_statistics(relative: bool = False, use_overall_column: bool = False) -> dict:
    """Find statistics for the parts of speech."""
    pos_stats = {}
    for year_shifts in SHIFTS:
        pos_year_stat = {}
        for shift in SHIFTS[year_shifts]:
            pos = shift.split('_')[1] if '_' in shift else 'NONE'
            pos_year_stat[pos] = pos_year_stat.get(pos, 0) + 1
        if use_overall_column:
            pos_year_stat['overall'] = sum(pos_year_stat.values())
        pos_stats[year_shifts] = pos_year_stat
    if relative:
        for year_shifts in SHIFTS:
            for pos in pos_stats[year_shifts]:
                if pos != 'overall':
                    pos_stats[year_shifts][pos] /= POS_VOCABS[year_shifts][pos]
                else:
                    pos_stats[year_shifts][pos] /= VOCABS[year_shifts]
    return pos_stats


def get_sum_pos_statistics(relative: bool = False, use_overall_column: bool = False) -> dict:
    """Find statistics for the parts of speech for all years together."""
    pos_stats = {}
    for year_shifts in SHIFTS:
        for shift in SHIFTS[year_shifts]:
            pos = shift.split('_')[1] if '_' in shift else 'NONE'
            pos_stats[pos] = pos_stats.get(pos, 0) + 1
    if use_overall_column:
        pos_stats['overall'] = sum(pos_stats.values())
    if relative:
        for pos in pos_stats:
            if pos != 'overall':
                pos_stats[pos] /= POS_VOCABS['overall_with_duplicates'][pos]
            else:
                pos_stats[pos] /= VOCABS['overall_with_duplicates']
    return pos_stats


def get_shift_counts_by_year(relative: bool = False) -> dict:
    """Get shift counts by year."""
    shift_counts = {}
    for year, shifts in SHIFTS.items():
        shift_counts[year] = len(shifts)
    if relative:
        for year in shift_counts:
            shift_counts[year] /= VOCABS[year]
    return shift_counts


def get_shifts_for_pos(years: str, pos: str) -> dict:
    """Get shifts for the part of speech."""
    shifts = {}
    for year_shifts in SHIFTS:
        if year_shifts == years:
            for shift in SHIFTS[year_shifts]:
                if shift.split('_')[1] == pos:
                    shifts[shift] = SHIFTS[year_shifts][shift]
    return shifts


def visualize(data: dict, title: str, x_label: str, y_label: str, output_file: str = "") -> None:
    """Visualiza data with seaborn."""
    plt.figure(figsize=(10, 5))
    sns.set_theme(style="whitegrid")
    sns.set_palette(sns.color_palette("Greys_r", len(data)))
    ax = sns.barplot(x=list(data.keys()), y=list(data.values()), order=sorted(data.keys()))
    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    if output_file:
        output_file = ROOT_PATH / output_file
        output_file.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_file)
    else:
        plt.show()
    plt.clf()


def print_pretty_dict(dictionary: dict, limit: int = -1) -> None:
    """Print dictionary in pretty format."""
    if limit != -1:
        dictionary = dict(list(dictionary.items())[:limit])
    print(json.dumps(dictionary, indent=4, ensure_ascii=False))


def save_shift_words_to_csv(json_filepath: str | Path, output_filepath: str | Path) -> None:
    """Save shift words to csv."""
    with open(json_filepath, 'r', encoding='utf-8') as json_file:
        shifts = json.load(json_file)
    with open(output_filepath, 'w', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        for word in shifts:
            writer.writerow([word])


def find_most_shifting_words(top_n_in_shifts: int, ignore_model_years: bool = False) -> dict:
    """Find most shifting words."""
    words = {}
    for year_shifts in SHIFTS:
        for shift in list(SHIFTS[year_shifts].keys())[:top_n_in_shifts]:
            shifting_word = words.get(shift, {})
            shifting_word['count'] = shifting_word.get('count', 0) + 1
            if not ignore_model_years:
                shifting_word['model_years'] = shifting_word.get('model_years', []) + [year_shifts]
            words[shift] = shifting_word

    for word in list(words.keys()):
        if words[word]['count'] < 2:
            del words[word]

    words = {k: v for k, v in sorted(words.items(), key=lambda item: item[1]['count'], reverse=True)}

    return words


if __name__ == '__main__':
    sum_pos_stats = get_sum_pos_statistics(relative=False, use_overall_column=False)
    print_pretty_dict(sum_pos_stats)
    visualize(sum_pos_stats, 'Parts of speech', 'Part of speech', 'Count',
              output_file="results/pos_stat_overall.png")
    #print(find_most_shifting_words(100, True))

    #save_shift_words_to_csv(ROOT_PATH / 'shifts' / 'shifts_2004_2022.json',
    #                        ROOT_PATH / 'shifts' / 'shifts_2004_2022.csv')
    raise Exception
    # Prints shifts for the word
    print(find_shifts("карма"))
    #print_pretty_dict(get_shifts_for_pos('2012_2013', 'NOUN'), limit=50)

    visualize(get_shift_counts_by_year(relative=True), 'Shifts by year', 'Year', 'Count',
              output_file="results/shifts_by_year.png")
    print(get_shift_counts_by_year(relative=True))
    print(get_shift_counts_by_year(relative=False))

    pos_statistics = get_pos_statistics(relative=True, use_overall_column=True)
    print(pos_statistics)

    for year, pos_stat in pos_statistics.items():
        print(f"POS stat for years {year}: {pos_stat}")
        visualize(pos_stat, f'POS statistics for {year}', 'POS', 'Count',
                  output_file=f"results/pos_stat_relative_{year}.png")
