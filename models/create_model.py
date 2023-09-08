"""Script to create Word2Vec model using gensim"""
from pathlib import Path

from gensim.models import Word2Vec

from constants import ROOT_PATH


def create_model(text_filepath: str | Path, config: dict = None) -> Word2Vec:
    """Create Word2Vec model using gensim"""
    with open(text_filepath, "r", encoding="utf-8") as file:
        text = file.read()

    if not config:
        config = {
            "min_count": 10,
            "window": 5,
            "sg": 0,
            "iter": 10
        }

    sentences = [sentence.split() for sentence in text.splitlines()]

    return Word2Vec(sentences, min_count=config["min_count"], size=300, workers=12, window=config["window"],
                    sg=config["sg"], iter=config["iter"])


if __name__ == "__main__":
    #model = create_model(str(ROOT_PATH / "datasets" / "test_2013_preprocessed.txt"))
    #model.wv.save_word2vec_format(str(ROOT_PATH / "models" / "test_2013.txt.gz"), binary=False)

    #model = create_model(str(ROOT_PATH / "datasets" / "test_2014_preprocessed.txt"))
    #model.wv.save_word2vec_format(str(ROOT_PATH / "models" / "test_2014.txt.gz"), binary=False)

    #raise Exception

    configs = [
        {
            "min_count": 10,
            "window": 5,
            "sg": 0,
            "iter": 10
        }
    ]

    for config in configs:
        print(f"Using config: min_count: {config['min_count']}, window: {config['window']},"
              f"sg: {config['sg']}, iter: {config['iter']}")
        for year in range(2000, 2023):
            print(f"Creating model for {year} year...")
            model = create_model(str(ROOT_PATH / "datasets" / "merged_dataset" / f"merged_{year}_preprocessed.txt"),
                                 config=config)
            model.wv.save_word2vec_format(str(ROOT_PATH / "models" / f"merged_cbow_-_{year}.txt.gz"),
                                          binary=False)
