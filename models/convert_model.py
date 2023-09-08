"""Script to convert Word2Vec model from binary to text format and vice versa"""
from pathlib import Path

from constants import ROOT_PATH
from utils.utils import load_model


def convert_model_to_text(model_filepath: str | Path, output_filepath: str | Path) -> None:
    """Convert Word2Vec model from binary to text format and vice versa"""
    model = load_model(model_filepath)
    model.save_word2vec_format(output_filepath, binary=False)


def convert_model_to_binary(model_filepath: str | Path, output_filepath: str | Path) -> None:
    """Convert Word2Vec model from binary to text format and vice versa"""
    model = load_model(model_filepath)
    model.save_word2vec_format(output_filepath, binary=True)


if __name__ == "__main__":
    for year in range(2000, 2023):
        print(f"Converting model for {year} year...")
        convert_model_to_binary(
            str(ROOT_PATH / "models" / f"merged_{year}_aligned.txt"),
            str(ROOT_PATH / "models" / f"merged_{year}_aligned.bin.gz"),
        )
        print(f"Model for {year} year converted and saved")
