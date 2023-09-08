from pathlib import Path

from procrustes import smart_procrustes_align_gensim
from utils.utils import load_model

from constants import ROOT_PATH


def align_and_save_model(base_model_filepath: str | Path, model_to_align_filepath: str | Path,
                         save_as_binary: bool = True) -> None:
    models = []

    for model_filepath in [base_model_filepath, model_to_align_filepath]:
        model = load_model(str(model_filepath))
        model.init_sims(replace=True)
        models.append(model)

    print("Aligning models...")
    models[1] = smart_procrustes_align_gensim(models[0], models[1])
    print("Alignment complete")

    name = str(model_to_align_filepath).split(".")[0]
    new_name = name + "_aligned"
    if save_as_binary:
        models[1].save_word2vec_format(new_name + ".bin.gz", binary=True)
    else:
        models[1].save_word2vec_format(new_name + ".txt.gz", binary=False)


if __name__ == "__main__":
    for year in range(2000, 2023):
        print(f"Aligning model for {year} year...")
        align_and_save_model(
            str(ROOT_PATH / "models" / "merged_cbow_2017.txt.gz"),
            str(ROOT_PATH / "models" / f"merged_cbow_{year}.txt.gz"),
            save_as_binary=False
        )
        print(f"Model for {year} year aligned and saved")
