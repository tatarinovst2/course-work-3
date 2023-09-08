from utils.utils import load_model
from constants import ROOT_PATH


def get_models_paths() -> list[str]:
    """Returns a list of paths to all model files from models directory"""
    paths = [str(model_path) for model_path in ROOT_PATH.glob('models/*.gz')]
    return sorted(paths)


def load_models() -> dict:
    """Loads all models into a dictionary"""
    models_dict = {}
    for model in get_models_paths():
        models_dict[model] = load_model(model)
    return models_dict


def get_vocab_pos_count(model1, model2) -> dict:
    """Returns a dictionary of parts of speech and their counts in the intersection of the two models' vocabularies"""
    vocab_pos = {}
    for word in set.intersection(set(model1.index2word), set(model2.index2word)):
        pos = word.split('_')[1] if '_' in word else 'NONE'
        vocab_pos[pos] = vocab_pos.get(pos, 0) + 1
    return vocab_pos


def get_vocab_count(model1, model2) -> int:
    """Returns the number of words in the intersection of the two models' vocabularies"""
    return len(set.intersection(set(model1.index2word), set(model2.index2word)))


def get_vocab_pos_count_for_all_models(ignore_duplicate_words: bool = False) -> dict:
    """Returns a dictionary of parts of speech and their counts in the yearly intersection of all models' vocabularies"""
    all_words = []
    models = load_models()
    for index in range(len(models) - 1):
        model1 = models[get_models_paths()[index]]
        model2 = models[get_models_paths()[index + 1]]
        for word in set.intersection(set(model1.index2word), set(model2.index2word)):
            all_words.append(word)
    if ignore_duplicate_words:
        all_words = list(set(all_words))
    vocab_pos = {}
    for word in all_words:
        pos = word.split('_')[1] if '_' in word else 'NONE'
        vocab_pos[pos] = vocab_pos.get(pos, 0) + 1
    return vocab_pos


def get_vocabularies() -> dict:
    """Returns a dictionary of model names and their vocabularies"""
    vocabularies = {}
    models = load_models()
    for index in range(len(models) - 1):
        model1 = models[get_models_paths()[index]]
        model2 = models[get_models_paths()[index + 1]]
        vocabularies[get_models_paths()[index]] = get_vocab_pos_count(model1, model2)
    return vocabularies


if __name__ == '__main__':
    #print(get_vocabularies())
    print(get_vocab_pos_count_for_all_models(ignore_duplicate_words=True))
    print(get_vocab_pos_count_for_all_models(ignore_duplicate_words=False))
