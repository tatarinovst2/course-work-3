import numpy as np
import csv

from constants import ROOT_PATH
from models.utils.utils import load_model


our_models = {}
with open(ROOT_PATH / 'config.tsv', 'r') as csvfile:
    reader = csv.DictReader(csvfile, delimiter='\t')
    for row in reader:
        our_models[row['identifier']] = {}
        our_models[row['identifier']]['path'] = row['path']
        our_models[row['identifier']]['default'] = row['default']
        our_models[row['identifier']]['tags'] = row['tags']
        our_models[row['identifier']]['corpus_size'] = int(row['size'])

models_dict = {}

def frequency(word, model):
    corpus_size = our_models[model]['corpus_size']
    if word not in models_dict[model].vocab:
        return 0, 'low'
    if not our_models[model]['vocabulary']:
        return 0, 'mid'
    wordfreq = models_dict[model].vocab[word].count
    relative = wordfreq / corpus_size
    tier = 'mid'
    if relative > 0.0001:
        tier = 'high'
    elif relative < 0.00005:
        tier = 'low'
    return wordfreq, tier


def find_shifts(query):
    print(query['model1'])
    model1 = models_dict[query['model1']]
    model2 = models_dict[query['model2']]
    pos = query.get("pos")
    n = query['n']
    results = {'frequencies': {}}
    shared_voc = list(set.intersection(set(model1.index2word), set(model2.index2word)))
    matrix1 = np.zeros((len(shared_voc), model1.vector_size))
    matrix2 = np.zeros((len(shared_voc), model2.vector_size))
    for nr, word in enumerate(shared_voc):
        matrix1[nr, :] = model1[word]
        matrix2[nr, :] = model2[word]
    sims = (matrix1 * matrix2).sum(axis=1)
    min_sims = np.argsort(sims)  # [:n]
    results['neighbors'] = list()
    results['frequencies'] = dict()
    freq_type_num = {"low": 0, "mid": 0, "high": 0}
    for nr in min_sims:
        if min(freq_type_num.values()) > n:
            break

        word = shared_voc[nr]
        sim = sims[nr]

        freq = frequency(word, query['model1'])
        if word.endswith(pos) or pos == "ALL":
            if freq_type_num[freq[1]] < n:
                results['neighbors'].append((word, sim))
                results['frequencies'][word] = freq
                freq_type_num[freq[1]] += 1

    return results



if __name__ == "__main__":
    request = {
        "model1": "2013",
        "model2": "2014",
        "pos": "ALL",
        "n": 30
    }

    for model in ["2013", "2014"]:
        print(model)
        models_dict[model] = load_model(str(ROOT_PATH) + our_models[model]['path'])
        our_models[model]['vocabulary'] = set(models_dict[model].index2word)

    print(find_shifts(request))
