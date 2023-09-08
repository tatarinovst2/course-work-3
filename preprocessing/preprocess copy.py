from __future__ import print_function
from __future__ import division
from multiprocessing import Process
import sys
import csv
from pathlib import Path

import os
from typing import List

import wget
import re
from ufal.udpipe import Model, Pipeline

from constants import ROOT_PATH

csv.field_size_limit(sys.maxsize)
'''
Этот скрипт принимает на вход необработанный русский текст 
(одно предложение на строку или один абзац на строку).
Он токенизируется, лемматизируется и размечается по частям речи с использованием UDPipe.
На выход подаётся последовательность разделенных пробелами лемм с частями речи 
("зеленый_NOUN трамвай_NOUN").
Их можно непосредственно использовать в моделях с RusVectōrēs (https://rusvectores.org).
'''


def num_replace(word):
    newtoken = 'x' * len(word)
    return newtoken


def clean_token(token, misc):
    """
    :param token:  токен (строка)
    :param misc:  содержимое поля "MISC" в CONLLU (строка)
    :return: очищенный токен (строка)
    """
    out_token = token.strip().replace(' ', '')
    if token == 'Файл' and 'SpaceAfter=No' in misc:
        return None
    return out_token


def clean_lemma(lemma, pos):
    """
    :param lemma: лемма (строка)
    :param pos: часть речи (строка)
    :return: очищенная лемма (строка)
    """
    out_lemma = lemma.strip().replace(' ', '').replace('_', '').lower()
    if '|' in out_lemma or out_lemma.endswith('.jpg') or out_lemma.endswith('.png'):
        return None
    if pos != 'PUNCT':
        if out_lemma.startswith('«') or out_lemma.startswith('»'):
            out_lemma = ''.join(out_lemma[1:])
        if out_lemma.endswith('«') or out_lemma.endswith('»'):
            out_lemma = ''.join(out_lemma[:-1])
        if out_lemma.endswith('!') or out_lemma.endswith('?') or out_lemma.endswith(',') \
                or out_lemma.endswith('.'):
            out_lemma = ''.join(out_lemma[:-1])
    return out_lemma


def list_replace(search, replacement, text):
    search = [el for el in search if el in text]
    for c in search:
        text = text.replace(c, replacement)
    return text


def unify_sym(text):  # принимает строку в юникоде
    text = list_replace \
        ('\u00AB\u00BB\u2039\u203A\u201E\u201A\u201C\u201F\u2018\u201B\u201D\u2019', '\u0022', text)

    text = list_replace \
        ('\u2012\u2013\u2014\u2015\u203E\u0305\u00AF', '\u2003\u002D\u002D\u2003', text)

    text = list_replace('\u2010\u2011', '\u002D', text)

    text = list_replace \
            (
            '\u2000\u2001\u2002\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u200B\u202F\u205F\u2060\u3000',
            '\u2002', text)

    text = re.sub('\u2003\u2003', '\u2003', text)
    text = re.sub('\t\t', '\t', text)

    text = list_replace \
            (
            '\u02CC\u0307\u0323\u2022\u2023\u2043\u204C\u204D\u2219\u25E6\u00B7\u00D7\u22C5\u2219\u2062',
            '.', text)

    text = list_replace('\u2217', '\u002A', text)

    text = list_replace('…', '...', text)

    text = list_replace('\u2241\u224B\u2E2F\u0483', '\u223D', text)

    text = list_replace('\u00C4', 'A', text)  # латинская
    text = list_replace('\u00E4', 'a', text)
    text = list_replace('\u00CB', 'E', text)
    text = list_replace('\u00EB', 'e', text)
    text = list_replace('\u1E26', 'H', text)
    text = list_replace('\u1E27', 'h', text)
    text = list_replace('\u00CF', 'I', text)
    text = list_replace('\u00EF', 'i', text)
    text = list_replace('\u00D6', 'O', text)
    text = list_replace('\u00F6', 'o', text)
    text = list_replace('\u00DC', 'U', text)
    text = list_replace('\u00FC', 'u', text)
    text = list_replace('\u0178', 'Y', text)
    text = list_replace('\u00FF', 'y', text)
    text = list_replace('\u00DF', 's', text)
    text = list_replace('\u1E9E', 'S', text)

    currencies = list \
            (
            '\u20BD\u0024\u00A3\u20A4\u20AC\u20AA\u2133\u20BE\u00A2\u058F\u0BF9\u20BC\u20A1\u20A0\u20B4\u20A7\u20B0\u20BF\u20A3\u060B\u0E3F\u20A9\u20B4\u20B2\u0192\u20AB\u00A5\u20AD\u20A1\u20BA\u20A6\u20B1\uFDFC\u17DB\u20B9\u20A8\u20B5\u09F3\u20B8\u20AE\u0192'
        )

    alphabet = list \
            (
            '\t\n\r абвгдеёзжийклмнопрстуфхцчшщьыъэюяАБВГДЕЁЗЖИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯ,.[]{}()=+-−*&^%$#@!~;:0123456789§/\|"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ')

    alphabet.append("'")

    allowed = set(currencies + alphabet)

    cleaned_text = [sym for sym in text if sym in allowed]
    cleaned_text = ''.join(cleaned_text)

    return cleaned_text


def process(pipeline, text='Строка', keep_pos=True, keep_punct=False):
    # Если частеречные тэги не нужны (например, их нет в модели), выставьте pos=False
    # в этом случае на выход будут поданы только леммы
    # По умолчанию знаки пунктуации вырезаются. Чтобы сохранить их, выставьте punct=True

    entities = {'PROPN'}
    named = False
    memory = []
    mem_case = None
    mem_number = None
    tagged_propn = []

    # обрабатываем текст, получаем результат в формате conllu:
    processed = pipeline.process(text)

    # пропускаем строки со служебной информацией:
    content = [l for l in processed.split('\n') if not l.startswith('#')]

    # извлекаем из обработанного текста леммы, тэги и морфологические характеристики
    tagged = [w.split('\t') for w in content if w]

    for t in tagged:
        if len(t) != 10:
            continue
        (word_id, token, lemma, pos, xpos, feats, head, deprel, deps, misc) = t
        token = clean_token(token, misc)
        lemma = clean_lemma(lemma, pos)
        if not lemma or not token:
            continue
        if pos in entities:
            if '|' not in feats:
                tagged_propn.append('%s_%s' % (lemma, pos))
                continue
            morph = {el.split('=')[0]: el.split('=')[1] for el in feats.split('|')}
            if 'Case' not in morph or 'Number' not in morph:
                tagged_propn.append('%s_%s' % (lemma, pos))
                continue
            if not named:
                named = True
                mem_case = morph['Case']
                mem_number = morph['Number']
            if morph['Case'] == mem_case and morph['Number'] == mem_number:
                memory.append(lemma)
                if 'SpacesAfter=\\n' in misc or 'SpacesAfter=\s\\n' in misc:
                    named = False
                    past_lemma = '::'.join(memory)
                    memory = []
                    tagged_propn.append(past_lemma + '_PROPN')
            else:
                named = False
                past_lemma = '::'.join(memory)
                memory = []
                tagged_propn.append(past_lemma + '_PROPN')
                tagged_propn.append('%s_%s' % (lemma, pos))
        else:
            if not named:
                if pos == 'NUM' and token.isdigit():  # Заменяем числа на xxxxx той же длины
                    lemma = num_replace(token)
                tagged_propn.append('%s_%s' % (lemma, pos))
            else:
                named = False
                past_lemma = '::'.join(memory)
                memory = []
                tagged_propn.append(past_lemma + '_PROPN')
                tagged_propn.append('%s_%s' % (lemma, pos))

    if not keep_punct:
        tagged_propn = [word for word in tagged_propn if word.split('_')[1] != 'PUNCT']
    if not keep_pos:
        tagged_propn = [word.split('_')[0] for word in tagged_propn]
    return tagged_propn
def preprocess_text_file_2(input_filename: str | Path, output_filename: str | Path, is_multiprocessed: bool = False) -> None:
    print(f"Preprocessing {input_filename}...")
    # URL of the UDPipe model
    udpipe_model_url = 'https://rusvectores.org/static/models/udpipe_syntagrus.model'
    udpipe_filename = udpipe_model_url.split('/')[-1]

    if not os.path.isfile(udpipe_filename):
        print('UDPipe model not found. Downloading...')
        wget.download(udpipe_model_url)

    print('Loading the model...')
    model = Model.load(udpipe_filename)
    process_pipeline = Pipeline(model, 'tokenize', Pipeline.DEFAULT, Pipeline.DEFAULT, 'conllu')

    print('Processing input...')
    with open(input_filename, 'r', encoding='utf-8') as input_file:
        preprocessed_lines = []
        lines = []

        if str(input_filename).endswith('.csv'):
            reader = csv.DictReader(input_file, delimiter=',', quotechar='"')
            for row in reader:
                lines.append(row['text'])
        else:
            lines = input_file.readlines()

        for line in lines:
            res = unify_sym(line.strip())
            output = process(process_pipeline, text=res)
            preprocessed_lines.append(' '.join(output))
            if is_multiprocessed:
                if len(preprocessed_lines) % 1000 == 0:
                    print(f'{input_filename} – lines processed: {len(preprocessed_lines)} / {len(lines)}')
            else:
                print(f'\rLines processed: {len(preprocessed_lines)} / {len(lines)}', end=''"")

    with open(output_filename, 'w', encoding='utf-8') as output_file:
        output_file.write('\n'.join(preprocessed_lines))

def preprocess_text_file(input_filename: str | Path, output_filename: str | Path, is_multiprocessed: bool = False, chunks: int = 6) -> None:
    print(f"Preprocessing {input_filename}...")
    # URL of the UDPipe model
    udpipe_model_url = 'https://rusvectores.org/static/models/udpipe_syntagrus.model'
    udpipe_filename = udpipe_model_url.split('/')[-1]

    if not os.path.isfile(udpipe_filename):
        print('UDPipe model not found. Downloading...')
        wget.download(udpipe_model_url)

    print('Loading the model...')
    model = Model.load(udpipe_filename)
    process_pipeline = Pipeline(model, 'tokenize', Pipeline.DEFAULT, Pipeline.DEFAULT, 'conllu')

    print('Processing input...')
    with open(input_filename, 'r', encoding='utf-8') as input_file:
        lines = []

        if str(input_filename).endswith('.csv'):
            reader = csv.DictReader(input_file, delimiter=',', quotechar='"')
            for row in reader:
                lines.append(row['text'])
        else:
            lines = input_file.readlines()

        chunked_lines = [lines[i::chunks] for i in range(chunks)]
        preprocessed_lines = []

        if is_multiprocessed:
            processes = []
            for chunk in chunked_lines:
                p = Process(target=preprocess_text_lines, args=(chunk, process_pipeline, True))
                processes.append(p)

            for p in processes:
                p.start()

            for p in processes:
                p.join()
        else:
            for chunk in chunked_lines:
                preprocessed_lines += preprocess_text_lines(chunk, process_pipeline=process_pipeline, is_multiprocessed=False)

    with open(output_filename, 'w', encoding='utf-8') as output_file:
        output_file.write('\n'.join(preprocessed_lines))


def preprocess_text_lines(text_lines: List[str], process_pipeline: Model, is_multiprocessed: bool = False) -> List[str]:
    preprocessed_lines = []

    for line in text_lines:
        res = unify_sym(line.strip())
        output = process(process_pipeline, text=res)
        preprocessed_lines.append(' '.join(output))
        if is_multiprocessed:
            if len(preprocessed_lines) % 1000 == 0:
                print(f'Lines processed: {len(preprocessed_lines)} / {len(text_lines)}')
        else:
            print(f'Lines processed: {len(preprocessed_lines)} / {len(text_lines)}', end=''"")

    return preprocessed_lines



if __name__ == "__main__":
    preprocess_text_file(ROOT_PATH / 'datasets' / 'merged_dataset' / 'merged_2000.csv',
                         ROOT_PATH / 'datasets' / 'merged_dataset' / 'merged_2000_preprocessed.csv',
                         is_multiprocessed=True)
