import json
import pickle
from os.path import dirname, join, realpath
from typing import Dict, Optional

import nltk
from nltk.corpus import stopwords


class LogTokenizer:
    def __init__(self, filters=r"([ |:|\(|\)|=|,])|(core.)|(\.{2,})", index2word: Optional[Dict] = None):
        self.filters = filters
        self.index2word = index2word or {0: '[PAD]', 1: '[CLS]', 2: '[MASK]', 3: '[UNK]'}
        self.word2index = {v: k for k, v in self.index2word.items()}
        self.n_words = len(self.index2word)
        try:
            self.stop_words = set(stopwords.words('english'))
        except LookupError:
            nltk.download('stopwords')
            self.stop_words = set(stopwords.words('english'))

    def add_word(self, word):
        if word not in self.word2index:
            self.word2index[word] = self.n_words
            self.index2word[self.n_words] = word
            self.n_words += 1

    def _perform_actions(self, token):
        lower = token.lower()
        if token.isalpha() and lower not in self.stop_words:
            return self.word2index.get(lower, None)

    def tokenize(self, message):
        message = list(filter(lambda x: x is not None, map(self._perform_actions, message.split(" "))))
        message = [self.word2index['[CLS]']] + message
        return message

    @staticmethod
    def load_from_pickle(path: Optional[str] = None):
        if not path:
            path = join(dirname(realpath(__file__)), "..", "models", "tokenizer_dict.pickle")
        return LogTokenizer(index2word=pickle.load(open(path, 'rb')))

    @staticmethod
    def load_from_json(path: Optional[str] = None):
        if not path:
            path = join(dirname(realpath(__file__)), "..", "models", "tokenizer_dict.json")
        return LogTokenizer(index2word=json.load(open(path, 'rb')))

    def convert_tokens_to_ids(self, tokens):
        return [self.word2index[w] for w in tokens]

    def convert_ids_to_tokens(self, ids):
        return [self.index2word[i] for i in ids]
