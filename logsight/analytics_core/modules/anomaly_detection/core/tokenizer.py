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
        self.regex_tokenizer = nltk.RegexpTokenizer(r' ', gaps=True)

    def add_word(self, word):
        if word not in self.word2index:
            self.word2index[word] = self.n_words
            self.index2word[self.n_words] = word
            self.n_words += 1

    def tokenize(self, message):
        message = self._preprocess(message)
        for w in range(len(message)):
            self.add_word(message[w])
            message[w] = self.word2index[message[w]]

        return message

    def _preprocess(self, message):
        message = self.regex_tokenizer.tokenize(message)
        message = [w.lower() for w in message if w.isalpha() and w.lower() not in self.stop_words]
        message = [word for word in message if word.isalpha()]
        message = [w for w in message if w not in self.stop_words]
        message = ['[CLS]'] + message
        return message

    def tokenize_test(self, message):
        message = self._preprocess(message)
        i = 0
        for _ in range(len(message)):
            if message[i] in self.word2index.keys():
                message[i] = self.word2index[message[i]]
                i += 1
            else:
                message.pop(i)
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
