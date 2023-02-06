import json
import pickle
from os.path import dirname, join, realpath
from typing import Dict, Optional

import nltk
from nltk.corpus import stopwords


class LogTokenizer:
    """
    This class tokenizes a log file into a list of tokens    
    """
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
        """
        If the word is not in the dictionary, add it to the dictionary.
        
        :param word: the word to be added to the vocabulary
        """
        if word not in self.word2index:
            self.word2index[word] = self.n_words
            self.index2word[self.n_words] = word
            self.n_words += 1

    def _perform_actions(self, token):
        """
        If the token is a word, and it's not a stop word, then return the index of the word in the
        vocabulary
        
        :param token: the token to be processed
        :return: The word2index dictionary is being returned.
        """
        lower = token.lower()
        if token.isalpha() and lower not in self.stop_words:
            return self.word2index.get(lower, None)

    def tokenize(self, message):
        """
        1. Split the message into a list of words
        2. Filter out any words that are not in the vocabulary
        3. Perform any actions that are required on the word
        4. Add the [CLS] token to the beginning of the message
        
        :param message: The message to be tokenized
        :return: A list of integers, where each integer is the index of the word in the vocabulary.
        """
        message = list(filter(lambda x: x is not None, map(self._perform_actions, message.split(" "))))
        message = [self.word2index['[CLS]']] + message
        return message

    @staticmethod
        """
        It loads the tokenizer from a pickle file
        
        :param path: The path to the pickle file
        :type path: Optional[str]
        :return: A LogTokenizer object
        """
    def load_from_pickle(path: Optional[str] = None):
        if not path:
            path = join(dirname(realpath(__file__)), "..", "models", "tokenizer_dict.pickle")
        return LogTokenizer(index2word=pickle.load(open(path, 'rb')))

    @staticmethod
        """
        Loads a tokenizer from a json file
        
        :param path: The path to the tokenizer dictionary
        :type path: Optional[str]
        :return: A LogTokenizer object
        """
    def load_from_json(path: Optional[str] = None):
        if not path:
            path = join(dirname(realpath(__file__)), "..", "models", "tokenizer_dict.json")
        return LogTokenizer(index2word=json.load(open(path, 'rb')))

    def convert_tokens_to_ids(self, tokens):
        """
        It takes a list of tokens and returns a list of their corresponding ids
        
        :param tokens: a list of tokens
        :return: A list of the indices of the words in the tokens list.
        """
        return [self.word2index[w] for w in tokens]

    def convert_ids_to_tokens(self, ids):
        """
        It takes a list of integers (ids) and returns a list of words (tokens)
        
        :param ids: a list of integers, the token ids to be converted
        :return: A list of words
        """
        return [self.index2word[i] for i in ids]
