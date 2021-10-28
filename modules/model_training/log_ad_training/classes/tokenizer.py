import re
import nltk
from nltk.corpus import stopwords


class LogTokenizer:
    def __init__(self, filters=r"([ |:|\(|\)|=|,])|(core.)|(\.{2,})"):
        self.filters = filters
        self.word2index = {'[PAD]': 0, '[CLS]': 1, '[MASK]': 2, '[UNK]': 3}
        self.index2word = {0: '[PAD]', 1: '[CLS]', 2: '[MASK]', 3: '[UNK]'}
        self.n_words = 4  # Count SOS and EOS
        self.stop_words = set(stopwords.words('english'))
        self.regex_tokenizer = nltk.RegexpTokenizer(r' ', gaps=True)

    def add_word(self, word):
        if word not in self.word2index:
            self.word2index[word] = self.n_words
            self.index2word[self.n_words] = word
            self.n_words += 1

    def tokenize(self, sent):
        sent = self.regex_tokenizer.tokenize(sent)
        sent = [w.lower() for w in sent if w.isalpha() and w.lower() not in self.stop_words]
        sent = [word for word in sent if word.isalpha()]
        sent = [w for w in sent if w not in self.stop_words]
        sent = ['[CLS]'] + sent
        for w in range(len(sent)):
            self.add_word(sent[w])
            sent[w] = self.word2index[sent[w]]

        return sent

    def tokenize_test(self, sent):
        sent = self.regex_tokenizer.tokenize(sent)
        sent = [w.lower() for w in sent if w.isalpha() and w.lower() not in self.stop_words]
        sent = [word for word in sent if word.isalpha()]
        sent = [w for w in sent if w not in self.stop_words]
        sent = ['[CLS]'] + sent
        i = 0
        for w in range(len(sent)):
            if sent[i] in self.word2index.keys():
                sent[i] = self.word2index[sent[i]]
                i += 1
            else:
                sent.pop(i)
        return sent


    def convert_tokens_to_ids(self, tokens):
        return [self.word2index[w] for w in tokens]

    def convert_ids_to_tokens(self, ids):
        return [self.index2word[i] for i in ids]