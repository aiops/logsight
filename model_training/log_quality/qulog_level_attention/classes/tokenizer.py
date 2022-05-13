import re
import nltk
from nltk.corpus import stopwords


class LogTokenizer:
    def __init__(self, filters=r"([ |:|\(|\)|=|,])|(core.)|(\.{2,})"):
        self.filters = filters
        self.word2index = {'[PAD]': 0, '[CLS]': 1, '[MASK]': 2, '[UNK]': 3}
        self.index2word = {0: '[PAD]', 1: '[CLS]', 2: '[MASK]', 3:'[UNK]'}
        self.n_words = 4  # Count SOS and EOS
        self.stop_words = set(stopwords.words('english'))
        # self.regex_tokenizer = nltk.RegexpTokenizer(r'\w+|.|')
        # self.stop_words = {"she's", 'we', 'each', "you've", 'where', 'me',
        #                    'yourselves', 'at', 'only', 'am', 'being', 'll', 'doing', 'than', 'them',
        #                    'itself', 'myself', 'himself', 'i', 'when', 'does', 'ourselves',
        #                    "it's", 'just', 'should', 'to', "you'd", 'why', 'won', 'other', 'you', 'yourself', 'who', 'very', 'these', 'he', 'isn', 'an','its', 'yours', 'then',
        #                    'mightn', 'my', 'those', 'her', 'were', 'his', 'don', 'm', 'ours', 'theirs', "that'll",
        #                    'd', 'did', 'their', 'him', 'whom', 'what', 'both', 's', 'herself', 'because', 'our', 'same', "should've", 'o', 'there', "haven't", "you'll", 'here', 'was',
        #                    "mightn't", 'she', "won't", 'y', 'it', 'into', "you're", 'through', 're', 'ma', 'hers', 'as', 'a', 'own', 'such', "doesn't", 'themselves', 'they', 'how', 've', 't',
        #                    'but', 'of', 'your', 'having', 'so'}
        self.regex_tokenizer = nltk.RegexpTokenizer(' ', gaps=True)

    def add_word(self, word):
        if word not in self.word2index:
            self.word2index[word] = self.n_words
            self.index2word[self.n_words] = word
            self.n_words += 1

    def tokenize(self, sent):
        # sent = re.sub(r'/.*:', '', sent, flags=re.MULTILINE)
        # sent = re.sub(r'/.*', '', sent, flags=re.MULTILINE)
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
        #         sent = re.sub(r'/.*:', '', sent, flags=re.MULTILINE)
        #         sent = re.sub(r'/.*', '', sent, flags=re.MULTILINE)
        sent = self.regex_tokenizer.tokenize(sent)
        sent = [w.lower() for w in sent if w.isalpha() and w.lower() not in self.stop_words]
        sent = [word for word in sent if word.isalpha()]
        sent = [w for w in sent if w not in self.stop_words]
        sent = ['[CLS]'] + sent
        i = 0
        zero_pad = [0]*32
        for w in range(len(sent)):
            if sent[i] in self.word2index.keys():
                sent[i] = self.word2index[sent[i]]
                i += 1
            else:
                #               print("THIS IS NON EXIStANT TOKEN", sent[w])
                #               sent[w] = self.word2index['[UNK]']
                sent.pop(i)
        sent += zero_pad
        return sent

    def convert_tokens_to_ids(self, tokens):
        return [self.word2index[w] for w in tokens]

    def convert_ids_to_tokens(self, ids):
        return [self.index2word[i] for i in ids]

# tokenizer = LogTokenizer()
# print(tokenizer.tokenize_test("Message queue empty"))
