import gc
import getopt
import os
import sys

import numpy as np

PREDICTION_THRESHOLD = os.environ.get('PREDICTION_THRESHOLD', 0.85)


def get_settings(argv):
    try:
        opts, args = getopt.getopt(argv, "hi:o:", ["elasticsearch=", "kafka=", "private-key=", "application-name="])
    except getopt.GetoptError:
        print('kafka_consumer.py -es <elasticsearch> -k <kafka> -pk <private-key> -an <application-name>')
        sys.exit(2)
    private_key = None
    application_name = None
    elasticsearch_url = None
    kafka_url = None
    for opt, arg in opts:
        if opt == '-h':
            print('kafka_consumer.py -es <elasticsearch> -k <kafka> -pk <private-key> -an <application-name>')
            sys.exit()
        elif opt in ("-pk", "--private-key"):
            private_key = str(arg)
        elif opt in ("-an", "--application-name"):
            application_name = str(arg)
        elif opt in ("-es", "--elasticsearch"):
            elasticsearch_url = arg
        elif opt in ("-k", "--kafka"):
            kafka_url = arg
    return private_key, application_name, elasticsearch_url, kafka_url


def softmax(x):
    e_x = np.exp(x - np.max(x, axis=1, keepdims=True))  # subtracts each row with its max value
    sum = np.sum(e_x, axis=1, keepdims=True)  # returns sum of each row and keeps same dims
    f_x = e_x / sum
    gc.collect()
    return f_x


def get_padded_data(arr, max_pad, pad_value=0):
    return np.array([np.pad(i, (0, max_pad - len(i)), mode='constant', constant_values=pad_value) for i in arr])


def get_word_significance(tokenized_words, attention_scores):
    scores = {}
    i = 0
    for word in tokenized_words[1:]:
        scores[word] = attention_scores[i].item()
        i += 1
    return scores
