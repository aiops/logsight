import json
import re

import torch
import pandas as pd
import numpy as np
import pickle
import sys
import time
import os

from sklearn.model_selection import train_test_split

sys.path.append("log_ad_training/classes")
print("Path:", os.getcwd())

from log_ad_training.classes.networks import *
from log_ad_training.classes.data_loader import *

PAD_LEN = 50
BATCH_SIZE = 512
ANOMALY_DATA_SIZE = 170000
LEARNING_RATE = 0.0001
DECAY = 0.001
BETAS = (0.9, 0.999)
N_EPOCHS = 3

def load_model():
    model = torch.load('/models/model_github.pth', map_location=torch.device('cpu'))
    return model


def load_tokenizer():
    with open("/models/github_tokenizer.pickle", 'rb') as file:
        tokenizer = pickle.load(file, )
    return tokenizer


def tokenize(log, tokenizer):
    regex = re.compile('[^a-zA-Z ]')
    try:
        x = ' '.join(regex.sub('', log).strip().split())
        x = tokenizer.tokenize_test(x)
    except Exception as e:
        print(e)
        pass
    tokenized = torch.tensor(x)
    return tokenized


def get_padded_data(tokenized):
    padded = torch.nn.utils.rnn.pad_sequence(tokenized, batch_first=True)
    return padded


def load_anomaly_data_from_github(tokenizer, len_data):
    anomaly_df = pd.read_csv('/code/log_ad_training/anomalies/anomalies.csv').sample(len_data).log_message.values
    anomaly_data = []
    for i in anomaly_df:
        tokenized = torch.tensor(tokenize(str(i), tokenizer))
        anomaly_data.append(tokenized[:PAD_LEN])
    return anomaly_data


def parse_data(es_data):
    data = []
    tokenizer = load_tokenizer()
    i = 0
    for item in es_data:
        try:
            message = item['_source']['template']
        except Exception as e:
            print("Cannot read the row from elasticsearch...")
        tokenized = tokenize(str(message), tokenizer)
        data.append(tokenized[:PAD_LEN])
        i += 1
        if i > ANOMALY_DATA_SIZE:
            break

    anomalies = load_anomaly_data_from_github(tokenizer, len(data))
    data_new = data + anomalies

    padded = get_padded_data(data_new)

    labels = np.append(np.ones(len(data)).flatten(), np.zeros(len(anomalies)).flatten())
    x_train, x_test, y_train, y_test = train_test_split(padded, labels, test_size=0.2, random_state=42)
    train_dataloader = create_data_loader(x_train, y_train, BATCH_SIZE)
    test_dataloader = create_test_loader(x_test, y_test, BATCH_SIZE)

    return train_dataloader, test_dataloader


def load_weights():
    with open("/models/github_weights.pickle", "rb") as file:
        class_weights = pickle.load(file)
    return class_weights


def run_train(dataloader, model, optimizer, f_loss, epoch):
    model.train()
    total_loss = 0
    start = time.time()
    for i, batch in enumerate(dataloader):
        load, y = batch

        out = model.forward(load.long(), None)

        if isinstance(f_loss, torch.nn.CosineSimilarity):
            loss = (1 - f_loss(out, y)).pow(2).sum()
        else:
            loss = f_loss(out, y.long())

        loss.backward()
        optimizer.step()
        optimizer.zero_grad()

        total_loss += loss

    print("Epoch %d Train Loss: %f" %
          (epoch, total_loss / len(dataloader)), end='\r')
    return total_loss / len(dataloader), model


def run_optimizer(model, train_dataloader, optimizer, n_epochs, f_loss):
    for epoch in range(1, 1 + n_epochs):
        print("Epoch:", epoch)
        train_loss, model = run_train(train_dataloader, model, optimizer, f_loss, epoch)

    return train_loss, model


def run_validation(model, dataloader, f_loss):
    model.eval()
    total_loss = 0
    for i, batch in enumerate(dataloader):
        load, y = batch
        out = model.forward(load.long(), None)
        if isinstance(f_loss, torch.nn.CosineSimilarity):
            loss = (1 - f_loss(out, y)).pow(2).sum()
        else:
            loss = f_loss(out, y.long())
        total_loss += loss
    print("Train Loss: %f" % (total_loss / len(dataloader)), end='\r')
    return total_loss / len(dataloader)


def train_model(train_dataloader, val_dataloader, last_best_loss):
    save_flag = True
    val_loss = 0
    model = load_model()
    # cross_entropy_loss = torch.nn.CrossEntropyLoss(weight=torch.tensor([1., 1.], dtype=torch.float).cpu())
    # adam_opt = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE, betas=BETAS, weight_decay=DECAY)
    # optimizers = {"adam": adam_opt}
    # optimizer = optimizers['adam']
    # train_loss, model = run_optimizer(model, train_dataloader, optimizer, N_EPOCHS, cross_entropy_loss)
    # val_loss = run_validation(model, val_dataloader, cross_entropy_loss)
    #
    # if last_best_loss is not None:
    #     if val_loss < last_best_loss:
    #         save_flag = True
    # else:
    #     save_flag = True
    return model, save_flag, val_loss
