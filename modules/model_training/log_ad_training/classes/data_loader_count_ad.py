from torch.utils.data import TensorDataset, DataLoader, RandomSampler, SequentialSampler
import torch
import numpy as np

TORCH_INT_TYPE = torch.int16
NP_INT_TYPE = np.int16


def create_data_loader(load_train, labels_train, batch_size, random=True):
    train_data = TensorDataset(
        torch.tensor(load_train, dtype=torch.float),
        torch.tensor(labels_train, dtype=torch.float))
    if random is True:
        train_sampler = RandomSampler(train_data)
    else:
        train_sampler = SequentialSampler(train_data)
    train_dataloader = DataLoader(train_data, sampler=train_sampler, batch_size=batch_size)

    return train_dataloader
