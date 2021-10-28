from torch.utils.data import TensorDataset, DataLoader, RandomSampler, SequentialSampler
import torch
import numpy as np

TORCH_INT_TYPE = torch.int16
NP_INT_TYPE = np.int16


def create_data_loader(load_train, labels_train, batch_size):
    train_data = TensorDataset(
        torch.tensor(load_train, dtype=torch.int32),
        torch.tensor(labels_train.astype(np.int32), dtype=torch.int32))

    train_sampler = RandomSampler(train_data)
    train_dataloader = DataLoader(train_data, sampler=train_sampler, batch_size=batch_size)

    return train_dataloader


def create_test_loader(load_test, labels_test, batch_size):
    test_data = TensorDataset(
        torch.tensor(load_test, dtype=torch.int32),
        torch.tensor(labels_test.astype(np.int32), dtype=torch.int32))

    test_sampler = SequentialSampler(test_data)
    test_dataloader = DataLoader(test_data, sampler=test_sampler, batch_size=batch_size)

    return test_dataloader
