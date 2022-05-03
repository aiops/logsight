import numpy as np


def softmax(x):
    e_x = np.exp(x - np.max(x, axis=1, keepdims=True))  # subtracts each row with its max value
    _sum = np.sum(e_x, axis=1, keepdims=True)  # returns sum of each row and keeps same dims
    f_x = e_x / _sum
    return f_x


def get_padded_data(arr, max_pad, pad_value=0):
    return np.array([np.pad(i, (0, max_pad - len(i)), mode='constant', constant_values=pad_value) for i in arr])
