import numpy as np


def softmax(x):
    e_x = np.exp(x - np.max(x, axis=1, keepdims=True))  # subtracts each row with its max value
    _sum = np.sum(e_x, axis=1, keepdims=True)  # returns sum of each row and keeps same dims
    f_x = e_x / _sum
    return f_x


def pad_sequences(sequences, maxlen=None, dtype='int32',
                  padding='post', truncating='post', value=0):
    if not hasattr(sequences, '__len__'):
        raise ValueError('`sequences` must be iterable.')
    num_samples = len(sequences)

    lengths = []
    sample_shape = ()
    flag = True

    # take the sample shape from the first non empty sequence
    # checking for consistency in the main loop below.

    for x in sequences:
        try:
            lengths.append(len(x))
            if flag and len(x):
                sample_shape = np.asarray(x).shape[1:]
                flag = False
        except TypeError as e:
            raise ValueError('`sequences` must be a list of iterables. '
                             f'Found non-iterable: {str(x)}') from e

    if maxlen is None:
        maxlen = np.max(lengths)

    is_dtype_str = np.issubdtype(dtype, np.str_) or np.issubdtype(
        dtype, np.unicode_)
    if isinstance(value, str) and dtype != object and not is_dtype_str:
        raise ValueError(
            f'`dtype` {dtype} is not compatible with `value`\'s type: '
            f'{type(value)}\nYou should set `dtype=object` for variable length '
            'strings.')

    x = np.full((num_samples, maxlen) + sample_shape, value, dtype=dtype)
    for idx, s in enumerate(sequences):
        if not len(s):  # pylint: disable=g-explicit-length-test
            continue  # empty list/array was found
        if truncating == 'pre':
            trunc = s[-maxlen:]  # pylint: disable=invalid-unary-operand-type
        elif truncating == 'post':
            trunc = s[:maxlen]
        else:
            raise ValueError(f'Truncating type "{truncating}" not understood')

        # check `trunc` has expected shape
        trunc = np.asarray(trunc, dtype=dtype)
        if trunc.shape[1:] != sample_shape:
            raise ValueError(f'Shape of sample {trunc.shape[1:]} of sequence at '
                             f'position {idx} is different from expected shape '
                             f'{sample_shape}')

        if padding == 'post':
            x[idx, :len(trunc)] = trunc
        elif padding == 'pre':
            x[idx, -len(trunc):] = trunc
        else:
            raise ValueError(f'Padding type "{padding}" not understood')
    return x
