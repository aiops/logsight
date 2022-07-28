import pytest

from logsight.analytics_core.modules.anomaly_detection.core.tokenizer import LogTokenizer


@pytest.fixture
def new_word():
    return "NewWord"


@pytest.fixture
def existing_word():
    return "[PAD]"


@pytest.fixture
def message():
    return "Hello world log message 32 alphanum1"


def test_add_word(existing_word):
    tokenizer = LogTokenizer()
    tokenizer.add_word(existing_word)

    assert existing_word in tokenizer.word2index.keys()
    assert len(tokenizer.index2word) == 4
    assert tokenizer.n_words == 4


def test_add_new_word(new_word):
    tokenizer = LogTokenizer()
    tokenizer.add_word(new_word)
    assert new_word in tokenizer.word2index.keys()
    assert len(tokenizer.index2word) == 5
    assert tokenizer.n_words == 5


def test_tokenize(message):
    tokenizer = LogTokenizer().load_from_pickle()
    result = tokenizer.tokenize(message)
    assert result == [1, 52, 2378, 28, 5]


def test_load_from_pickle():
    tokenizer = LogTokenizer.load_from_pickle()
    assert isinstance(tokenizer, LogTokenizer)
    assert len(tokenizer.index2word) == 60180
    assert len(tokenizer.stop_words) == 179


def test_load_from_json():
    tokenizer = LogTokenizer.load_from_json()
    assert isinstance(tokenizer, LogTokenizer)
    assert len(tokenizer.index2word) == 60180
    assert len(tokenizer.stop_words) == 179
