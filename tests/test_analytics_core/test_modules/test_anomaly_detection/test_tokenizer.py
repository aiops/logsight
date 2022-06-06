import pytest

from analytics_core.modules.anomaly_detection.core.tokenizer import LogTokenizer


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


def test__preprocess(message):
    tokenizer = LogTokenizer()
    result = tokenizer._preprocess(message)
    num_valid = len(message.split(" ")) - 2  # two invalid words
    assert len(result) == num_valid + 1  # two invalid dropped + special token
    assert result[0] == "[CLS]"
    assert any(x.isupper() for x in "".join(result[1:])) is False  # no uppercase
    assert any(x.isdigit() for x in "".join(result[1:])) is False  # no numbers
    assert all(x.isalpha() for x in "".join(result[1:])) is True  # all alphabet


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
