import json
import re
import logging
import dateutil.parser as date_parser


def has_numbers(s):
    return any(char.isdigit() for char in s)


def seq_dist(seq1, seq2):
    assert len(seq1) == len(seq2)
    sim_tokens = 0
    number_of_pairs = 0

    for token1, token2 in zip(seq1, seq2):
        if token1 == '<*>':
            number_of_pairs += 1
            continue
        if token1 == token2:
            sim_tokens += 1

    ret_value = float(sim_tokens) / len(seq1)

    return ret_value, number_of_pairs


def get_parameter_list(log, template):
    template_regex = re.sub(r"<.>", "<*>", template)
    if "<*>" not in template_regex:
        return []
    template_regex = re.sub(r'([^A-Za-z0-9])', r'\\\1', template_regex)
    template_regex = re.sub(r'\\ +', r'\s+', template_regex)
    template_regex = "^" + template_regex.replace("\<\*\>", "(.*?)") + "$"
    parameter_list = re.findall(template_regex, log)
    parameter_list = parameter_list[0] if parameter_list else ()
    parameter_list = list(parameter_list) if isinstance(parameter_list, tuple) else [parameter_list]
    return parameter_list


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    # TODO: Remove logging from this function.
    try:
        int(string)
        return False
    except Exception as e:
        logging.log(logging.DEBUG, e)
        pass

    try:
        float(string)
        return False
    except Exception as e:
        logging.log(logging.DEBUG, e)
        pass
    try:
        date_parser.parse(string, fuzzy=fuzzy)
        return True
    except Exception as e:
        logging.log(logging.DEBUG, e)
        return False


def get_template(seq1, seq2):
    assert len(seq1) == len(seq2)
    ret_value = []

    i = 0
    for word in seq1:
        if word == seq2[i]:
            ret_value.append(word)
        else:
            ret_value.append('<*>')

        i += 1

    return ret_value


def add_parameters_to_log_json(log, new_template, parameter_list):
    # i = 0
    for w in range(len(parameter_list)):
        # if is_date(parameter_list[w]):
        #     log['param_' + str(w)] = json.dumps(parameter_list[w])
        # else:
        log['param_' + str(w)] = parameter_list[w]
        # i += 1
    return log
