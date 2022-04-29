import logging
import sys

try:
    import regex as re
except ImportError as e:
    # If you import re, grok_match can't handle regular expression containing atomic group(?>)
    import re
import codecs
import os
import pkg_resources

DEFAULT_PATTERNS_DIR = pkg_resources.resource_filename(__name__, 'patterns')
TIME_PATTERN_FILE = "times"
logger = logging.getLogger(__name__)


class Grok:
    def __init__(self, pattern: str, custom_patterns: dict = None, full_match: bool = True,
                 required_fields: list = None):
        self.pattern = pattern
        self.predefined_patterns = _reload_patterns([DEFAULT_PATTERNS_DIR])
        self.full_match = full_match
        self.required_fields = required_fields if required_fields else None

        custom_pats = {}
        if custom_patterns:
            for pat_name, regex_str in custom_patterns.items():
                custom_pats[pat_name] = Pattern(pat_name, regex_str)

        if len(custom_pats) > 0:
            self.predefined_patterns.update(custom_pats)

        self._load_search_pattern()

    def parse(self, message):
        """If text is matched with pattern, return variable names specified(%{pattern:variable name})
        in pattern and their corresponding values.If not matched, return None.
        custom patterns can be passed in by custom_patterns(pattern name, pattern regular expression pair)
        or custom_patterns_dir.
        """

        if self.full_match:
            match_obj = self.regex_obj.fullmatch(message)
        else:
            match_obj = self.regex_obj.search(message)

        if match_obj is None:
            return None
        matches = match_obj.groupdict()
        if not self._verify_matches(matches):
            return None

        for key, match in matches.items():
            try:
                if self.type_mapper[key] == 'int':
                    matches[key] = int(match)
                if self.type_mapper[key] == 'float':
                    matches[key] = float(match)
            except (TypeError, KeyError):
                pass

        return matches

    def set_search_pattern(self, pattern=None):
        if type(pattern) is not str:
            raise ValueError("Please supply a valid pattern")
        self.pattern = pattern
        self._load_search_pattern()

    def _verify_matches(self, match_obj):
        if self.required_fields:
            if all(k in match_obj for k in self.required_fields):
                return True
            else:
                return False
        else:
            return True

    def _load_search_pattern(self):
        self.type_mapper = {}
        py_regex_pattern = self.pattern
        while True:
            # Finding all types specified in the groks
            m = re.findall(r'%{(\w+):(\w+):(\w+)}', py_regex_pattern)
            for n in m:
                self.type_mapper[n[1]] = n[2]
            # replace %{pattern_name:custom_name} (or %{pattern_name:custom_name:type}
            # with regex and regex group name

            py_regex_pattern = re.sub(r'%{(\w+):(\w+)(?::\w+)?}',
                                      lambda x: "(?P<" + x.group(2) + ">" + self.predefined_patterns[
                                          x.group(1)].regex_str + ")",
                                      py_regex_pattern)

            # replace %{pattern_name} with regex
            py_regex_pattern = re.sub(r'%{(\w+)}',
                                      lambda x: "(" + self.predefined_patterns[x.group(1)].regex_str + ")",
                                      py_regex_pattern)

            if re.search(r'%{\w+(:\w+)?}', py_regex_pattern) is None:
                break

        self.regex_obj = re.compile(py_regex_pattern)


def _wrap_pattern_name(pat_name):
    return '%{' + pat_name + '}'


def _reload_patterns(patterns_dirs):
    all_patterns = {}
    for d in patterns_dirs:
        for f in os.listdir(d):
            patterns = _load_patterns_from_file(os.path.join(d, f))
            all_patterns.update(patterns)

    return all_patterns


def _load_patterns_from_file(file):
    patterns = {}
    with codecs.open(file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line == '' or line.startswith('#'):
                continue

            sep = line.find(' ')
            pat_name = line[:sep]
            regex_str = line[sep:].strip()
            pat = Pattern(pat_name, regex_str)
            patterns[pat.pattern_name] = pat
    return patterns


def read_grok_datetime_parsers():
    times_file = os.path.join(DEFAULT_PATTERNS_DIR, TIME_PATTERN_FILE)
    datetime_groks = {}
    with open(times_file, 'r') as f:
        for line in f.readlines():
            if line == '' or line.startswith('#'):
                continue
            try:
                time_pattern_name = line.split()[0]
            except Exception as ex:
                logger.info('Failed to retrieve time pattern name for line %s. Reason %s', line, ex)
                continue
            datetime_groks[time_pattern_name.lower()] = Grok(
                "%{{{}:timestamp}}".format(time_pattern_name), full_match=False
            )
    return datetime_groks


class Pattern(object):
    def __init__(self, pattern_name, regex_str, sub_patterns=None):
        self.pattern_name = pattern_name
        self.regex_str = regex_str
        self.sub_patterns = sub_patterns if sub_patterns else {}  # sub_pattern name list

    def __str__(self):
        return '<Pattern:%s,  %s,  %s>' % (self.pattern_name, self.regex_str, self.sub_patterns)
