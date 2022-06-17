import multiprocessing
import re
from multiprocessing import Pool
from typing import List, NamedTuple, Optional

from cachetools import cachedmethod, LRUCache

from .parsing_lib.mask_config import MaskParserConfig
from .parsing_lib.masking import LogMasker
from .parser import Parser
from ...logs import LogsightLog

ExtractedParameter = NamedTuple("ExtractedParameter", [("value", str), ("mask_name", str)])


class MaskLogParser(Parser):
    def __init__(self):
        super().__init__()
        config = MaskParserConfig()
        config.load()
        self.masker = LogMasker(config.masking_instructions, config.mask_prefix, config.mask_suffix)
        self.template_key = config.template_key
        self.parameters_key = config.parameters_key
        self.enable_parameter_extraction = config.enable_parameter_extraction
        self.parameter_extraction_cache = LRUCache(config.parameter_extraction_cache_capacity)

        cpu_count = multiprocessing.cpu_count()
        self.worker_pool = Pool(cpu_count - 1) if (cpu_count - 1) else Pool(cpu_count)

    # The Pool.map method applies pickle do IPC with python objects. It cannot pickle the pool itself.
    # We Exclude the worker_pool from pickle to prevent Pool.map raise an exception
    # See: https://stackoverflow.com/questions/25382455/python-notimplementederror-pool-objects-cannot-be-passed-between-processes
    def __getstate__(self):
        self_dict = self.__dict__.copy()
        del self_dict['worker_pool']
        return self_dict

    def __setstate__(self, state):
        self.__dict__.update(state)

    def close(self):
        self.worker_pool.close()

    def parse(self, logs: List[LogsightLog]) -> List[LogsightLog]:
        return self.worker_pool.map(self._run_parse, logs)

    def _run_parse(self, log: LogsightLog) -> LogsightLog:
        """
        This method will parse the templates via masking and extract parameters if parameter extraction is enabled.
        Parallel execution of this method via worker threads is assumed.
        """
        template = self.masker.mask(log.message)
        log.metadata[self.template_key] = template
        if self.enable_parameter_extraction:
            parameters = self._extract_parameters(template, log.message)
            log.metadata[self.parameters_key] = parameters
        return log

    def _get_parameter_list(self, log_template: str, log_message: str) -> List[str]:
        """
        Extract parameters from a log message according to a provided template that was generated
        by calling `add_log_message()`.
        This function is deprecated. Please use extract_parameters instead.
        :param log_template: log template corresponding to the log message
        :param log_message: log message to extract parameters from
        :return: An ordered list of parameter values present in the log message.
        """

        extracted_parameters = self._extract_parameters(log_template, log_message, exact_matching=False)
        if not extracted_parameters:
            return []
        return [parameter.value for parameter in extracted_parameters]

    def _extract_parameters(
            self, log_template: str, log_message: str, exact_matching: bool = True
    ) -> Optional[List[ExtractedParameter]]:
        """
        Extract parameters from a log message according to a provided template that was generated
        by calling `add_log_message()`.
        For most accurate results, it is recommended that
        - Each `MaskingInstruction` has a unique `mask_with` value,
        - No `MaskingInstruction` has a `mask_with` value of `*`,
        - The regex-patterns of `MaskingInstruction` do not use unnamed back-references;
          instead use back-references to named groups e.g. `(?P=some-name)`.
        :param log_template: log template corresponding to the log message
        :param log_message: log message to extract parameters from
        :param exact_matching: whether to apply the correct masking-patterns to match parameters, or try to approximate;
            disabling exact_matching may be faster but may lead to situations in which parameters
            are wrongly identified.
        :return: A ordered list of ExtractedParameter for the log message
            or None if log_message does not correspond to log_template.
        """

        template_regex, param_group_name_to_mask_name = self._get_template_parameter_extraction_regex(
            log_template, exact_matching)

        # Parameters are represented by specific named groups inside template_regex.
        parameter_match = re.match(template_regex, log_message)

        # log template does not match template
        if not parameter_match:
            return None

        # create list of extracted parameters
        extracted_parameters = []
        for group_name, parameter in parameter_match.groupdict().items():
            if group_name in param_group_name_to_mask_name:
                mask_name = param_group_name_to_mask_name[group_name]
                extracted_parameter = ExtractedParameter(parameter, mask_name)
                extracted_parameters.append(extracted_parameter)

        return extracted_parameters

    @cachedmethod(lambda self: self.parameter_extraction_cache)
    def _get_template_parameter_extraction_regex(self, log_template: str, exact_matching: bool):
        param_group_name_to_mask_name = dict()
        param_name_counter = [0]

        def get_next_param_name():
            param_group_name = "p_" + str(param_name_counter[0])
            param_name_counter[0] += 1
            return param_group_name

        # Create a named group with the respective patterns for the given mask-name.
        def create_capture_regex(_mask_name):
            allowed_patterns = []
            if exact_matching:
                # get all possible regex patterns from masking instructions that match this mask name
                masking_instructions = self.masker.instructions_by_mask_name(_mask_name)
                for mi in masking_instructions:
                    # MaskingInstruction may already contain named groups.
                    # We replace group names in those named groups, to avoid conflicts due to duplicate names.
                    if hasattr(mi, 'regex'):
                        mi_groups = mi.regex.groupindex.keys()
                        pattern = mi.pattern
                    else:
                        # non regex masking instructions - support only non-exact matching
                        mi_groups = []
                        pattern = ".+?"

                    for group_name in mi_groups:
                        param_group_name = get_next_param_name()

                        def replace_captured_param_name(param_pattern):
                            _search_str = param_pattern.format(group_name)
                            _replace_str = param_pattern.format(param_group_name)
                            return pattern.replace(_search_str, _replace_str)

                        pattern = replace_captured_param_name("(?P={}")
                        pattern = replace_captured_param_name("(?P<{}>")

                    # support unnamed back-references in masks (simple cases only)
                    pattern = re.sub(r"\\(?!0)\d{1,2}", r"(?:.+?)", pattern)
                    allowed_patterns.append(pattern)

            if not exact_matching or _mask_name == "*":
                allowed_patterns.append(r".+?")

            # Give each capture group a unique name to avoid conflicts.
            param_group_name = get_next_param_name()
            param_group_name_to_mask_name[param_group_name] = _mask_name
            joined_patterns = "|".join(allowed_patterns)
            capture_regex = "(?P<{}>{})".format(param_group_name, joined_patterns)
            return capture_regex

        # For every mask in the template, replace it with a named group of all
        # possible masking-patterns it could represent (in order).
        mask_names = set(self.masker.mask_names)

        # the Drain catch-all mask
        mask_names.add("*")

        escaped_prefix = re.escape(self.masker.mask_prefix)
        escaped_suffix = re.escape(self.masker.mask_suffix)
        template_regex = re.escape(log_template)

        # replace each mask name with a proper regex that captures it
        for mask_name in mask_names:
            search_str = escaped_prefix + re.escape(mask_name) + escaped_suffix
            while True:
                rep_str = create_capture_regex(mask_name)
                # Replace one-by-one to get a new param group name for each replacement.
                template_regex_new = template_regex.replace(search_str, rep_str, 1)
                # Break when all replaces for this mask are done.
                if template_regex_new == template_regex:
                    break
                template_regex = template_regex_new

        # match also messages with multiple spaces or other whitespace chars between tokens
        template_regex = re.sub(r"\\ ", r"\\s+", template_regex)
        template_regex = "^" + template_regex + "$"
        return template_regex, param_group_name_to_mask_name
