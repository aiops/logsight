import configparser
import json
import os

from logsight.analytics_core.modules.log_parsing.parsing_lib.masking import MaskingInstruction


class MaskParserConfig:
    def __init__(self):
        self.masking_instructions = []
        self.mask_prefix = "<"
        self.mask_suffix = ">"
        self.enable_parameter_extraction = False
        self.parameter_extraction_cache_capacity = 3000
        self.parametrize_numeric_tokens = True
        self.template_key = "template"
        self.parameters_key = "parameters"

        # TODO: Pull out the configuration into pipeline building
        self.config_file_path = os.path.join(os.path.dirname(__file__), "mask_config.ini")
        self.section_masking = 'MASKING'

    def load(self) -> bool:
        parser = configparser.ConfigParser()
        if len(parser.read(self.config_file_path)) <= 0:
            return False  # Config files were not read. Default values are used

        masking_instructions_str = parser.get(self.section_masking, 'masking', fallback=str(self.masking_instructions))
        self.mask_prefix = parser.get(self.section_masking, 'mask_prefix', fallback=self.mask_prefix)
        self.mask_suffix = parser.get(self.section_masking, 'mask_suffix', fallback=self.mask_suffix)
        self.enable_parameter_extraction = parser.get(self.section_masking, 'enable_parameter_extraction',
                                                      fallback=self.enable_parameter_extraction)
        self.parameter_extraction_cache_capacity = parser.get(self.section_masking,
                                                              'parameter_extraction_cache_capacity',
                                                              fallback=self.parameter_extraction_cache_capacity)
        self.template_key = parser.get(self.section_masking, 'template_key', fallback=self.mask_suffix)
        self.parameters_key = parser.get(self.section_masking, 'parameters_key', fallback=self.mask_suffix)

        masking_instructions = []
        masking_list = json.loads(masking_instructions_str)
        for mi in masking_list:
            instruction = MaskingInstruction(mi['regex_pattern'], mi['mask_with'])
            masking_instructions.append(instruction)
        self.masking_instructions = masking_instructions

        return True
