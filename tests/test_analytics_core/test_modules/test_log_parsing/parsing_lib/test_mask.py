import unittest

from logsight.analytics_core.modules.log_parsing.parsing_lib.masking import MaskingInstruction


class TestMaskingInstruction(unittest.TestCase):
    regex_pattern = "((?<=[^A-Za-z0-9])|^)(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})((?=[^A-Za-z0-9])|$)"
    mask_with = "IP"
    mask_prefix = "<:"
    mask_suffix = ":>"

    def setUp(self) -> None:
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()

    def test_mask(self):
        # given
        test_cases = [
            {
                "input": "This is an IP: 192.168.77.31",
                "expected": "This is an IP: <:IP:>"
            },
            {
                "input": "192.168.77.31",
                "expected": "<:IP:>"
            },
            {
                "input": "No masking here",
                "expected": "No masking here"
            },
            {
                "input": "",
                "expected": ""
            }
        ]
        mi = MaskingInstruction(pattern=self.regex_pattern, mask_with=self.mask_with)

        # run test
        for test_case in test_cases:
            result = mi.mask(test_case["input"], self.mask_prefix, self.mask_suffix)
            self.assertEqual(test_case["expected"], result)

    def test_mask_with_none(self):
        # given
        mi = MaskingInstruction(pattern=self.regex_pattern, mask_with=self.mask_with)

        # run test
        # mi.mask is expected to throw an TypeError since a non-nullable string argument is expected
        self.assertRaises(TypeError, mi.mask, None, self.mask_prefix, self.mask_suffix)
