import unittest

from logsight.analytics_core.modules.log_parsing.parsing_lib.mask_config import MaskParserConfig


class TestMaskParserConfig(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()

    def test_load(self):
        config = MaskParserConfig()
        self.assertTrue(config.load())


