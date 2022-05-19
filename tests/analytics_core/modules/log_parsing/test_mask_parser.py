import time
import unittest

from logsight.analytics_core.modules.log_parsing.mask_parser import MaskLogParser
from tests.utils import TestInputConfig


class TestMaskLogParser(unittest.TestCase):

    def setUp(self) -> None:
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()

    def test_parse_template(self) -> None:
        # given
        test_logs = TestInputConfig.logsight_logs
        expected_template = TestInputConfig.default_template
        parser = MaskLogParser()

        # test
        result_logs = parser.parse(test_logs)
        self.assertEqual(len(test_logs), len(result_logs))
        self.assertTrue(all([parser.template_key in result_log.metadata for result_log in result_logs]))
        self.assertTrue(all([expected_template == result_log.metadata[parser.template_key] for result_log in result_logs]))

    def test_parse_parameters(self) -> None:
        # given
        test_logs = TestInputConfig.logsight_logs
        expected_parameter = TestInputConfig.default_parameter
        parser = MaskLogParser()
        parser.enable_parameter_extraction = True

        # test
        result_logs = parser.parse(test_logs)
        self.assertEqual(len(test_logs), len(result_logs))
        self.assertTrue(all([parser.parameters_key in result_log.metadata for result_log in result_logs]))
        self.assertTrue(all([len(result_log.metadata[parser.parameters_key]) == 1 for result_log in result_logs]))
        self.assertTrue(all([result_log.metadata[parser.parameters_key][0] == expected_parameter for result_log in result_logs]))

    def test_parse_empty(self) -> None:
        # given
        test_logs = []
        parser = MaskLogParser()

        # test
        result_logs = parser.parse(test_logs)
        self.assertEqual(0, len(result_logs))

    def test_parse_stress_template(self) -> None:
        # given
        num_logs = 1000000
        test_logs = [TestInputConfig.logsight_log] * num_logs
        parser = MaskLogParser()

        # test
        start = time.time()
        _ = parser.parse(test_logs)
        print(f"Parsing templates of {num_logs} log events took {time.time() - start} seconds.")

    def test_parse_stress_parameters(self) -> None:
        # given
        num_logs = 1000000
        test_logs = [TestInputConfig.logsight_log] * num_logs
        parser = MaskLogParser()
        parser.enable_parameter_extraction = True

        # test
        start = time.time()
        _ = parser.parse(test_logs)
        print(f"Parsing templates and extracting parameters of {num_logs} log events took {time.time() - start} seconds.")
