import re
import unittest

from logsight_lib.log_parsing import DrainLogParser


class ParsingRegexTest(unittest.TestCase):
    def test_valid_regex_to_be_parsed(self):

        test_lines = [
            "2021-12-16 05:15:56,329 INFO org.eclipse.jetty.server.handler.ContextHandler: Started o.e.j.w.WebAppContext@1800a575{datanode,/,file:///home/hduser/hadoop-3.3.0/share/hadoop/hdfs/webapps/datanode/,AVAILABLE}{file:/home/hduser/hadoop-3.3.0/share/hadoop/hdfs/webapps/datanode}",
            "2021-12-16 05:15:57,821 INFO org.eclipse.jetty.server.Server: Started @1368ms, host_ms=52, /file/file2/file.txt"]

        test_lines_correct_outputs = [
            "<*> <*> INFO <*> Started <*>",
            "<*> <*> INFO <*> Started <*> host_ms = <*> , <*>"
        ]

        parser = DrainLogParser()
        for line, line_correct_output in zip(test_lines, test_lines_correct_outputs):
            parsed_line, unparsed_line = parser.preprocess(line)
            self.assertEqual(line_correct_output, parsed_line)  # add assertion here


if __name__ == '__main__':
    unittest.main()
