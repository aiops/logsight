import re
import unittest


class ParsingRegexTest(unittest.TestCase):
    def test_valid_regex_to_be_parsed(self):
        rex = [r'(\S*\d+\S*)',
               r'(\S*((?:[A-Z]:|(?<![:/\\])[\\\/]|\~[\\\/]|(?:\.{1,2}[\\\/])+)[\w+\\\s_\-\(\)\/]*(?:\.\w+)*)\S*)',
               r'(\S*(?:\.+\S*)+)']

        test_lines = [
            "2021-12-16 05:15:56,329 INFO org.eclipse.jetty.server.handler.ContextHandler: Started o.e.j.w.WebAppContext@1800a575{datanode,/,file:///home/hduser/hadoop-3.3.0/share/hadoop/hdfs/webapps/datanode/,AVAILABLE}{file:/home/hduser/hadoop-3.3.0/share/hadoop/hdfs/webapps/datanode}"]
        "2021-12-16 05:15:57,821 INFO org.eclipse.jetty.server.Server: Started @1368ms, host_ms=52, /file/file2/file.txt"
        test_lines_correct_outputs = [
            "<*> <*> INFO <*> Started <*>",
            "<*> <*> INFO <*> Started <*>, host_ms= <*> <*>"
        ]

        for line, line_output in zip(test_lines, test_lines_correct_outputs):
            line_tmp = re.sub(r'=[A-Z 0-9 a-z]*', r" = <*> ", line)
            line_tmp = re.sub("\"", "\\\"", line_tmp)

            if rex is not None:
                for current_rex in rex:
                    line_tmp = re.sub(current_rex, '<*>', line_tmp)
            print(line_tmp)
            print(line_output)
            self.assertEqual(line_tmp,line_output)  # add assertion here


if __name__ == '__main__':
    unittest.main()
