import logging
import sys
from os.path import dirname
from .base import Parser

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.file_persistence import FilePersistence

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')
MESSAGE_MAX_SIZE = 2000
TOO_LONG_TEMPLATE = "TOO_LONG_TEMPLATE"


class DrainLogParser(Parser):
    def __init__(self):
        super().__init__()
        self.config = TemplateMinerConfig()
        self.config.load(dirname(__file__) + "/drain3.ini")
        self.persistence = None
        self.template_miner = TemplateMiner(self.persistence, self.config)

    def parse(self, log):
        if len(log['message']) > MESSAGE_MAX_SIZE:
            log['template'] = TOO_LONG_TEMPLATE
            return log
        result = self.template_miner.masker.mask(log['message'])
        log['template'] = result
        return log

    def process_log(self, log):
        return self.parse(log)
