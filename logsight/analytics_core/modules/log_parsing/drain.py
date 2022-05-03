import re
from collections import deque
from typing import List

from .base import Parser
from .utils import add_parameters_to_log_json, get_parameter_list, get_template, has_numbers, seq_dist
from ...logs import LogsightLog


class Node:
    def __init__(self, child_d=None, depth=0, digit_token=None):
        self.child_d = child_d or {}
        self.depth = depth
        self.digit_token = digit_token

    def __str__(self):
        return self.child_d.__str__()


class LogCluster:
    def __init__(self, log_template: List[str] = None):
        self.log_template = log_template or []


class DrainLogParser(Parser):
    def __init__(self, depth=3, st=0.35, max_child=100, rex=None, keep_para=True):
        super().__init__()
        if rex is None:
            rex = [r'(\S*\d+\S*)',
                   r'(\S*((?:[A-Z]:|(?<![:/\\])[\\\/]|\~[\\\/]|(?:\.{1,2}[\\\/])+)[\w+\\\s_\-\(\)\/]*(?:\.\w+)*)\S*)',
                   r'(\S*(?:\.+\S*)+)']
        self.rootNode = Node()
        self.depth = depth - 2
        self.st = st
        self.maxChild = max_child
        self.logName = None
        self.df_log = None
        self.rex = rex
        self.keep_para = keep_para
        self.mean_sum = 0

    def tree_search(self, rn, seq):
        ret_log_cluster = None

        seq_len = len(seq)
        if seq_len not in rn.child_d:
            return ret_log_cluster

        parent_n = rn.child_d[seq_len]

        current_depth = 1
        for token in seq:
            if current_depth >= self.depth or current_depth > seq_len:
                break

            if token in parent_n.child_d:
                parent_n = parent_n.child_d[token]
            elif '<*>' in parent_n.child_d:
                parent_n = parent_n.child_d['<*>']
            else:
                return ret_log_cluster
            current_depth += 1

        log_cluster_l = parent_n.child_d

        ret_log_cluster = self.fast_match(log_cluster_l, seq)

        return ret_log_cluster

    def add_seq_to_prefix_tree(self, rn, log_cluster):
        seq_len = len(log_cluster.log_template)
        if seq_len not in rn.child_d:
            fist_layer_node = Node(depth=1, digit_token=seq_len)
            rn.child_d[seq_len] = fist_layer_node
        else:
            fist_layer_node = rn.child_d[seq_len]

        parent_n = fist_layer_node

        current_depth = 1
        for token in log_cluster.log_template:

            # Add current log cluster to the leaf node
            if current_depth >= self.depth or current_depth > seq_len:
                if len(parent_n.child_d) == 0:
                    parent_n.child_d = deque(maxlen=100)
                parent_n.child_d.append(log_cluster)
                break

            # If token not matched in this layer of existing tree.
            if token in parent_n.child_d:
                parent_n = parent_n.child_d[token]

            elif not has_numbers(token):
                if '<*>' in parent_n.child_d:
                    if len(parent_n.child_d) < self.maxChild:
                        new_node = Node(depth=current_depth + 1, digit_token=token)
                        parent_n.child_d[token] = new_node
                        parent_n = new_node
                    else:
                        parent_n = parent_n.child_d['<*>']
                elif len(parent_n.child_d) + 1 < self.maxChild:
                    new_node = Node(depth=current_depth + 1, digit_token=token)
                    parent_n.child_d[token] = new_node
                    parent_n = new_node
                elif len(parent_n.child_d) + 1 == self.maxChild:
                    new_node = Node(depth=current_depth + 1, digit_token='<*>')
                    parent_n.child_d['<*>'] = new_node
                    parent_n = new_node
                else:
                    parent_n = parent_n.child_d['<*>']

            elif '<*>' not in parent_n.child_d:
                new_node = Node(depth=current_depth + 1, digit_token='<*>')
                parent_n.child_d['<*>'] = new_node
                parent_n = new_node
            else:
                parent_n = parent_n.child_d['<*>']

            current_depth += 1

    def fast_match(self, log_cluster_l, seq):
        max_sim = -1
        max_num_of_para = -1
        max_clusters = None

        for log_cluster in log_cluster_l:
            cur_sim, cur_num_of_para = seq_dist(log_cluster.log_template, seq)

            if cur_sim > max_sim or (cur_sim == max_sim and cur_num_of_para > max_num_of_para):
                max_sim = cur_sim
                max_num_of_para = cur_num_of_para
                max_clusters = log_cluster

        return max_clusters if max_sim >= self.st else None

    def parse(self, log: LogsightLog) -> LogsightLog:
        # filter message using regex
        log_message_preprocessed, log_tmp = self.preprocess(log.event.message)
        log_message_preprocessed = log_message_preprocessed.strip().split()

        # trying to match existing cluster
        match_cluster = self.tree_search(self.rootNode, log_message_preprocessed)

        # Match no existing log cluster
        if match_cluster is None:
            log.metadata['template'] = ' '.join(log_message_preprocessed)
            parameter_list = get_parameter_list(log_tmp, ' '.join(log_message_preprocessed))
            add_parameters_to_log_json(log.metadata, log_message_preprocessed, parameter_list)
        else:
            new_template = get_template(log_message_preprocessed, match_cluster.log_template)
            new_template_str = ' '.join(new_template)
            if ' '.join(new_template) != ' '.join(match_cluster.log_template):
                match_cluster.log_template = new_template
            parameter_list = get_parameter_list(log_tmp, new_template_str)
            log.metadata['template'] = ' '.join(new_template)
            add_parameters_to_log_json(log.metadata, new_template, parameter_list)

        return log

    def preprocess(self, line):
        line_tmp = re.sub(r'=[A-Z0-9 a-z]*', r" = <*> ", line)
        line_tmp = line_tmp.replace("\"", '\\\"')
        line_r = line_tmp
        if self.rex is not None:
            for current_rex in self.rex:
                line_r = re.sub(current_rex, '<*>', line_r)
        return line_r, line_tmp

    def process_log(self, log):
        return self.parse(log)
