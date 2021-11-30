import re
from collections import deque
from typing import List
from .base import Parser
from .utils import has_numbers, seq_dist, get_parameter_list, get_template, add_parameters_to_log_json


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
            rex = [r'<\d+\ssec', r'0x.*?\s', r'(\d+\.){3}\d+(:\d+)?', r'\S*\d+\S*']
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

        parentn = rn.child_d[seq_len]

        current_depth = 1
        for token in seq:
            if current_depth >= self.depth or current_depth > seq_len:
                break

            if token in parentn.child_d:
                parentn = parentn.child_d[token]
            elif '<*>' in parentn.child_d:
                parentn = parentn.child_d['<*>']
            else:
                return ret_log_cluster
            current_depth += 1

        log_cluster_l = parentn.child_d

        ret_log_cluster = self.fast_match(log_cluster_l, seq)

        return ret_log_cluster

    def add_seq_to_prefix_tree(self, rn, log_cluster):
        seq_len = len(log_cluster.log_template)
        if seq_len not in rn.child_d:
            fist_layer_node = Node(depth=1, digit_token=seq_len)
            rn.child_d[seq_len] = fist_layer_node
        else:
            fist_layer_node = rn.child_d[seq_len]

        parentn = fist_layer_node

        current_depth = 1
        for token in log_cluster.log_template:

            # Add current log cluster to the leaf node
            if current_depth >= self.depth or current_depth > seq_len:
                if len(parentn.child_d) == 0:
                    parentn.child_d = deque(maxlen=100)
                parentn.child_d.append(log_cluster)
                break

            # If token not matched in this layer of existing tree.
            if token in parentn.child_d:
                parentn = parentn.child_d[token]

            elif not has_numbers(token):
                if '<*>' in parentn.child_d:
                    if len(parentn.child_d) < self.maxChild:
                        new_node = Node(depth=current_depth + 1, digit_token=token)
                        parentn.child_d[token] = new_node
                        parentn = new_node
                    else:
                        parentn = parentn.child_d['<*>']
                elif len(parentn.child_d) + 1 < self.maxChild:
                    new_node = Node(depth=current_depth + 1, digit_token=token)
                    parentn.child_d[token] = new_node
                    parentn = new_node
                elif len(parentn.child_d) + 1 == self.maxChild:
                    new_node = Node(depth=current_depth + 1, digit_token='<*>')
                    parentn.child_d['<*>'] = new_node
                    parentn = new_node
                else:
                    parentn = parentn.child_d['<*>']

            elif '<*>' not in parentn.child_d:
                new_node = Node(depth=current_depth + 1, digit_token='<*>')
                parentn.child_d['<*>'] = new_node
                parentn = new_node
            else:
                parentn = parentn.child_d['<*>']

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

    def parse(self, log):
        # filter message using regex
        log_message_preprocessed, log_tmp = self.preprocess(log['message'])
        log_message_preprocessed = log_message_preprocessed.strip().split()

        # trying to match existing cluster
        match_cluster = self.tree_search(self.rootNode, log_message_preprocessed)
        # Match no existing log cluster
        if match_cluster is None:
            if self.state in [Parser.TRAIN_STATE, Parser.TUNE_STATE]:
                new_cluster = LogCluster(log_message_preprocessed)
                self.add_seq_to_prefix_tree(self.rootNode, new_cluster)
            log['template'] = ' '.join(log_message_preprocessed)
            parameter_list = get_parameter_list(log_tmp, ' '.join(log_message_preprocessed))
            log = add_parameters_to_log_json(log, log_message_preprocessed, parameter_list)
        else:
            new_template = get_template(log_message_preprocessed, match_cluster.log_template)
            new_template_str = ' '.join(new_template)
            if ' '.join(new_template) != ' '.join(match_cluster.log_template):
                match_cluster.log_template = new_template
            parameter_list = get_parameter_list(log_tmp, new_template_str)
            log['template'] = ' '.join(new_template)
            log = add_parameters_to_log_json(log, new_template, parameter_list)
        return log

    def preprocess(self, line):
        line_tmp = re.sub("=", " = ", line)
        line_tmp = re.sub("\"", "\\\"", line_tmp)
        if self.rex is not None:
            for current_rex in self.rex:
                line = re.sub(current_rex, '<*>', line_tmp)
        return line, line_tmp

    def process_log(self, log):
        return self.parse(log)
