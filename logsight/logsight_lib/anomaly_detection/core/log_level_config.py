class ConfigLogLevelEstimation:
    __conf = {
        'pad_len': 50,
        'max_len': 20,
        'log_mapper': {0: 'anomaly', 1: 'normal'}
    }

    @staticmethod
    def get(name):
        return ConfigLogLevelEstimation.__conf[name]

    @staticmethod
    def set(name, value):
        if name in ConfigLogLevelEstimation.__conf.keys():
            ConfigLogLevelEstimation.__conf[name] = value
        else:
            raise ValueError("The variable name does not exist! "
                             "The possible config variables to be set are:",
                             ConfigLogLevelEstimation.__conf.keys())
