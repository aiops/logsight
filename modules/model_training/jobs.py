import pickle

import torch

from modules.api.job import Job
from abc import abstractmethod
from .log_ad_training.count_ad_training import train_count_ad_model


class TrainModelJob(Job):
    def __init__(self, job_config, **kwargs):
        super().__init__(job_config, **kwargs)

    def _execute(self):
        print("[JOB] Loading data")
        data = self._load_data()
        print("[JOB] Training model")

        model = self._train_model(data)
        print("[JOB] Saving model")

        model_path = self._save_model(model)
        print('[JOB] Done execute.')
        return model_path

    @abstractmethod
    def _load_data(self):
        raise NotImplementedError

    @abstractmethod
    def _train_model(self, data):
        raise NotImplementedError

    @abstractmethod
    def _save_model(self, model):
        raise NotImplementedError


class TrainCountADModelJob(TrainModelJob):
    def _load_data(self):
        pass

    def _train_model(self, data):
        return train_count_ad_model(data)

    def _save_model(self, model):
        model, le, templates = model
        with open('models/' + self.job_config['user_app'] + '_model_count_ad_' +
                  self.job_config["status"] + "_" + self.job_config["baselineTagId"] + '.pickle', "wb") as file:
            pickle.dump(model, file)

        with open('models/' + self.job_config['user_app'] + '_template_count_ad_' +
                  self.job_config["status"] + "_" + self.job_config["baselineTagId"] + '.pickle', "wb") as file:
            pickle.dump(templates, file)

        with open('models/' + self.job_config['user_app'] + '_le_count_ad_' +
                  self.job_config["status"] + "_" + self.job_config["baselineTagId"] + '.pickle', "wb") as file:
            pickle.dump(le, file)


class TrainLogsyADModelJob(TrainModelJob):
    def _load_data(self):
        pass

    def _train_model(self, data):
        return torch.load('models/model_github.pth', map_location=torch.device('cpu'))

    def _save_model(self, model):
        path = 'models/' + self.job_config['user_app'] + '_model_toy_example_plus_anomalies_fine_tuned0.pth'
        torch.save(model, path)
        return path
