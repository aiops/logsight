import datetime
import pickle

import numpy as np
import pandas as pd


class ModelNotLoadedException(Exception):
    pass


class CountADPredictor:
    MINIMUM_CHANGE_PERCENTAGE = 0.2

    def __init__(self):
        self.label_encoder = None
        self.model = None
        self.model_loaded = False

    def load_model(self, config):

        self.model = pickle.load(
            open('/models/' + config['user_app'] + '_model_count_ad' + '_regular' + '.pickle', 'rb'))
        self.label_encoder = pickle.load(
            open('/models/' + config['user_app'] + '_le_count_ad' + '_regular' + '.pickle', 'rb'))

    def predict(self, data):
        if not self.model_loaded:
            raise ModelNotLoadedException("Model is not loaded")
        new_data = data[~data['template'].isin(self.label_encoder.classes_)]
        data.template[~data['template'].isin(self.label_encoder.classes_)] = "NEW_TEMPLATE"
        data['template_labels'] = self.label_encoder.serialize(data.template.values)
        window_size_in_seconds = 60
        tmp = tmp_tmp = None
        for window in range(1):
            start_time = data.index[window].to_pydatetime() + datetime.timedelta(
                seconds=int(window_size_in_seconds / 2))
            end_time = start_time + datetime.timedelta(seconds=int(window_size_in_seconds / 2))
            mask = (data.index >= start_time) & (data.index <= end_time)
            unique, counts = np.unique(data.loc[mask].template_labels.values, return_counts=True)
            tmp = dict(zip(unique, counts))

        for window in range(1):
            start_time = data.index[window].to_pydatetime()
            end_time = start_time + datetime.timedelta(seconds=2 * window_size_in_seconds)
            mask = (data.index >= start_time) & (data.index <= end_time)
            data_tmp = data.loc[mask].template_labels.values
            unique, counts = np.unique(data_tmp, return_counts=True)
            tmp_tmp = dict(zip(unique, counts))

        predictions = []
        output = []
        for i in tmp.keys():
            try:
                tmp_rate = {}
                idx, smr = self.model[i][0], self.model[i][1]

                # print(i," is correlated with : ", idx, " with smr ", smr)
                for j in idx:
                    try:
                        # The above code is checking if the rate is a string or a float. If it is a string, it is
                        # converted to a float.
                        tmp_rate[j] = tmp_tmp[j]
                    except Exception as e:
                        print(e)
                        tmp_rate[j] = 0
                tmp_rate = pd.DataFrame(tmp_rate, index=np.arange(0, len(tmp_rate)))
                tmp_rate.iloc[0] = tmp_rate.iloc[0] / tmp_rate.iloc[0].sum()

                tmp_rate = tmp_rate.iloc[0].values
                anom_idx = []
                for t in range(len(tmp_rate)):
                    if np.abs((tmp_rate[t] - smr[t]) / smr[t]) >= self.MINIMUM_CHANGE_PERCENTAGE:
                        # tmp_a = [self.label_encoder.inverse_transform([idx[k]])[0] for k in range(len(idx))]
                        # tmp_b = data.drop_duplicates(subset=['template'])
                        # tmp_c = tmp_b[tmp_b['template'].isin(tmp_a)]
                        anom_idx.append((self.label_encoder.inverse_transform([int(idx[t])])[0],
                                         float(tmp_rate[t]),
                                         float(smr[t]),
                                         [{"templatetags": data[
                                                               data.template ==
                                                               self.label_encoder.inverse_transform([idx[k]])[0]].iloc[
                                                           0:1].dropna(axis='columns').to_dict('records'),
                                           "rate_now": tmp_rate[k], "expected_rate": smr[k]} for k in
                                          range(len(idx))]
                                         ))

                if anom_idx:
                    prediction = len(anom_idx) / len(idx)
                    predictions.append(prediction)
                    output.append([anom_idx, prediction])

            except Exception as e:
                print(e)

        prediction = np.mean(predictions) if predictions else 0
        new_data = new_data.drop_duplicates(subset=['template'])
        new_data.index = new_data.index.astype(str)
        # new_templates = [new_data.iloc[i:i + 1].dropna(axis='columns').to_dict('records') for i in range(len(new_data))]

        x0, idx = np.unique([j[0] for i in output for j in i[0]], return_index=True)
        # print("OUTPUT 0", x0, idx)

        x1 = np.array([j[1] for i in output for j in i[0]])[idx]
        # print("OUTPUT 1", x1)
        x2 = np.array([j[2] for i in output for j in i[0]])[idx]
        # print("OUTPUT 2", x2)
        x3 = np.array([j[3] for i in output for j in i[0]])[idx]

        data = data.drop_duplicates(subset=['template'])
        x_df = data[data['template'].isin(x0)]
        x0_i = np.array([i for i in range(len(x0)) if x0[i] in x_df.template.values])
        x_df['rate_now'] = x1[x0_i].tolist()
        x_df['smrs'] = x2[x0_i].tolist()
        x_df['templates_to_go'] = x3[x0_i].tolist()

        return x_df
