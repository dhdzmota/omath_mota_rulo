from chalice import Chalice

from chalice import Rate
from data_scientia.data import capacidad_hospitalaria
from data_scientia.data import covid_municipalities
from data_scientia.config import DATA_DIR
from data_scientia.features.train_data import get_municipio_features

from chalicelib import config

import boto3
import json
from io import StringIO
import numpy as np
import json

app = Chalice(app_name='data-scientia')

with open(config.MODEL_JSON_FILE_PATH, 'r') as f:
    model_json = json.loads(f.read())


@app.route('/')
def index():
    return {'Data Mexico': 'Data Scientia'}


@app.schedule(Rate(12, unit=Rate.HOURS))
def get_capacidad():
    capacidad_hospitalaria.download()
    return {'Resulst': 'Success'}


@app.schedule(Rate(12, unit=Rate.HOURS))
def get_contagios():
    bucket = 'data-scientia'

    with open(DATA_DIR+'/codes.json', 'r') as j:
        codes = json.loads(j.read())
    for code, municipio in codes.items():
        print(code)
        df = covid_municipalities.download_municipio_covid_data(code)
        if df is None:
            continue
        df['municipio_code'] = code
        df['municipio'] = municipio
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, str(code)+'.csv').put(Body=csv_buffer.getvalue())
    return {'Resulst': 'Success'}


@app.route('/get_score', methods=['POST'])
def get_score():

    request = app.current_request
    json_data = request.json_body

    hospital = json_data['hospital']
    date = json_data['date']
    print("Getting features for: "+str(hospital)+' '+str(date))
    X = get_municipio_features(hospital, [date]).iloc[0].to_dict()
    X = {x[0]: to_nan(x[1]) for x in X.items()}

    print("Features: "+str(X))

    score = predict_proba(X)

    return {'Result': score}


def get_tree_leaf(node, x):
    """Get tree leaf score.
    """
    if 'leaf' in node:
        score = node['leaf']
        return score
    else:
        x_f_val = x[node['split']]
        if np.isnan(x_f_val):
            next_node_id = node['missing']
        elif x_f_val < node['split_condition']:
            next_node_id = node['yes']
        else:
            next_node_id = node['no']
        for children in node['children']:
            if children['nodeid'] == next_node_id:
                return get_tree_leaf(children, x)


def predict_proba(x):
    """Get model score.
    """
    logit = []
    for tree in model_json:
        leaf_score = get_tree_leaf(
            node=tree,
            x=x)
        logit.append(leaf_score)
    pos_class_probability = 1 / (1 + np.exp(-sum(logit)))
    return pos_class_probability


def to_nan(x):
    """
    """
    if type(x) not in [float, int]:
        return np.nan
    return x