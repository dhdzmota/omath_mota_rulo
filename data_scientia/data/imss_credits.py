#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import requests
import pandas as pd

from src import config

DATA_PATH = os.path.join(
    config.DATA_DIR,
    'raw/imss_credits.csv')

API_URL = (
   'https://api.datamexico.org/tesseract/cubes/'
   'imss_credits/aggregate.jsonrecords?'
   'drilldowns%5B%5D=Sex.Sex.Sex&'
   'drilldowns%5B%5D=Geography+Municipality.Geography.Municipality&'
   'measures%5B%5D=Credits&'
   'parents=false&sparse=false')


def download():
    """
    """
    req = requests.get(API_URL)
    json_data = json.loads(req.text)

    data = pd.json_normalize(json_data['data'])

    if config.VERBOSE:
        print(DATA_PATH)

    data.to_csv(DATA_PATH, index=False)


def get():
    """
    """

    data = pd.read_csv(DATA_PATH)

    return data


if __name__ == '__main__':
    """
    """
    download()
