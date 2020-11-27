#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 21:51:56 2020

@author: Raul Sanchez-Vazquez
"""

import os
import urllib3
import json
import pandas as pd


from omath_mota_rulo import config


DATA_PATH = os.path.join(
    config.DATA_DIR,
    'raw/capacidad_hospitalaria.csv.gz')


def download():
    """
    """
    HTTP = urllib3.PoolManager()

    search_api = 'https://datos.cdmx.gob.mx/api/records/1.0/search/'
    datase_name = '?dataset=capacidad-hospitalaria&'

    params = '&'.join([
            'q=',
            'facet=fecha',
            'facet=nombre_hospital',
            'facet=institucion',
            'facet=estatus_capacidad_hospitalaria',
            'facet=estatus_capacidad_uci',
            'rows=-1'
    ])

    dates = [
        ('&refine.fecha=%s %s' % (
            year,
            '{:02d}'.format(month)
        )).replace(' ', '%2F')
        for year in ['2019', '2020']
        for month in range(0, 12)]


    data = []
    for date in dates:

        endpoint = search_api + datase_name + params + date
        response = HTTP.request(
            'GET',
            endpoint,
            headers={'Content-Type': 'application/json'})

        response_json = json.loads(response.data.decode('utf-8'))

        n_records = len(response_json['records'])
        print(date, n_records)
        if n_records > 0:
            data += response_json['records']

    data = pd.json_normalize(data)

    data.to_csv(
        DATA_PATH,
        index=False,
        compression='gzip')


def get():
    """
    """

    data = pd.read_csv(
        DATA_PATH,
        compression='gzip')

    return data


if __name__ == '__main__':

    download()
