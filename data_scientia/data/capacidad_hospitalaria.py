#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import urllib3
import json
import pandas as pd

from data_scientia import config


# Data path
DATA_PATH = os.path.join(
    config.DATA_DIR,
    'raw/capacidad_hospitalaria.csv.gz')


def download():
    """Download data.

    Download hospital occupancy data.
    """
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

    # Get all cadidate dates.
    dates = [
        ('&refine.fecha=%s %s' % (
            year,
            '{:02d}'.format(month)
        )).replace(' ', '%2F')
        for year in ['2019', '2020']
        for month in range(0, 13)]

    # Query all the months
    data = []
    HTTP = urllib3.PoolManager()
    for date in dates:
        endpoint = search_api + datase_name + params + date
        response = HTTP.request(
            'GET',
            endpoint,
            headers={'Content-Type': 'application/json'})

        response_json = json.loads(response.data.decode('utf-8'))

        n_records = len(response_json['records'])
        if config.VERBOSE:
            print(date, n_records)
        if n_records > 0:
            data += response_json['records']

    # Denormalize data
    data = pd.json_normalize(data)

    # Reformat column names
    data.columns = [x.replace('fields.', '') for x in data.columns]

    # Add numberic notation of capacity status
    data['estatus_capacidad_uci_percent'] = data[
        'estatus_capacidad_uci'
    ].map({'Buena': 49, 'Media': 89, 'Crítica': 100})

    data['estatus_capacidad_uci_ordinal'] = data[
        'estatus_capacidad_uci'
    ].map({'Buena': 1, 'Media': 2, 'Crítica': 3})

    # Denormalize geolocation.
    data['coordenadas'] = data['coordenadas'].apply(eval)
    data['latitude'] = data['coordenadas'].apply(lambda x: x[0])
    data['longitude'] = data['coordenadas'].apply(lambda x: x[1])

    # Save data
    data.to_csv(
        DATA_PATH,
        index=False,
        compression='gzip')


def get():
    """Get data.

    Fetch hospital occupancy data.

    Returns
    -------
    data: pandas.DataFrame
    """

    data = pd.read_csv(
        DATA_PATH,
        compression='gzip')

    data['fecha'] = pd.to_datetime(data['fecha'])

    data = data[~data['estatus_capacidad_uci_percent'].isnull()]

    return data


if __name__ == '__main__':
    """
    """

    download()
