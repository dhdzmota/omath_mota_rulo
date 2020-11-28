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

from data_scientia.features.utils import distance_utils
from data_scientia import config


DATA_PATH = os.path.join(
    config.DATA_DIR,
    'raw/capacidad_hospitalaria.csv.gz')

# Aux. variable use to keep the data in memory
_DATA_HOSPITAL = None
_HOSPITAL_LAT = None
_HOSPITAL_LONG = None


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


    # Query all the months
    data = []
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

    # Save data
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

    data['fecha'] = pd.to_datetime(data['fecha'])
    data['coordenadas'] = data['coordenadas'].apply(eval)

    data['latitude'] = data['coordenadas'].apply(lambda x: x[0])
    data['longitude'] = data['coordenadas'].apply(lambda x: x[1])

    return data


def load():
    """
    """
    global _DATA_HOSPITAL
    global _HOSPITAL_LAT
    global _HOSPITAL_LONG

    if _DATA_HOSPITAL is None:
        _DATA_HOSPITAL = get()

    if _HOSPITAL_LAT is None:
        _HOSPITAL_LAT = _DATA_HOSPITAL.groupby('nombre_hospital')['latitude'].mean()

    if _HOSPITAL_LONG is None:
        _HOSPITAL_LONG = _DATA_HOSPITAL.groupby('nombre_hospital')['longitude'].mean()


def get_hospital_near_geo(geo_points, max_meters=5e+3):

    load()

    global _DATA_HOSPITAL
    global _HOSPITAL_LAT
    global _HOSPITAL_LONG


    hospitales_in_radio = []
    for lat, long in geo_points:
        lat_a = [lat] * _HOSPITAL_LAT.shape[0]
        long_a = [long] * _HOSPITAL_LONG.shape[0]

        dist = pd.Series(distance_utils.coord_dist(
            lat_a=lat_a,
            long_a=long_a,
            lat_b=_HOSPITAL_LAT,
            long_b=_HOSPITAL_LONG),
            index=_HOSPITAL_LAT.index)

        valid_dist = dist[(dist <= max_meters).values]
        near_hospitals = _DATA_HOSPITAL[
            _DATA_HOSPITAL['nombre_hospital'].isin(valid_dist.index)].copy()

        near_hospitals.set_index('nombre_hospital', inplace=True)
        near_hospitals['meters_to_geo_point'] = valid_dist

        hospitales_in_radio.append(near_hospitals.index.unique().to_list())

    return hospitales_in_radio


if __name__ == '__main__':

    download()
