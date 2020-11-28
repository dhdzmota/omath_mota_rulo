#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os

import pandas as pd

from data_scientia import config
from data_scientia.data import municipios
from data_scientia.data import capacidad_hospitalaria
from data_scientia.features import target_days_to_peak


DATA_PATH = os.path.join(
    config.DATA_DIR,
    'processed/capacidad_hospitalar.csv.gz')

_TARGET_NAME = 'is_next_peak_in_7_days'


def get_municipios_near_hospitals(capacidad_hosp_data):
    """Get the municipios codes near by a hospital geolocation.
    """
    # Get unique hospitals
    hospital_geos = capacidad_hosp_data[[
        'latitude',
        'longitude',
        'nombre_hospital'
    ]].drop_duplicates()


    # Get municipios located at most 15km
    near_muninicipios = municipios.get_municipio_near_geo(
        hospital_geos[['latitude', 'longitude']].values.tolist(),
        max_meters=15e+3)

    # Get a dictionary mapping hospital names to municipio codes
    zip_near_municipios_hospital = zip(
        hospital_geos['nombre_hospital'], near_muninicipios)

    near_municipios_hospital = dict(
         (k, v)
         for k, v in zip_near_municipios_hospital)

    return near_municipios_hospital


def get_near_hospitals(capacidad_hosp_data):
    """Get the hospitals near by.
    """
    # Get unique hospitals
    hospital_geos = capacidad_hosp_data[[
        'latitude',
        'longitude',
        'nombre_hospital'
    ]].drop_duplicates()


    # Get hospitals located at most 15km
    near_hospitals = capacidad_hospitalaria.get_hospital_near_geo(
        hospital_geos[['latitude', 'longitude']].values.tolist(),
        max_meters=5e+3)

    # Get a dictionary mapping hospital names to municipio codes
    zip_near_hospitals = zip(
        hospital_geos['nombre_hospital'], near_hospitals)

    near_hospitals = dict(
         (k, list(set(v) - set([k])))
         for k, v in zip_near_hospitals)

    return near_hospitals


def process():
    """
    """
    dataset = target_days_to_peak.get()
    capacidad_hosp_data = capacidad_hospitalaria.get()

    near_municipios = get_municipios_near_hospitals(
        capacidad_hosp_data)
    near_hospitals = get_near_hospitals(
        capacidad_hosp_data)

    dataset.set_index('nombre_hospital', inplace=True)
    dataset['near_municipios'] = pd.Series(near_municipios)
    dataset['near_hospitals'] = pd.Series(near_hospitals)

    if config.VERBOSE:
        print(DATA_PATH)

    dataset.to_csv(
        DATA_PATH,
        compression='gzip')


def get():
    """
    """
    data = pd.read_csv(
        DATA_PATH,
        compression='gzip')

    return data


if __name__ == '__main__':
    process()
