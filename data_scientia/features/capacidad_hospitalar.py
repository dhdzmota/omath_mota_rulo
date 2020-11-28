#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os

import pandas as pd

from data_scientia import config
from data_scientia.data import municipios
from data_scientia.data import capacidad_hospitalaria
from data_scientia.features import target_days_to_peak
from data_scientia.features import ts_features
from data_scientia.features import ocupacion_features


DATA_PATH = os.path.join(
    config.DATA_DIR,
    'processed/capacidad_hospitalar.csv.gz')

# Target variable
_TARGET_NAME = 'is_next_peak_in_7_days'

# Max. meters from a hospital in order to use data belonging to the municipio
_MAX_METERS_MUNICIPIOS = 15e+3

# Max. meters from a hospital in order to use data belonging other hospitals
_MAX_METERS_HOSPITALS = 5e+3


def get_municipios_near_hospitals(capacidad_hosp_data):
    """Get the municipios near by.

    Parameters
    ----------
    capacidad_hosp_data: pandas.DataFrame

    Returns
    --------
    near_hospitals: dict
        Contains a hospital name that maps to a list of municipios near by.
    """
    # Get unique hospitals
    hospital_geos = capacidad_hosp_data[[
        'latitude',
        'longitude',
        'nombre_hospital'
    ]].drop_duplicates()

    # Get municipios located near by the hospital
    near_muninicipios = municipios.get_municipio_near_geo(
        hospital_geos[['latitude', 'longitude']].values.tolist(),
        max_meters=_MAX_METERS_MUNICIPIOS)

    # Get a dictionary mapping hospital names to municipio codes
    zip_near_municipios_hospital = zip(
        hospital_geos['nombre_hospital'], near_muninicipios)

    near_municipios_hospital = dict(
         (k, v)
         for k, v in zip_near_municipios_hospital)

    return near_municipios_hospital


def get_near_hospitals(capacidad_hosp_data):
    """Get the hospitals near by.

    Parameters
    ----------
    capacidad_hosp_data: pandas.DataFrame

    Returns
    --------
    near_hospitals: dict
        Contains a hospital name that maps to a list of other hospitals
        near by.
    """
    # Get unique hospitals
    hospital_geos = capacidad_hosp_data[[
        'latitude',
        'longitude',
        'nombre_hospital'
    ]].drop_duplicates()

    # Get other hospitals located near by the hospital
    near_hospitals = capacidad_hospitalaria.get_hospital_near_geo(
        hospital_geos[['latitude', 'longitude']].values.tolist(),
        max_meters=_MAX_METERS_HOSPITALS)

    # Get a dictionary mapping hospital names to municipio codes
    zip_near_hospitals = zip(
        hospital_geos['nombre_hospital'], near_hospitals)

    near_hospitals = dict(
         (k, list(set(v) - set([k])))
         for k, v in zip_near_hospitals)

    return near_hospitals


def get_municipio_features(muncipios_data, fechas):
    """
    Returns
    --------
    X_municipios: pandas.DataFrame
        One row for each fecha.
    """
    # ts_features.transform(data)

    return


def get_hospital_features(hospitals_data, fechas):
    """
    Returns
    --------
    X_hospitals: pandas.DataFrame
        One row for each fecha.
    """

    data = {}
    for h, h_data in hospitals_data.groupby('nombre_hospital'):
        print(h)
        estatus_capacidad_uci_percent = h_data.drop_duplicates(
            'fecha'
        ).set_index(
            'fecha'
        )['estatus_capacidad_uci_percent']

        estatus_capacidad_uci_percent = estatus_capacidad_uci_percent.reindex(
            fechas)

        data[h] = estatus_capacidad_uci_percent

    data = pd.DataFrame(data)

    X_hospitals = []
    for fecha in fechas:
        x_local = ocupacion_features.transform(data.loc[:fecha].values)
        X_hospitals.append(x_local)

    X_hospitals = pd.concat(X_hospitals)
    X_hospitals.index = fechas

    return X_hospitals


def process():
    """
    """
    dataset = target_days_to_peak.get()
    capacidad_hosp_data = capacidad_hospitalaria.get()

    near_municipios = get_municipios_near_hospitals(
        capacidad_hosp_data)
    near_hospitals = get_near_hospitals(
        capacidad_hosp_data)

    X = []
    for hospital, hospital_data in dataset.groupby('nombre_hospital'):
        fechas = hospital_data['fecha']
        local_near_hosp = near_hospitals[hospital]
        local_near_municipios = near_municipios[hospital]


        if len(local_near_hosp) > 0:
            break

        hospitals_data = capacidad_hosp_data[
            capacidad_hosp_data['nombre_hospital'].isin(local_near_hosp)]

        X_hospitals = get_hospital_features(
            hospitals_data=hospitals_data,
            fechas=fechas)

        X.append(X_hospitals)

    X = pd.concat(X)

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
