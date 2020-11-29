#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import datetime

import pandas as pd

from data_scientia import config
from data_scientia.features import hospital
from data_scientia.features import target_days_to_peak
from data_scientia.features.utils import parallel
from data_scientia.features import features_contagios
from data_scientia.features import features_hospital


DATA_PATH = os.path.join(
    config.DATA_DIR,
    'processed/train_data.csv.gz')


_TARGETS = [
    'is_next_peak_in_7_days',
     'is_next_peak_in_15_days',
     'is_next_peak_in_20_days',
     'is_next_peak_in_30_days']

_DATA_GRP = None


def get_municipio_features(hospital_name, fechas):
    """
    Returns
    --------
    X_municipios: pandas.DataFrame
        One row for each fecha.
    """

    # Get municipios daily cases
    daily_cases = hospital.get_neighbor_municipio_daily_cases(hospital_name)
    daily_cases.index = pd.to_datetime(daily_cases.index)

    X_municipios = []
    for fecha in fechas:
        fecha_ = fecha - datetime.timedelta(1)
        x_local = features_contagios.transform(
            daily_cases.loc[:fecha_].values
        ).add_prefix('contagios_')
        X_municipios.append(x_local)

    X_municipios = pd.concat(X_municipios)
    X_municipios.index = fechas

    return X_municipios


def get_hospital_features(hospital_name, fechas):
    """
    """
    # Get municipios daily cases
    neighbor_hospitals_status = hospital.get_neighbor_hospitals_status(
        hospital_name)
    neighbor_hospitals_status.index = pd.to_datetime(
        neighbor_hospitals_status.index)

    X_neighbor_hospitals, dates = [], []
    for fecha in fechas:
        fecha_ = fecha - datetime.timedelta(1)

        try:
            neighbor_hospitals_status_ = neighbor_hospitals_status.loc[
                :fecha_
            ].values
        except:
            continue

        if neighbor_hospitals_status_.shape[0] == 0:
            continue

        x_local = features_hospital.transform(
            neighbor_hospitals_status_
        ).add_prefix('neighbor_hosp_')

        dates.append(fecha_)
        X_neighbor_hospitals.append(x_local)

    if len(X_neighbor_hospitals) == 0:
        X_neighbor_hospitals = pd.DataFrame()
    else:
        X_neighbor_hospitals = pd.concat(X_neighbor_hospitals)
        X_neighbor_hospitals.index = pd.to_datetime(dates)

    return X_neighbor_hospitals


def process_hospital(hospital_name):
    """
    """

    global _DATA_GRP

    hospital_data = _DATA_GRP.get_group(hospital_name)

    fechas = hospital_data['fecha']

    # Municipio features
    X_municipio = get_municipio_features(
        hospital_name,
        fechas)

    # Hospital features
    X_neighbor_hospitals = get_hospital_features(
        hospital_name,
        fechas)

    idx_dates = pd.Series(pd.Series(
        X_municipio.index.tolist() + X_neighbor_hospitals.index.tolist()
    ).sort_values().unique())

    dataset_local = pd.concat([
        X_municipio.reindex(idx_dates),
        X_neighbor_hospitals.reindex(idx_dates),
        hospital_data.set_index('fecha').reindex(idx_dates)[_TARGETS]
    ], axis=1)

    dataset_local = dataset_local.loc[
        dataset_local[_TARGETS].isnull().sum(axis=1) == 0]

    dataset_local['nombre_hospital'] = hospital_name

    return dataset_local


def process():
    """
    """
    global _DATA_GRP

    data = target_days_to_peak.get()
    data.drop_duplicates(
        ['fecha', 'nombre_hospital'],
        keep='first',
        inplace=True)

    _DATA_GRP = data.groupby('nombre_hospital')
    hospital_names = data['nombre_hospital'].unique()

    dataset = parallel.apply(
        process_hospital,
        hospital_names,
        n_jobs=config.N_JOBS)
    """
    dataset = []
    for hospital_name in hospital_names:
        print(hospital_name)
        dataset_local = process_hospital(hospital_name)
        dataset.append(dataset_local)
    """
    dataset = pd.concat(dataset)
    dataset.index.name = 'fecha'

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

    data['fecha'] = pd.to_datetime(data['fecha'])

    return data


if __name__ == '__main__':
    process()
