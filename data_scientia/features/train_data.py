#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import datetime

import pandas as pd

from data_scientia import config
from data_scientia.features import hospital
from data_scientia.features import target_days_to_peak
from data_scientia.features.utils import parallel
from data_scientia.features import features_timeseries


# Filepath of the dataset.
DATA_PATH = os.path.join(
    config.DATA_DIR,
    'processed/train_data.csv.gz')

# Target variables
_TARGETS = [
    'is_next_peak_in_7_days',
    'is_next_peak_in_15_days',
    'is_next_peak_in_20_days',
    'is_next_peak_in_30_days']

# Auxiliar variable of the groupy of hospitalss
_DATA_GRP = None


def get_municipio_features(hospital_name, fechas):
    """
    Returns
    --------
    X_municipios: pandas.DataFrame
        One row for each fecha.

    Examples
    ---------
    ::

        hospital_name = 'ALTA ESPECIALIDAD DE ZUMPANGO'
        fechas = pd.to_datetime([
            '2020-11-05',
            '2020-11-06',
            '2020-11-07',
            '2020-11-08',
            '2020-11-09',
            '2020-11-10',
            '2020-11-11',
            '2020-11-12',
            '2020-11-13',
            '2020-11-14'])
    """

    # Get municipios daily cases
    daily_cases = hospital.get_neighbor_municipio_daily_cases(
        hospital_name,
        max_meters=15e+3)

    daily_cases.index = pd.to_datetime(daily_cases.index)

    X_covid_cases_features = []
    for fecha in fechas:
        fecha_upper_boundary = fecha - datetime.timedelta(1)

        daily_cases_local = daily_cases.loc[:fecha_upper_boundary]

        x = features_timeseries.transform(
            daily_cases_local.values
        ).add_prefix('covid_cases_')

        x_rolling = [x]
        for day_break in [8, 36, 43, 29]:
            x_r = features_timeseries.transform(
                daily_cases_local.rolling(
                    window=day_break
                ).sum().values
            ).add_prefix(f'covid_cases__sum_{day_break}_days_')

            x_rolling.append(x_r)

        x = pd.concat(x_rolling, axis=1)

        X_covid_cases_features.append(x)

    X_covid_cases_features = pd.concat(X_covid_cases_features)
    X_covid_cases_features.index = fechas

    return X_covid_cases_features


def process_hospital(hospital_name):
    """Compute features for the hospital and its timeline.

    Parameters
    ----------
    hospital_name: str
        Hospital name.

    Returns
    -------

    """

    global _DATA_GRP

    # Hospital data (From the occuation)
    hospital_data = _DATA_GRP.get_group(hospital_name)

    # Hospital timeline
    fechas = hospital_data['fecha']

    # Compute covid cases features for the hospital timeline
    X_covid_cases_hospital = get_municipio_features(
        hospital_name,
        fechas)

    idx_dates = pd.Series(pd.Series(
        X_covid_cases_hospital.index.tolist()
    ).sort_values().unique())

    dataset_local = pd.concat([
        X_covid_cases_hospital.reindex(idx_dates),
        hospital_data.set_index('fecha').reindex(idx_dates)[_TARGETS]
    ], axis=1)

    dataset_local = dataset_local.loc[
        dataset_local[_TARGETS].isnull().sum(axis=1) == 0]

    dataset_local['nombre_hospital'] = hospital_name

    return dataset_local


def process():
    """Compute the dataset.

    Compute hospital dataset for all candidate dates.
    The processesing is done assyncronously by hospital.
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


    dataset = pd.concat(dataset)
    dataset.index.name = 'fecha'

    if config.VERBOSE:
        print(DATA_PATH)

    dataset.to_csv(
        DATA_PATH,
        compression='gzip')


def get():
    """Fetch dataset.
    """
    data = pd.read_csv(
        DATA_PATH,
        compression='gzip')

    data['fecha'] = pd.to_datetime(data['fecha'])

    return data


if __name__ == '__main__':
    process()
