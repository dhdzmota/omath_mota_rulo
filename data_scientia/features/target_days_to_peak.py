#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import datetime
import numpy as np
import pandas as pd

from data_scientia import config
from data_scientia.data import capacidad_hospitalaria
from data_scientia.features import critical_peaks


DATA_PATH = os.path.join(
    config.DATA_DIR,
    'processed/target_days_to_peak.csv.gz')

# Day used to compute the targets
_TARGET_DAYS = [7, 10, 15, 20, 30]


def process():
    """Get target: is critial state happening in the next n days?.
    """

    # Get hospital state's timelines
    capacidad_hosp_data = capacidad_hospitalaria.get()

    # Get hospitals critical peaks
    peaks_data = critical_peaks.get()

    dataset = []
    for hospital, hospital_peaks in peaks_data.groupby('nombre_hospital'):
        # Get hospital timeline
        hosp_capacidad = capacidad_hosp_data[
            capacidad_hosp_data['nombre_hospital'] == hospital
        ].sort_values('fecha', ascending=True)

        # Instances to be considered as negative class at the end of the
        # hospital timeline
        margin_end_timeline = (
            hosp_capacidad['fecha'].max() - datetime.timedelta(15))

        is_non_peak_instance_after_last_peak = (
            hosp_capacidad['fecha'] > hospital_peaks['peak_date'].max()
        ) & (
            hosp_capacidad['fecha'] < margin_end_timeline)

        # Compute days to all peaks of the hospital
        days_to_peak = hosp_capacidad['fecha'].apply(
            lambda x: (hospital_peaks['peak_date'] - x).dt.days
        ).T
        days_to_peak.columns = hosp_capacidad['fecha']
        days_to_peak.index = hospital_peaks['peak_date']

        # Get closest peak a head with respect to the current day
        days_to_peak = days_to_peak.apply(lambda x: x[x > 0].min())

        days_to_peak.loc[
            is_non_peak_instance_after_last_peak.values
        ] = np.inf

        # Append to the hospital timeline the number of days from the
        # next critical peak
        hosp_capacidad.set_index('fecha', inplace=True)
        hosp_capacidad['days_to_peak'] = days_to_peak

        # Subset portions of the timeline that are of interest to be part of
        # dataset
        hosp_target = hosp_capacidad[(
            # The Day of the peak is no considered
            hosp_capacidad['estatus_capacidad_uci'] != 'Crítica'
        ) & (
            # Ensure that the end of timeline that has no peaks is
            # not considered
            ~hosp_capacidad['days_to_peak'].isnull()
        )]

        dataset.append(hosp_target)

    dataset = pd.concat(dataset)

    # Compute different variants of the target
    dataset['days_to_peak_inv'] = 1 / dataset['days_to_peak']

    for days in _TARGET_DAYS:
        dataset['is_next_peak_in_%s_days' % days] = (
            dataset['days_to_peak'] <= days
        ).astype(int)

    # Save dataset
    if config.VERBOSE:
        print(DATA_PATH)

    dataset.to_csv(
        DATA_PATH,
        compression='gzip')


def get():
    """
    """
    data = pd.read_csv(DATA_PATH)
    data['fecha'] = pd.to_datetime(data['fecha'])

    return data


if __name__ == '__main__':
    process()
