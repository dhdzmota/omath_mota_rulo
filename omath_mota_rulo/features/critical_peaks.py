#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd

from omath_mota_rulo import config
from omath_mota_rulo.data import capacidad_hospitalaria


def get_cooldown(hosp_data):
    """
    """

    is_cooldown = (hosp_data['estatus_capacidad_uci'] != 'Crítica')

    if is_cooldown.sum() > 0:
        idx_cooldown = is_cooldown.argmax()
        cooldown_data = hosp_data.iloc[
            :idx_cooldown + 1
        ]

        hosp_data = hosp_data.iloc[
            idx_cooldown + 1:]
    else:
        cooldown_data = hosp_data.copy()
        hosp_data = pd.DataFrame()

    return cooldown_data, hosp_data


def get_peak(hosp_data):
    """
    """

    is_peak = hosp_data['estatus_capacidad_uci'] == 'Crítica'
    if is_peak.sum() > 0:
        idx_peak = is_peak.argmax()

        peak_data = hosp_data.iloc[
            :idx_peak + 1]

        hosp_data = hosp_data.iloc[
            idx_peak + 1:]
    else:
        peak_data = None
        hosp_data = None

    return peak_data, hosp_data


def get():
    """
    """
    data = capacidad_hospitalaria.get()

    data = data[~data['estatus_capacidad_uci'].isnull()]

    peaks = []
    for hospital, hospital_data in data.groupby('nombre_hospital'):
        if config.VERBOSE:
            print(hospital)

        hospital_data.sort_values('fecha', inplace=True)

        hospital_peaks = []
        remaining_hospital_data = hospital_data.copy()
        while(remaining_hospital_data.shape[0] > 2):
            peak_data, remaining_hospital_data = get_peak(
                hosp_data=remaining_hospital_data)

            if peak_data is None:
                break

            last_peak = peak_data['fecha'].min()
            peak_date = peak_data['fecha'].max()

            cooldown_data, remaining_hospital_data = get_cooldown(
                hosp_data=remaining_hospital_data)

            date_cooldown = cooldown_data['fecha'].max()

            hospital_peaks.append({
                'nombre_hospital': hospital,
                'last_peak': last_peak,
                'peak_date': peak_date,
                'days_since_last_peak': (peak_date - last_peak).days,
                'peak_cooldown': date_cooldown,
                'peak_length': (date_cooldown - peak_date).days
            })

        if len(hospital_peaks) == 0:
            continue

        hospital_peaks = pd.DataFrame(hospital_peaks)

        hospital_peaks

        peaks.append(hospital_peaks)

    peaks = pd.concat(peaks)

    return peaks
