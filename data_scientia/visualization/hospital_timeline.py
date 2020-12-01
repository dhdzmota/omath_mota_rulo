#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
from matplotlib.dates import date2num

from data_scientia import config
from data_scientia.data import capacidad_hospitalaria
from data_scientia.features import critical_peaks
from data_scientia.features import target_days_to_peak


def plot_hospital_timeline(
    hospital_name, ax, target_name='is_next_peak_in_7_days', gray=False):
    """
    """
    data = capacidad_hospitalaria.get()
    peaks_data = critical_peaks.get()
    days_to_peak_data = target_days_to_peak.get()

    hospital_data = data[data['nombre_hospital'] == hospital_name]
    hospital_peaks_data = peaks_data[peaks_data['nombre_hospital'] == hospital_name]
    hospital_days_to_peak_data = days_to_peak_data[
        days_to_peak_data['nombre_hospital'] == hospital_name]

    if not gray:
        color = 'blue'
    else:
        color = 'gray'

    hospital_data.set_index('fecha')['estatus_capacidad_uci_ordinal'].plot(
        marker='',
        color=color,
        markersize=3,
        grid=True,
        ax=ax)

    ax.set_title(hospital_name)

    if not gray:
        color = 'purple'
    else:
        color = 'gray'
    ax.scatter(
        x=hospital_peaks_data['peak_date'].apply(date2num),
        y=[3] * hospital_peaks_data.shape[0],
        s=100,
        color=color,
        alpha=.5)

    if not gray:
        binary_color = {
            1: 'red',
            0: 'green'}
    else:
        binary_color = {
            1: 'gray',
            0: 'gray'}

    weight_importance = {
        1: 'days_to_peak',
        0: 'days_to_peak_inv'
    }

    target_grp = hospital_days_to_peak_data.groupby(target_name)

    for is_peak_in_next_n, is_peak_in_next_n_data in target_grp:
        s = is_peak_in_next_n_data[
            weight_importance[is_peak_in_next_n]]

        if is_peak_in_next_n == 0:
             s *= 10000
        else:
            s *= 100

        s = s.clip(100, np.inf)

        ax.scatter(
            x=is_peak_in_next_n_data['fecha'].apply(date2num),
            y=is_peak_in_next_n_data['estatus_capacidad_uci_ordinal'],
            s=100,
            alpha=.5,
            color=binary_color[is_peak_in_next_n])

    ax.set_ylim(0, 4)
