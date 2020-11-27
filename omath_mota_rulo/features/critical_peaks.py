#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd

from omath_mota_rulo import config
from omath_mota_rulo.data import capacidad_hospitalaria


def get_down_hill(hosp_data):
    """Given the temporal data of a hospital, get the portion of the timeline
    belonging to a down-hill, up to the point of the first state that is no
    longer critical.

    Parameters
    -----------
    hosp_data: pd.DataFrame
        Historical capacidad hospitalaria of a single hospital.
        Rows correspond to a daily record of its capacity.

    Returns
    --------
    down_hilll_data: pd.DataFrame
        Portion of the data that bellongs to an down-hill segment of the
        historical data up to the first non critical status.
    hosp_data: pd.DataFrame
        Remaining portion of the data, with the down_hilll_data removed.
    """

    is_cooldown = (hosp_data['estatus_capacidad_uci'] != 'Crítica')

    if is_cooldown.sum() > 0:
        idx_cooldown = is_cooldown.argmax()
        down_hilll_data = hosp_data.iloc[
            :idx_cooldown + 1
        ]

        hosp_data = hosp_data.iloc[
            idx_cooldown + 1:]
    else:
        down_hilll_data = hosp_data.copy()
        hosp_data = pd.DataFrame()

    return down_hilll_data, hosp_data


def get_up_hill(hosp_data):
    """Given the temporal data of a hospital, get the portion of the timeline
    belonging to an up-hill peak, up to the point of the first critical state
    is found.

    Parameters
    -----------
    hosp_data: pd.DataFrame
        Historical capacidad hospitalaria of a single hospital.
        Rows correspond to a daily record of its capacity.

    Returns
    --------
    up_hill_data: pd.DataFrame
        Portion of the data that bellongs to an up-hill segment of the
        historical data up to the first next critical status.
    hosp_data: pd.DataFrame
        Remaining portion of the data, with the up_hill_data removed.
    """

    is_peak = hosp_data['estatus_capacidad_uci'] == 'Crítica'
    if is_peak.sum() > 0:
        idx_peak = is_peak.argmax()

        up_hill_data = hosp_data.iloc[
            :idx_peak + 1]

        hosp_data = hosp_data.iloc[
            idx_peak + 1:]
    else:
        up_hill_data = None
        hosp_data = None

    return up_hill_data, hosp_data


def get():
    """Get peaks dataset.
    """

    # Get hosp. capacity data
    data = capacidad_hospitalaria.get()

    # Do not consider rows with UCI status.
    data = data[~data['estatus_capacidad_uci'].isnull()]

    # Find peaks and its statistics
    peaks = []
    for hospital, hospital_data in data.groupby('nombre_hospital'):
        if config.VERBOSE:
            print(hospital)

        # Ensure data is ordered
        hospital_data.sort_values('fecha', inplace=True)

        # Loop until the end of the data
        hospital_peaks = []
        remaining_hospital_data = hospital_data.copy()
        while(remaining_hospital_data.shape[0] > 2):
            # Get next peak.
            up_hilll_data, remaining_hospital_data = get_up_hill(
                hosp_data=remaining_hospital_data)

            # If no additional peaks stop the loop
            if up_hilll_data is None:
                break

            # Get next cooldown
            down_hilll_data, remaining_hospital_data = get_down_hill(
                hosp_data=remaining_hospital_data)

            # get peak stats (start, date of the peak, end of the down-hill)
            last_peak = up_hilll_data['fecha'].min()
            peak_date = up_hilll_data['fecha'].max()
            date_cooldown = down_hilll_data['fecha'].max()

            hospital_peaks.append({
                'nombre_hospital': hospital,
                'last_peak': last_peak,
                'peak_date': peak_date,
                'days_since_last_peak': (peak_date - last_peak).days,
                'peak_cooldown': date_cooldown,
                'peak_length': (date_cooldown - peak_date).days
            })

        # Hospital time has no peaks.
        if len(hospital_peaks) == 0:
            continue

        # Keep track of all peaks
        hospital_peaks = pd.DataFrame(hospital_peaks)
        peaks.append(hospital_peaks)

    peaks = pd.concat(peaks)

    return peaks
