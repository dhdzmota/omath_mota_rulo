#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import pandas as pd

from data_scientia import config
from data_scientia.data import municipios
from data_scientia.data import capacidad_hospitalaria
from data_scientia.features import covid_municipalities
from data_scientia.features.utils import distance_utils


DATA_PATH = os.path.join(
    config.DATA_DIR,
    'processed/capacidad_hospitalaria.csv.gz')

# Aux. variable use to keep the data in memory
_CAPACIDAD_HOSP_DATA = None
_HOSPITAL_LAT = None
_HOSPITAL_LONG = None


def load():
    """Load auxiliar variables.
    """
    global _CAPACIDAD_HOSP_DATA
    global _HOSPITAL_LAT
    global _HOSPITAL_LONG

    if _CAPACIDAD_HOSP_DATA is None:
        _CAPACIDAD_HOSP_DATA = capacidad_hospitalaria.get()

    if _HOSPITAL_LAT is None:
        _HOSPITAL_LAT = _CAPACIDAD_HOSP_DATA.groupby(
            'nombre_hospital')['latitude'].mean()

    if _HOSPITAL_LONG is None:
        _HOSPITAL_LONG = _CAPACIDAD_HOSP_DATA.groupby(
            'nombre_hospital')['longitude'].mean()


def get_neighbor_municipios(hospital_name, max_meters=15e+3):
    """Get near by municipios.

    Parameters
    -----------
    hospital_name: str
        Name of the hospital.
    max_meters: int, float
        Max. distance in order to consider hospital neighbors.

    Returns
    --------
    near_municipios: list
        List of the neighbor municipios.

    Example
    --------
    ::

        max_meters = 15e+3
        hospital_name = 'INSTITUTO NACIONAL DE NUTRICIÓN'

        get_neighbor_municipios(hospital_name, max_meters=max_meters)

    """

    load()

    global _CAPACIDAD_HOSP_DATA
    global _HOSPITAL_LAT
    global _HOSPITAL_LONG

    geo_points = [[
        float(_HOSPITAL_LAT.loc[hospital_name]),
        float(_HOSPITAL_LONG.loc[hospital_name])]]

    near_municipios = municipios.get_municipio_near_geo(
        geo_points,
        max_meters=max_meters)[0]

    return near_municipios


def get_neighbor_hospitals(hospital_name, max_meters=5e+3):
    """Get hospital neighbors.

    Parameters
    -----------
    hospital_name: str
        Name of the hospital.
    max_meters: int, float
        Max. distance in order to consider hospital neighbors.

    Returns
    --------
    neighbor_hospitals: list
        List of the neighbor hospitals.

    Example
    --------
    ::

        hospital_name = 'INSTITUTO NACIONAL DE NUTRICIÓN'
        get_neighbor_hospitals(hospital_name, max_meters=5e+3)
        Out[1]:
        ['INSTITUTO  NACIONAL DE CARDIOLOGÍA IGNACIO CHÁVEZ',
         'INSTITUTO NACIONAL DE ENFERMEDADES RESPIRATORIAS',
         'INER',
         'HOSPITAL GENERAL DR. MANUEL GEA GONZÁLEZ',
         'HOSPITAL GENERAL DE ZONA 32 (CDMX SUR)',
         'HOSPITAL GENERAL 02 (CDMX SUR) VILLA COAPA',
         'HOSPITAL GENERAL DR MANUEL GEA GONZÁLEZ',
         'INSTITUTO NACIONAL DE CARDIOLOGÍA IGNACIO CHÁVEZ',
         'HOSPITAL GENERAL 02 (CDMX SUR) VILLA COAPA (COY.)']

        get_neighbor_hospitals(hospital_name, max_meters=.1e+3)
        Out[2]: []
    """

    load()

    global _CAPACIDAD_HOSP_DATA
    global _HOSPITAL_LAT
    global _HOSPITAL_LONG

    lat_a = [_HOSPITAL_LAT.loc[hospital_name]] * _HOSPITAL_LAT.shape[0]
    long_a = [_HOSPITAL_LONG.loc[hospital_name]] * _HOSPITAL_LONG.shape[0]

    dist = pd.Series(distance_utils.coord_dist(
        lat_a=lat_a,
        long_a=long_a,
        lat_b=_HOSPITAL_LAT,
        long_b=_HOSPITAL_LONG),
        index=_HOSPITAL_LAT.index)

    valid_dist = dist[(dist <= max_meters).values]

    neighbor_hospitals = list(set(valid_dist.index) - set([hospital_name]))

    return neighbor_hospitals


def get_hospital_daily_status(hospital_names):
    """Get daily status of hospitals.

    Parameters
    -----------
    hospital_names: list
        List of hospitals.

    Returns
    --------
    daily_status: pandas.DataFrame
        Dataframe where columns are hospitals and rows are dates, and contains
        UCI ocupancy percent.

    Examples
    ---------
    ::

        hospital_names = [
            'INSTITUTO NACIONAL DE NUTRICIÓN',
            'INSTITUTO NACIONAL DE ENFERMEDADES RESPIRATORIAS']

        get_hospital_daily_status(hospital_names)
    """

    load()

    global _CAPACIDAD_HOSP_DATA
    global _HOSPITAL_LAT
    global _HOSPITAL_LONG

    # Get hospitals data
    hospitals_data = _CAPACIDAD_HOSP_DATA[
        _CAPACIDAD_HOSP_DATA['nombre_hospital'].isin(hospital_names)]

    # Compose a dataframe where each column corresponds to a hospital
    daily_status = {}
    for h, h_data in hospitals_data.groupby('nombre_hospital'):
        estatus_capacidad_uci_percent = h_data.drop_duplicates(
            'fecha'
        ).set_index(
            'fecha'
        )['estatus_capacidad_uci_percent']

        daily_status[h] = estatus_capacidad_uci_percent

    daily_status = pd.DataFrame(daily_status)

    return daily_status


def get_neighbor_hospitals_status(hospital_name, max_meters=5e+3):
    """Get neighbor hospitals daily status.

    Parameters
    -----------
    hospital_name: str
        Name of the hospital.
    max_meters: int, float
        Max. distance in order to consider hospital neighbors.

    Returns
    --------
    neighbor_hospitals_daily_status: pandas.DataFrame
        Dataframe where columns are hospitals and rows are dates.

    Example
    --------
    ::

        max_meters = 5e+3
        hospital_name = 'INSTITUTO NACIONAL DE NUTRICIÓN'

        get_hospital_neighbors_status(
            hospital_name,
            max_meters=max_meters)
    """
    load()

    global _CAPACIDAD_HOSP_DATA
    global _HOSPITAL_LAT
    global _HOSPITAL_LONG

    neighbor_hospital_names = get_neighbor_hospitals(
        hospital_name,
        max_meters=max_meters)

    neighbor_hospitals_daily_status = get_hospital_daily_status(
        neighbor_hospital_names)

    return neighbor_hospitals_daily_status


def get_neighbor_municipio_daily_cases(hospital_name, max_meters=15e+3):
    """Get neighbor hospitals daily status.

    Parameters
    -----------
    hospital_name: str
        Name of the hospital.
    max_meters: int, float
        Max. distance in order to consider hospital neighbors.

    Returns
    --------
    neighbor_municipios_daily_cases: pandas.DataFrame
        Dataframe where columns are hospitals and rows are dates.

    Example
    --------
    ::

        max_meters = 15e+3
        hospital_name = 'INSTITUTO NACIONAL DE NUTRICIÓN'

        get_hospital_neighbors_status(
            hospital_name,
            max_meters=max_meters)
    """
    load()

    global _CAPACIDAD_HOSP_DATA
    global _HOSPITAL_LAT
    global _HOSPITAL_LONG

    neighbor_municipios = get_neighbor_municipios(
        hospital_name,
        max_meters=max_meters)

    neighbor_municipios_daily_cases = covid_municipalities.get_daily_cases(
        neighbor_municipios)

    neighbor_municipios_daily_cases.index = pd.to_datetime(
        neighbor_municipios_daily_cases.index)
    neighbor_municipios_daily_cases.sort_index(inplace=True)

    return neighbor_municipios_daily_cases
