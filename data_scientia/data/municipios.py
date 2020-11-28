#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os

import pandas as pd

import data_scientia.config as config
from data_scientia.features.utils import distance_utils


DATA_PATH = os.path.join(
    config.DATA_DIR,
    'raw/Municipios.csv')

# Aux. variable use to keep the data in memory
_DATA_MUN = None
_MUNICIPIO_LAT = None
_MUNICIPIO_LONG = None


def get():
    """
    """
    data = pd.read_csv(DATA_PATH)

    # Add municipio composed id
    data['Cve_Ent_Mun'] = data['Cve_Ent'].astype(str)
    data['Cve_Ent_Mun'] += data['Cve_Mun'].apply(
        lambda x: '{:03d}'.format(x))

    # Get degree decimal notation for latitude and longitude
    data['longitude'] = data['Longitud'].apply(
        distance_utils.degrees_minutes_seconds_to_decimal_degrees)

    data['latitude'] = data['Latitud'].apply(
        distance_utils.degrees_minutes_seconds_to_decimal_degrees)

    return data



def get_municipio_codes(nom_ent=None):
    """
    """
    municipios_data = get()

    # Filter entidades
    if nom_ent is not None:
        municipios_data = municipios_data[
            municipios_data['Nom_Ent'] == nom_ent]

    # Get a dictionary municipio codes and their municipio names
    municipio_codes = municipios_data[[
        'Nom_Mun', 'Cve_Ent_Mun'
    ]].drop_duplicates().set_index(
        'Cve_Ent_Mun'
    )['Nom_Mun'].to_dict()


    return municipio_codes


def load():
    """
    """
    global _DATA_MUN
    global _MUNICIPIO_LAT
    global _MUNICIPIO_LONG

    if _DATA_MUN is None:
        _DATA_MUN = get()

    if _MUNICIPIO_LAT is None:
        _MUNICIPIO_LAT = _DATA_MUN.groupby('Cve_Ent_Mun')['latitude'].mean()

    if _MUNICIPIO_LONG is None:
        _MUNICIPIO_LONG = _DATA_MUN.groupby('Cve_Ent_Mun')['longitude'].mean()


def get_municipio_near_geo(geo_points, max_meters=15e+3):
    """
    Examples
    ---------
    ::

        from omath_mota_rulo.data import capacidad_hospitalaria

        geo_points = capacidad_hospitalaria.get().sample(2, random_state=0)[[
            'latitude',
            'longitude'
        ]].drop_duplicates().values.tolist()

        # Get municipios loated at most 5K meters.
        get_municipio_near_geo(
            geo_points,
            max_meters=5e+3)
    """

    load()

    global _DATA_MUN
    global _MUNICIPIO_LAT
    global _MUNICIPIO_LONG

    municipios_in_radio = []
    for lat, long in geo_points:
        lat_a = [lat] * _MUNICIPIO_LAT.shape[0]
        long_a = [long] * _MUNICIPIO_LONG.shape[0]

        dist = pd.Series(distance_utils.coord_dist(
            lat_a=lat_a,
            long_a=long_a,
            lat_b=_MUNICIPIO_LAT,
            long_b=_MUNICIPIO_LONG),
            index=_MUNICIPIO_LAT.index)

        valid_dist = dist[(dist <= max_meters).values]
        near_municipios = _DATA_MUN[
            _DATA_MUN['Cve_Ent_Mun'].isin(valid_dist.index)].copy()

        near_municipios.set_index('Cve_Ent_Mun', inplace=True)
        near_municipios['meters_to_geo_point'] = valid_dist

        municipios_in_radio.append(near_municipios.index.unique().to_list())

    return municipios_in_radio
