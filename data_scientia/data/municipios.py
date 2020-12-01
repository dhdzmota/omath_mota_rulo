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
    """Fech data.
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


def get_municipio_codes(state_name=None):
    """Get municipio codes and names.

    Parameters
    ----------
    state_name: str
        State name, valid strings are:
            - "Aguascalientes"
            - "Baja California"
            - "Baja California Sur"
            - "Campeche"
            - "Chiapas"
            - "Chihuahua"
            - "Ciudad de México"
            - "Coahuila de Zaragoza"
            - "Colima"
            - "Durango"
            - "Guanajuato"
            - "Guerrero"
            - "Hidalgo"
            - "Jalisco"
            - "Michoacán de Ocampo"
            - "Morelos"
            - "México"
            - "Nayarit"
            - "Nuevo León"
            - "Oaxaca"
            - "Puebla"
            - "Querétaro"
            - "Quintana Roo"
            - "San Luis Potosí"
            - "Sinaloa"
            - "Sonora"
            - "Tabasco"
            - "Tamaulipas"
            - "Tlaxcala"
            - "Veracruz de Ignacio de la Llave"
            - "Yucatán"
            - "Zacatecas"
    Returns
    --------
    municipio_codes: dict
        Dictionary containing municipio's codes and their names.

    Example
    --------
    ::

        from data_scientia.data import municipios

        state_name = 'Colima'
        municipios.get_municipio_codes(state_name=state_name)
        Out[1]:
        {'6001': 'Armería',
         '6002': 'Colima',
         '6003': 'Comala',
         '6004': 'Coquimatlán',
         '6005': 'Cuauhtémoc',
         '6006': 'Ixtlahuacán',
         '6007': 'Manzanillo',
         '6008': 'Minatitlán',
         '6009': 'Tecomán',
         '6010': 'Villa de Álvarez'}

    """
    load()
    global _DATA_MUN

    # Filter specific state_name
    if isinstance(state_name, str):
        municipios_data = _DATA_MUN.query(
            'Nom_Ent == "%s"' % state_name)

    # Get a dictionary municipio codes and their municipio names
    municipio_codes = municipios_data[[
        'Nom_Mun', 'Cve_Ent_Mun'
    ]].drop_duplicates().set_index(
        'Cve_Ent_Mun'
    )['Nom_Mun'].to_dict()

    return municipio_codes


def load():
    """Load auxiliar variables used by this module.
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
    Parameters
    -----------
    geo_points: list
        List containing (latitude, longitude) coordinates.

    max_meters: int, float
        Max. number of meters from the geo_points to the municipio centroid
        used to filter municipios.

    Examples
    ---------
    ::

        from data_scientia.data import capacidad_hospitalaria
        from data_scientia.data import municipios

        # Guadalajara geolocation
        geo_points = [[20.6737777, -103.4054536]]

        # Get municipios loated at most 5K meters.
        municipios.get_municipio_near_geo(
            geo_points,
            max_meters=15e+3)
        Out[1]: [['14039', '14098']]

        municipios.get_municipio_codes(state_name='Jalisco')['14039']
        Out[2]: 'Guadalajara'

        municipios.get_municipio_codes(state_name='Jalisco')['14098']
        Out[3]: 'San Pedro Tlaquepaque'
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
