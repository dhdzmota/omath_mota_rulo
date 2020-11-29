#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from data_scientia.data import municipios
from data_scientia.data import capacidad_hospitalaria
from data_scientia.features import features_hospital


# List of hospital geos.
_CAPACIDAD_HOSP_DATA = None
_NEAR_MUNICIPIOS = None
_NEAR_HOSPITALS = None

# Max. meters from a hospital in order to use data belonging to the municipio
_MAX_METERS_MUNICIPIOS = 15e+3

# Max. meters from a hospital in order to use data belonging other hospitals
_MAX_METERS_HOSPITALS = 5e+3


def load():
    """Load list of hospital geolocations, their neighbor
    """
    global _NEAR_MUNICIPIOS
    global _NEAR_HOSPITALS
    global _CAPACIDAD_HOSP_DATA

    _CAPACIDAD_HOSP_DATA = capacidad_hospitalaria.get()

    hospitals_geos = capacidad_hospitalaria.get()[[
        'latitude',
        'longitude',
        'nombre_hospital'
    ]].drop_duplicates()

    # Get municipios located near by the hospital
    near_muninicipios = municipios.get_municipio_near_geo(
        hospitals_geos[['latitude', 'longitude']].values.tolist(),
        max_meters=_MAX_METERS_MUNICIPIOS)

    # Get a dictionary mapping hospital names to municipio codes
    zip_near_municipios_hospital = zip(
        hospitals_geos['nombre_hospital'],
        near_muninicipios)

    _NEAR_MUNICIPIOS = dict(
         (k, v)
         for k, v in zip_near_municipios_hospital)

    # Get other hospitals located near by the hospital
    near_hospitals = capacidad_hospitalaria.get_hospital_near_geo(
        hospitals_geos[['latitude', 'longitude']].values.tolist(),
        max_meters=_MAX_METERS_HOSPITALS)

    # Get a dictionary mapping hospital names to municipio codes
    zip_near_hospitals = zip(
        hospitals_geos['nombre_hospital'],
        near_hospitals)

    # Remove the hospital itself in the list of its neighbor hospitals.
    _NEAR_HOSPITALS = dict(
         (k, list(set(v) - set([k])))
         for k, v in zip_near_hospitals)

    return


def get_hospital_features(hospital_name, fechas):
    """Get hospital features related to its neighbor hospitals.

    Parameters
    ----------
    hospital_name: str
        Name of a hospital.

    Returns
    --------
    X_hospitals: pandas.DataFrame
        One row for each fecha.

    Example
    ---------
    ::

        import pandas as pd

        hospital_name = 'HOSPITAL TEPEPAN'
        fechas = pd.Series([
                '2020-04-16', '2020-04-17', '2020-04-18', '2020-04-19',
               '2020-04-20', '2020-04-22', '2020-04-21', '2020-04-23'])
    """

    load()

    global _NEAR_MUNICIPIOS
    global _NEAR_HOSPITALS
    global _CAPACIDAD_HOSP_DATA

    # Get names of the nearby hospitals
    near_hospitals = _NEAR_HOSPITALS[hospital_name]

    # Get the data of the nearby hospitals
    near_hospitals_data_ = _CAPACIDAD_HOSP_DATA[
        _CAPACIDAD_HOSP_DATA['nombre_hospital'].isin(near_hospitals)]

    # Compose a dataframe where each column corresponds to a hospital
    near_hospitals_data = {}
    for h, h_data in near_hospitals_data_.groupby('nombre_hospital'):
        estatus_capacidad_uci_percent = h_data.drop_duplicates(
            'fecha'
        ).set_index(
            'fecha'
        )['estatus_capacidad_uci_percent']

        near_hospitals_data[h] = estatus_capacidad_uci_percent

    near_hospitals_data = pd.DataFrame(near_hospitals_data)

    # Ensure that the dataframe does not contain nulls.
    for hospital in near_hospitals_data.columns:
        is_null = near_hospitals_data[hospital].isnull()

        found_first_non_nan = False
        new_data = []
        for is_nan, x in zip(is_null, near_hospitals_data[hospital]):
            if (found_first_non_nan == False) and is_nan:
                new_data.append(0)
                found_first_non_nan = True
            else:
                new_data.append(x)

        near_hospitals_data[hospital] = new_data
        near_hospitals_data[hospital].fillna(
            method='ffill',
            inplace=True)

    X_hospital = []
    for fecha in fechas:
        # Subset valid hospital data up to the current date.
        valid_near_hosp_data = data.loc[:fecha].iloc[:-1]

        # Only consider hospitals with historical data
        percent_of_null = (
            valid_near_hosp_data.isnull().sum()
        ) / (
            valid_near_hosp_data.shape[0])

        valid_near_hosp_data = valid_near_hosp_data[
            percent_of_null[percent_of_null < 1].index]

        # Compute features
        x_local = features_hospital.transform(
            valid_near_hosp_data.fillna(0).values)

        X_hospitals.append(x_local)

    X_hospitals = pd.concat(X_hospitals)
    X_hospitals.index = fechas

    return X_hospitals
