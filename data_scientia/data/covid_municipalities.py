#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import requests
import os
import glob

import pandas as pd

from data_scientia import config
from data_scientia.data import municipios

# Data path
DATA_PATH = os.path.join(
    config.DATA_DIR,
    'raw/covid_mun/')

# Create data folder.
os.makedirs(DATA_PATH, exist_ok=True)


search_api = 'https://datamexico.org/api/data.jsonrecords?'

munic_neigh = 'Municipality=%s,%s:neighbors&'

params = '&'.join([
    'cube=gobmx_covid_stats_mun',
    'drilldowns=Time,Municipality',
    ('measures='
     'Accum+Cases,'
     'Daily+Cases,'
     'AVG+7+Days+Accum+Cases,'
     'AVG+7+Days+Daily+Cases,'
     'Rate+Daily+Cases,'
     'Rate+Accum+Cases,'
     'Days+from+50+Cases'),
    'parents=true',
    'sparse=false',
    's=Casos positivos diarios',
    'q=Fecha',
    'r=withoutProcessOption'
])

api_url = search_api + munic_neigh + params


def download_municipio_covid_data(municipio_code):
    """Download a single municipio covid data.

    Parameters
    -----------
    municipio_code: str
        Municipio code.

    Returns
    --------
    data: pandas.DataFrame
        Dataframe containing daily cases of covid.
    """

    req = requests.get(
        api_url % (municipio_code, municipio_code)
    ).content
    req_data = json.loads(req.decode('utf-8'))

    if 'status' in req_data.keys():
        data = None
    else:
        data = pd.json_normalize(req_data['data'])

    return data


def download(keep_current_downloads=False):
    """Download all municipios covid data.

    Parameters
    ----------
    keep_current_downloads: bool
        Flag to skip downloads for files already downloaded.
        Set to False in order download and replace current files.
    """

    # Get target municipios
    municipios_codes = {
        **municipios.get_municipio_codes(state_name='Jalisco'),
        **municipios.get_municipio_codes(state_name='Ciudad de México'),
        **municipios.get_municipio_codes(state_name='Morelos'),
        **municipios.get_municipio_codes(state_name='México')}

    errors = {}
    for municipio_code, municipio in municipios_codes.items():
        if config.VERBOSE:
            print(municipio, municipio_code)

        # Path of the file
        data_path = os.path.join(
            DATA_PATH,
            '%s_%s.csv.gz' % (municipio, municipio_code))

        # If file already exist and you are willing to keep the current
        # download, skip this municipio
        # Set keep_current_downloads to False in order to always
        # download the file regardless of what you have in the download
        # folder.
        if os.path.exists(data_path):
            if keep_current_downloads:
                continue

        # Get municipio data
        municipio_data = download_municipio_covid_data(municipio_code)

        # Keep track of failed downloads
        if municipio_data is None:
            errors[municipio] = municipio_code
            if config.VERBOSE:
                print('Error', municipio, municipio_code)
            continue

        # Add to the downloaded municipio data some metadata
        municipio_data['municipio_code'] = municipio_code
        municipio_data['municipio'] = municipio

        # Save the data
        municipio_data.to_csv(
            data_path, index=False, compression='gzip')

    if config.VERBOSE:
        print('Errors')
        print(errors)


def get(municipio_code, filter_just_municipio=False):
    """Fetch municipio covid data.

    Parameters
    -----------
    municipio_code: str

    Returns
    -------
    data: pandas.DataFrame

    Example
    --------
    ::

        municipio_code = '9002'
        data = get(municipio_code)

        data.iloc[0]
    """
    municipio_filepath = glob.glob(
        DATA_PATH + '*_%s.csv.gz' % municipio_code)[0]

    data = pd.read_csv(
        municipio_filepath,
        dtype={
            'municipio_code': str,
            'Municipality ID': str})

    if filter_just_municipio:
        data = data[data['Municipality ID'] == municipio_code]

    return data


def get_state(state_name):
    """Get state's municipios codes.

    Parameters
    -----------
    state_name: str
        State name, see `municipios.get_municipio_codes` to find the valid
        strings for the state names.

    Returns
    --------
    state_df: pandas.DataFrame
        Dataframe with the state covid cases.

    Example
    --------
    ::

        from data_scientia.data import covid_municipalities

        state_name = 'Jalisco'
        covid_municipalities.get_state(state)

    """

    # Get municipio codes for the state.
    codes = municipios.get_municipio_codes(state_name)

    # Fetch municipio data
    state_df = pd.DataFrame()
    for code in codes:
        municipio_data = get(code, filter_just_municipio=True)
        state_df = state_df.append(municipio_data)

    # Convert dates.
    state_df['Time'] = pd.to_datetime(state_df['Time'])

    return state_df


if __name__ == '__main__':
    """
    """
    download(keep_current_downloads=True)
