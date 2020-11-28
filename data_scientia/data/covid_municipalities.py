#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import requests
import os
import glob

import pandas as pd

from data_scientia import config
from data_scientia.data import municipios


DATA_PATH = os.path.join(
    config.DATA_DIR,
    'raw/covid_mun/')

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

urls = search_api + munic_neigh + params


def get_data_from_api(url):
    """
    """
    req = requests.get(url).content
    req_data = json.loads(req.decode('utf-8'))

    if 'status' in req_data.keys():
        data = None
    else:
        data = pd.json_normalize(req_data['data'])

    return data


def download(keep_current_downloads=False):
    """
    """

    # Get municipios and their codes
    municipios_codes = municipios.get_municipio_codes()
    all_data, errors = {}, {}
    for municipios_code, municipio in municipios_codes.items():
        # Path of the file
        data_path = os.path.join(
            DATA_PATH,
            '%s_%s.csv.gz' % (municipio, municipios_code))

        # If file already exist and you are willing to keep the current
        # download, skip this municipio
        # Set keep_current_downloads to False in order to always
        # download the file regardless of what you have in the download
        # folder.
        if os.path.exists(data_path):
            if keep_current_downloads:
                continue

        # Get municipio data
        municipio_data = get_data_from_api(
            urls % (municipios_code, municipios_code))

        # Keep track of failed downloads
        if municipio_data is None:
            errors[municipio] = municipios_code
            if config.VERBOSE:
                print('Error', municipio, municipios_code)
            continue
        else:
            if config.VERBOSE:
                print(municipio, municipios_code)

        # Add to the downloaded municipio data some metadata
        municipio_data['municipio_code'] = municipios_code
        municipio_data['municipio'] = municipio

        # Keep in memory all the data
        all_data[municipio] = municipio_data

        # Save the data
        municipio_data.to_csv(
            data_path, index=False, compression='gzip')

    if config.VERBOSE:
        print('Errors')
        print(errors)


def get(municipio_code):
    """Fetch municipio covid data.

    Parameters
    -----------
    municipio_code: str


    Returns
    -------
    data: pandas.DataFrame
    """
    municipio_filepath = glob.glob(
        DATA_PATH + '*_%s.csv.gz' % municipio_code)[0]

    data = pd.read_csv(municipio_filepath)

    return data


def get_municipios_daily_cases(municipio_codes):
    """Fecha daily cases of a pool of municipio codes.

    Parameters
    ----------
    municipio_codes: list
        List os strings containing municipio codes.

    Returns
    --------
    daily_cases: pandas.DataFrame
        Daily cases for each municipio code.

    Example
    --------
    ::

        municipio_codes = [
            '9002',
            '9003',
            '9005',
            '9006']

        get_multiple_municipios_daily_cases(municipio_codes)
        Out[101]:
                    9002  9003  9005  9006
        Time
        2020-01-01     0     0     0     0
        2020-01-02     0     0     0     0
        2020-01-03     0     0     0     0
        2020-01-04     0     0     0     0
        2020-01-05     0     0     0     0
                 ...   ...   ...   ...
        2020-11-22    91   130   134   116
        2020-11-23   199   454   281   213
        2020-11-24   102   306   146   157
        2020-11-25    90   183    96    49
        2020-11-26     1     0     1     0

        [331 rows x 4 columns]
    """

    daily_cases = [
        get(x).groupby('Time')['Daily Cases'].sum()
        for x in municipio_codes]

    daily_cases = pd.concat(daily_cases, axis=1)
    daily_cases.columns = municipio_codes

    return daily_cases


if __name__ == '__main__':

    download(keep_current_downloads=True)
