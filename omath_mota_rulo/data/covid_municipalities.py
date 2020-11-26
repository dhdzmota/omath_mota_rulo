# -*- coding: utf-8 -*-
import json
import requests
import os

import pandas as pd

from omath_mota_rulo import config
from omath_mota_rulo.data import municipios


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
    keep_current_downloads = True
    """

    # Get municipios and their codes
    municipios_codes = municipios.get_municipio_codes()
    all_data, errors = {}, {}
    for municipio, municipios_code in municipios_codes.items():
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


if __name__ == '__main__':

    download()
