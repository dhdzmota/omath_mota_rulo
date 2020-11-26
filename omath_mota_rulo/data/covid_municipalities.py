# -*- coding: utf-8 -*-
import json
import requests
import os

import pandas as pd

import omath_mota_rulo.config as config


DATA_PATH = os.path.join(
    config.DATA_DIR,
    'raw/covid_mun/')


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


def load_data(url):
    """

    """
    urlData = requests.get(url).content
    data_dict = json.loads(urlData.decode('utf-8'))
    if 'status' in data_dict.keys():
        return None
    data = pd.json_normalize(data_dict['data'])

    if config.VERBOSE:
        print('Success')
    return data


def get_keys_muns(municipios):
    """
    """
    id_est = municipios['Cve_Ent'].unique()
    for _id_est in id_est:
        id_mun = municipios[
            municipios['Cve_Ent'] == _id_est
        ]['Cve_Mun'].unique()
        for _id_mun in id_mun:

            m = municipios[(
                municipios['Cve_Ent'] == _id_est
            ) & (
                municipios['Cve_Mun'] == _id_mun)]

            name = m['Nom_Mun'].unique()[0]

            if _id_mun < 10:
                _id_mun = '00' + str(_id_mun)
            elif _id_mun < 100:
                _id_mun = '0' + str(_id_mun)
            else:
                _id_mun = str(_id_mun)
            key = str(_id_est) + _id_mun
            yield key, name


def download():
    """
    """

    municipios = pd.read_csv('data/raw/Municipios.csv')

    dict_mun = {}
    for mun in get_keys_muns(municipios):
        if config.VERBOSE:
            print(mun)
        key, name = mun
        dict_mun[name] = load_data(urls % (key, key))
        dict_mun[name].to_csv(
            os.path.join(DATA_PATH, '%s.csv' % name))


if __name__ == '__main__':

    download()
