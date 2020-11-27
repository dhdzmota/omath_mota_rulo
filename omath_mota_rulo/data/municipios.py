# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
import os

import pandas as pd

import omath_mota_rulo.config as config


DATA_PATH = os.path.join(
    config.DATA_DIR,
    'raw/Municipios.csv')


def get():
    """
    """
    data = pd.read_csv(DATA_PATH)

    return data


def get_municipio_codes():
    """
    """
    municipios_data = get()

    municipios_data['Cve_Ent_Mun'] = municipios_data['Cve_Ent'].astype(str)
    municipios_data['Cve_Ent_Mun'] += municipios_data['Cve_Mun'].apply(
        lambda x: '{:03d}'.format(x))

    municipio_codes = municipios_data[[
        'Nom_Mun','Cve_Ent_Mun'
    ]].drop_duplicates()

    municipio_codes = municipio_codes.set_index(
        'Nom_Mun')['Cve_Ent_Mun'].to_dict()

    return municipio_codes