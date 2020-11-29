#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd

from data_scientia.data import covid_municipalities


def get_daily_cases(municipio_codes):
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
        covid_municipalities.get(
            x, filter_just_municipio=True
        ).groupby('Time')['Daily Cases'].sum()
        for x in municipio_codes]

    daily_cases = pd.concat(daily_cases, axis=1)
    daily_cases.columns = municipio_codes

    return daily_cases
