#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd


def impute_nans(data):
    """Impute nans

    Parameters
    ----------
    data: numpy.array

    Returns
    ---------
    imputed_data: numpy.array
        Data imputed nans
    """

    data_df = pd.DataFrame(data)

    imputed_data = data_df.fillna(
        method='ffill'
    ).fillna(
        method='bfill'
    ).values

    return imputed_data
