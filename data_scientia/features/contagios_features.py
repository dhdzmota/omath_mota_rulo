#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd


from data_scientia.features import *


def transform(data):
    """Create time series features.

    Parameters
    ----------
    ts: numpy.array

    Returns
    ---------
    features: list
        List of calculated features.
    """

    if not isinstance(data, np.ndarray):
        if config.VERBOSE:
            print('Error: argument is not a numpy array')
        return None

    n_municipios = num_municipios(data)

    ts = data.sum(axis=1)

    features = pd.DataFrame()
    for feature in contagios_features_list:
        feature_name = str(feature).split(' ')[1]
        features[feature_name] = [feature(ts)]

    features['n_municipios'] = [n_municipios]

    return features
