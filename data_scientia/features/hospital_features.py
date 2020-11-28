#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd


from data_scientia.features import *


def transform(ts):
    """Create time series features.

    Parameters
    ----------
    ts: numpy.array

    Returns
    ---------
    features: list
        List of calculated features.
    """

    if not isinstance(ts, np.ndarray):
        if config.VERBOSE:
            print('Error: argument is not a numpy array')
        return None

    features = pd.DataFrame()
    for feature in hospital_features_list:
        feature_name = str(feature).split(' ')[1]
        features[feature_name] = [feature(ts)]

    return features


