#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

from data_scientia import config

def num_hospitals_around(data):
    return data.shape[1] - 1


def max_capacity_percent(data):
    return data.max()


def min_capacity_percent(data):
    return data.min()


def mean_capactity_percent(data):
    return data.mean()


def mean_capacity_slope(data):
    return ((data[-1, :] - data[0, :])/len(data)).mean()


def mean_std(data):
    return data.std(axis=0).mean()


features_list =[
    num_hospitals_around,
    max_capacity_percent,
    min_capacity_percent,
    mean_capactity_percent,
    mean_capacity_slope,
    mean_std
]


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
    for feature in features_list:
        feature_name = str(feature).split(' ')[1]
        features[feature_name] = [feature(ts)]

    return features


