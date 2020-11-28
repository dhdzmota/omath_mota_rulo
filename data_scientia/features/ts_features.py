#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

from data_scientia import config


from tsfresh.feature_extraction.feature_calculators import (
    abs_energy,
    absolute_sum_of_changes,
    cid_ce,
    count_above_mean,
    count_below_mean,
    kurtosis,
    maximum,
    mean,
    mean_abs_change,
    mean_change,
    mean_second_derivative_central,
    median,
    minimum,
    sample_entropy,
    skewness,
    standard_deviation,
    sum_values,
    variance,
    variation_coefficient
)


def num_municipios(data):
    return data.shape[1]

features_list = [
    abs_energy,
    absolute_sum_of_changes,
    count_above_mean,
    count_below_mean,
    kurtosis,
    maximum,
    mean,
    mean_abs_change,
    mean_change,
    mean_second_derivative_central,
    median,
    minimum,
    sample_entropy,
    skewness,
    standard_deviation,
    sum_values,
    variance,
    variation_coefficient
]


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
    for feature in features_list:
        feature_name = str(feature).split(' ')[1]
        features[feature_name] = [feature(ts)]

    features['n_municipios'] = [n_municipios]

    return features
