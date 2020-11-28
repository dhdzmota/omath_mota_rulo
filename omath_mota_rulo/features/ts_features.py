# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd


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


def transform(ts):
    """ Create time series features

    Parameters
    ----------
    ts: numpy array

    Returns
    ---------
    features: list
        list of calculated features
    """
    if isinstance(ts, list):
        ts = np.array(ts)

    if not isinstance(ts, np.ndarray):
        print('Error: argument is not a numpy array')
        return None

    features = pd.DataFrame()
    for feature in features_list:
        feature_name = str(feature).split(' ')[1]
        features[feature_name] = [feature(ts)]

    return features
