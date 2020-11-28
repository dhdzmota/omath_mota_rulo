import numpy as np

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


def mean_std_capactity_percent(data):
    return data.std(axis=0).mean()


def mean_median_capactity_percent(data):
    return np.median(data, axis=0).mean()


contagios_features_list = [
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

hospital_features_list = [
    num_hospitals_around,
    max_capacity_percent,
    min_capacity_percent,
    mean_capactity_percent,
    mean_capacity_slope,
    mean_std_capactity_percent,
    mean_median_capactity_percent
]
