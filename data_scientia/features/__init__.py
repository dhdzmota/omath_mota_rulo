#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd


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


def abs_energy(data):
    return np.dot(data, data)


def absolute_sum_of_changes(data):
    return np.sum(np.abs(np.diff(data)))


def count_above_mean(data):
    m = np.mean(data)
    return np.where(data > m)[0].size


def count_below_mean(data):
    m = np.mean(data)
    return np.where(data < m)[0].size


def kurtosis(data):
    if not isinstance(data, pd.Series):
        x = pd.Series(data)
    return pd.Series.kurtosis(x)


def maximum(data):
    return np.max(data)


def mean(data):
    return np.mean(data)


def mean_abs_change(data):
    return np.mean(np.abs(np.diff(data)))


def mean_change(data):
    x = np.asarray(data)
    return (x[-1] - x[0]) / (len(x) - 1) if len(x) > 1 else np.NaN


def mean_second_derivative_central(data):
    x = np.asarray(data)
    return (x[-1] - x[-2] - x[1] + x[0]) / (2 * (len(x) - 2)) if len(x) > 2 else np.NaN


def median(data):
    return np.median(data)


def minimum(data):
    return np.min(data)


def sample_entropy(x):

    x = np.array(x)

    if np.isnan(x).any():
        return np.nan

    m = 2
    tolerance = 0.2 * np.std(x)

    xm = _into_subchunks(x, m)

    B = np.sum([np.sum(np.abs(xmi - xm).max(axis=1) <= tolerance) - 1 for xmi in xm])

    xmp1 = _into_subchunks(x, m + 1)

    A = np.sum([np.sum(np.abs(xmi - xmp1).max(axis=1) <= tolerance) - 1 for xmi in xmp1])

    return -np.log(A / B)


def _into_subchunks(x, subchunk_length, every_n=1):
    """"""
    len_x = len(x)

    assert subchunk_length > 1
    assert every_n > 0

    num_shifts = (len_x - subchunk_length) // every_n + 1
    shift_starts = every_n * np.arange(num_shifts)
    indices = np.arange(subchunk_length)

    indexer = np.expand_dims(indices, axis=0) + np.expand_dims(shift_starts, axis=1)
    return np.asarray(x)[indexer]


def skewness(data):

    if not isinstance(data, pd.Series):
        x = pd.Series(data)
    return pd.Series.skew(x)


def standard_deviation(data):
    return np.std(data)


def sum_values(data):
    return np.sum(data)


def variance(data):
    return np.var(data)


def variation_coefficient(data):
    mean = np.mean(data)
    if mean != 0:
        return np.std(data) / mean
    else:
        return np.nan


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
