#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
import scipy


def mean_ci(sample, confidence_level=0.95):
    """
    """
    degrees_freedom = len(sample) - 1
    sample_mean = np.mean(sample)
    sample_standard_error = scipy.stats.sem(sample)

    confidence_interval = scipy.stats.t.interval(
        confidence_level,
        degrees_freedom,
        sample_mean,
        sample_standard_error)

    return confidence_interval


def yerr_mean_ci(sample, confidence_level=0.95):
    """
    """
    confidence_interval = mean_ci(sample, confidence_level=0.95)

    yerr = (
        max(confidence_interval) - min(confidence_interval)
    ) / 2

    return yerr
