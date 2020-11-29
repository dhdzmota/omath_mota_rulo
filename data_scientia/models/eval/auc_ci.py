#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
from scipy import stats

from kpay_fraud_model.evaluation.auc_delong_xu import delong_roc_variance


def auc_ci_Delong(y_true, y_scores, alpha=.95):
    """AUC confidence interval via DeLong.

    Computes de ROC-AUC with its confidence interval via delong_roc_variance

    References
    -----------
        See this `Stack Overflow Question
        <https://stackoverflow.com/questions/19124239/scikit-learn-roc-curve-with-confidence-intervals/53180614#53180614/>`_
        for further details

    Examples
    --------

    ::

        y_scores = np.array(
            [0.21, 0.32, 0.63, 0.35, 0.92, 0.79, 0.82, 0.99, 0.04])
        y_true = np.array([0, 1, 0, 0, 1, 1, 0, 1, 0])

        auc, auc_var, auc_ci = auc_ci_Delong(y_true, y_scores, alpha=.95)

        np.sqrt(auc_var) * 2
        max(auc_ci) - min(auc_ci)

        print('AUC: %s' % auc, 'AUC variance: %s' % auc_var)
        print('AUC Conf. Interval: (%s, %s)' % tuple(auc_ci))

        Out:
            AUC: 0.8 AUC variance: 0.028749999999999998
            AUC Conf. Interval: (0.4676719375452081, 1.0)


    Parameters
    ----------
    y_true : list
        Ground-truth of the binary labels (allows labels between 0 and 1).
    y_scores : list
        Predicted scores.
    alpha : float
        Default 0.95

    Returns
    -------
        auc : float
            AUC
        auc_var : float
            AUC Variance
        auc_ci : tuple
            AUC Confidence Interval given alpha

    """

    y_true = np.array(y_true)
    y_scores = np.array(y_scores)

    # Get AUC and AUC variance
    auc, auc_var = delong_roc_variance(
        y_true,
        y_scores)

    auc_std = np.sqrt(auc_var)

    # Confidence Interval
    lower_upper_q = np.abs(np.array([0, 1]) - (1 - alpha) / 2)
    lower_upper_ci = stats.norm.ppf(
        lower_upper_q,
        loc=auc,
        scale=auc_std)

    lower_upper_ci[lower_upper_ci > 1] = 1

    return auc, auc_var, lower_upper_ci
