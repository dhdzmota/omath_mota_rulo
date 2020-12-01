#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from data_scientia.data import covid_municipalities
from data_scientia.data import capacidad_hospitalaria
from data_scientia.features import target_days_to_peak
from data_scientia.data import capacidad_hospitalaria

# Download data
covid_municipalities.download(keep_current_downloads=False)
capacidad_hospitalaria.download()

#
target_days_to_peak.